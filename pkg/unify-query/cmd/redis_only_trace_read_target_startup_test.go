// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.

package cmd

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// 不配 Consul 的部署形态：Storage 与 Trace 读目标都走 Redis，
// Consul 地址为空。冷启动、特性开关调和与 Trace 读目标重载都不能写 Error，也不能去连任何 Consul。
func TestRedisOnlyShapeWithTraceReadTargetsStartsWithoutErrors(t *testing.T) {
	redis := miniredis.RunT(t)
	envListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	envRequests := countingConsul(t, envListener)
	var legacyRequests *atomic.Int32
	if legacyListener, err := net.Listen("tcp", "127.0.0.1:8500"); err == nil {
		legacyRequests = countingConsul(t, legacyListener)
	} else {
		t.Logf("127.0.0.1:8500 is in use, requests to the former default address are not counted: %v", err)
	}

	p := startRedisOnlyTraceRootProcess(t, redis, "CONSUL_HTTP_ADDR="+envListener.Addr().String())
	for i := 0; i < 3; i++ {
		if page := p.featureFlags(true); strings.Contains(page, "refresh feature flags err") {
			t.Fatalf("reconcile %d must not fail without Consul: %s", i, page)
		}
	}
	lines := p.stop()
	if errorLines := logLines(lines, "error", ""); len(errorLines) != 0 {
		t.Errorf("the Redis-only shape must write no error line, got %d:\n%s", len(errorLines), strings.Join(errorLines, "\n"))
	}
	// 读目标确实按 Redis 重载过：没有发布时只说直读关闭（Warn），不是 Consul 路径的错误。
	if reloads := logLines(lines, "warn", "trace profile read targets"); len(reloads) == 0 {
		t.Errorf("the Trace read target must be reloaded from Redis:\n%s", strings.Join(lines, "\n"))
	}
	if calls := envRequests.Load(); calls != 0 {
		t.Errorf("no Consul client may be built from the environment address, got %d requests", calls)
	}
	if legacyRequests != nil && legacyRequests.Load() != 0 {
		t.Errorf("no Consul client may target the former default address, got %d requests", legacyRequests.Load())
	}
}

// startRedisOnlyTraceRootProcess 与 startRootProcess 相同，只是配置为 Consul 地址为空、
// storage.source 为 redis，并配置 trace_profile，让 Trace 读目标从 Redis 读。
func startRedisOnlyTraceRootProcess(t *testing.T, redis *miniredis.Miniredis, env ...string) *rootProcess {
	t.Helper()
	dir := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpPort := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	redisPort, err := strconv.Atoi(redis.Port())
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]any{
		"redis": map[string]any{
			"mode": "standalone", "host": redis.Host(), "port": redisPort,
			"kv_base_path": "optional:uq", "dial_timeout": "1s", "read_timeout": "1s",
		},
		"influxdb": map[string]any{
			"space_router_prefix":            "optional:spaces",
			"space_router_bbolt_path":        filepath.Join(dir, "space.db"),
			"space_router_bbolt_bucket_name": "optional", "is_cache": false,
		},
		"consul":        map[string]any{"consul_address": ""},
		"storage":       map[string]any{"source": "redis"},
		"trace_profile": map[string]any{"allowed_bk_app_codes": []string{"app"}},
		"http":          map[string]any{"address": "127.0.0.1", "port": httpPort},
		"jwt":           map[string]any{"enabled": false},
		"trace":         map[string]any{"enable": false},
		"logger":        map[string]any{"level": "info", "path": filepath.Join(dir, "uq.log")},
		"bbolt":         map[string]any{"default_path": filepath.Join(dir, "default.db")},
	}
	configData, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "startup.json")
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := os.Create(filepath.Join(dir, "process.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = output.Close() })

	p := &rootProcess{
		t:       t,
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", httpPort),
		logPath: filepath.Join(dir, "uq.log"),
		process: exec.Command(os.Args[0], "-test.run=^TestRedisOnlyStartupHelper$"),
		done:    make(chan error, 1),
	}
	p.process.Dir = dir
	for _, entry := range os.Environ() {
		upper := strings.ToUpper(entry)
		if !strings.HasPrefix(upper, "UNIFY-QUERY_") && !strings.HasPrefix(upper, "CONSUL_") &&
			!strings.HasPrefix(entry, "UQ_REDIS_STARTUP_HELPER_CONFIG=") && !strings.HasPrefix(entry, "GOPS_CONFIG_DIR=") {
			p.process.Env = append(p.process.Env, entry)
		}
	}
	p.process.Env = append(p.process.Env, "UQ_REDIS_STARTUP_HELPER_CONFIG="+configPath, "GOPS_CONFIG_DIR="+filepath.Join(dir, "gops"))
	p.process.Env = append(p.process.Env, env...)
	p.process.Stdout, p.process.Stderr = output, output
	if err := p.process.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.done <- p.process.Wait() }()
	t.Cleanup(func() {
		if !p.stopped {
			_ = p.process.Process.Kill()
			<-p.done
		}
		if t.Failed() {
			for _, path := range []string{output.Name(), p.logPath} {
				data, _ := os.ReadFile(path)
				t.Logf("%s:\n%s", filepath.Base(path), data)
			}
		}
	})

	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case err := <-p.done:
			p.stopped = true
			t.Fatalf("root command exited before becoming ready: %v", err)
		default:
		}
		response, err := client.Head(p.baseURL + "/")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return p
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("root command did not become ready")
		}
		time.Sleep(25 * time.Millisecond)
	}
}
