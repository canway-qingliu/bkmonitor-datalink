// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goRedis "github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/redis"
	redisService "github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/redis"
)

const (
	redisTraceDataPrefix = "test:unify-query:data:trace:read_target:"
	redisTraceChannel    = "test:unify-query:data:trace:read_target:read_target_channel"
)

func setUpRedisTracePublication(t *testing.T) (context.Context, goRedis.UniversalClient, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	ctx := context.Background()
	if err := redis.SetInstance(ctx, "test:unify-query", "test:spaces", &goRedis.UniversalOptions{Addrs: []string{mr.Addr()}}); err != nil {
		t.Fatal(err)
	}
	client := redis.Client()
	if err := client.Set(ctx, redisTraceDataPrefix+"system", publishedBusiness132, 0).Err(); err != nil {
		t.Fatal(err)
	}
	config := traceProfileRuntimeConfig()
	delete(config, "read_routes")
	previousConfig, previousSource := viper.Get(TraceProfileConfigPath), viper.Get("storage.source")
	previousBasePath := redisService.KVBasePath
	viper.Set(TraceProfileConfigPath, config)
	viper.Set("storage.source", "redis")
	redisService.KVBasePath = "test:unify-query"
	resetTraceProfileLoader(t)
	previousRuntime := traceProfileRuntimeValue.Swap(nil)
	previousConsulRead := consul.GetKVDataContext
	consul.GetKVDataContext = func(context.Context, string) ([]byte, error) {
		return nil, errors.New("Redis Trace source called Consul")
	}
	t.Cleanup(func() {
		consul.GetKVDataContext = previousConsulRead
		viper.Set(TraceProfileConfigPath, previousConfig)
		viper.Set("storage.source", previousSource)
		redisService.KVBasePath = previousBasePath
		if current := traceProfileRuntimeValue.Swap(previousRuntime); current != nil {
			current.close()
		}
		redis.Close()
	})
	return ctx, client, mr
}

func TestTracePublishedRoutesLoadFromRedisWhenStorageSourceIsRedis(t *testing.T) {
	_, _, _ = setUpRedisTracePublication(t)
	if !traceProfileUsesPublishedRoutes() {
		t.Fatal("Redis Trace publication was not enabled without Consul paths")
	}
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatalf("Redis publication was not loaded: %v", err)
	}
	runtime := traceProfileRuntimeValue.Load()
	if runtime == nil || runtime.routeCount != 1 {
		t.Fatalf("published business routes were not installed: %+v", runtime)
	}
}

func TestRedisStorageWithoutTraceProfileDoesNotEnableTraceLoader(t *testing.T) {
	previousConfig, previousSource := viper.Get(TraceProfileConfigPath), viper.Get("storage.source")
	t.Cleanup(func() {
		viper.Set(TraceProfileConfigPath, previousConfig)
		viper.Set("storage.source", previousSource)
	})
	viper.Set("storage.source", "redis")
	viper.Set(TraceProfileConfigPath, nil)
	if traceProfileUsesPublishedRoutes() {
		t.Fatal("Redis storage enabled Trace loader without trace_profile config")
	}
}

func TestTraceRedisInvalidPublicationRevokesOldRoute(t *testing.T) {
	ctx, client, _ := setUpRedisTracePublication(t)
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(publishedBusiness132, "otel_trace_span_132", "otel_trace_span_133", 1)
	if err := client.Set(ctx, redisTraceDataPrefix+"system", bad, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := reloadTraceProfilePublishedRuntime(); err == nil {
		t.Fatal("invalid Redis publication was accepted")
	}
	if traceProfileRuntimeValue.Load() != nil {
		t.Fatal("old Trace route remained after invalid Redis publication")
	}
}

func TestTraceRedisNotificationReloadsRoutes(t *testing.T) {
	ctx, client, _ := setUpRedisTracePublication(t)
	watchCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	if err := startTraceProfileRouteWatch(watchCtx, &wg); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	for traceProfileRuntimeValue.Load() == nil {
		select {
		case <-deadline:
			t.Fatal("Redis Trace route was not loaded after subscribing")
		case <-time.After(10 * time.Millisecond):
		}
	}
	oldVersion := traceProfileRuntimeValue.Load().routeVersion
	next := strings.ReplaceAll(strings.Replace(publishedBusiness132, `"route_snapshot_version":7`, `"route_snapshot_version":8`, 1), "132", "133")
	if err := client.Set(ctx, redisTraceDataPrefix+"system", next, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(ctx, redisTraceChannel, `{"bk_tenant_id":"system"}`).Err(); err != nil {
		t.Fatal(err)
	}
	for {
		if current := traceProfileRuntimeValue.Load(); current != nil && current.routeVersion != oldVersion {
			return
		}
		select {
		case <-deadline:
			t.Fatal("Redis notification did not replace Trace routes")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Redis 发布源、Trace 前缀下没有任何键（读方已切到 Redis、写方尚未写入）：明确报告"未发布"
// （400、只在进入该状态时记一条 Warn、按发布源计数），不计为读取失败、不返回 503；键出现后可加载。
func TestRedisSourceWithNoTraceKeysIsUnpublished(t *testing.T) {
	ctx, client, _ := setUpRedisTracePublication(t)
	traceProfileLoad.mu.Lock()
	previous := traceProfileLoad.lastResult
	traceProfileLoad.lastResult = ""
	traceProfileLoad.mu.Unlock()
	t.Cleanup(func() {
		traceProfileLoad.mu.Lock()
		traceProfileLoad.lastResult = previous
		traceProfileLoad.mu.Unlock()
	})
	if err := client.Del(ctx, redisTraceDataPrefix+"system").Err(); err != nil {
		t.Fatal(err)
	}
	logs := captureTraceLogs(t)
	before := traceProfileReloadCount(t, "redis", traceReloadUnpublished)
	for range 3 {
		err := reloadTraceProfilePublishedRuntime()
		if !errors.Is(err, errTraceReadTargetsUnpublished) || traceProfileRuntimeClosedReason() != traceRuntimeUnpublished {
			t.Fatalf("no Redis keys was not reported as unpublished: err=%v reason=%q", err, traceProfileRuntimeClosedReason())
		}
	}
	if failures := traceProfileLoad.consecutiveFailures(); failures != 0 {
		t.Fatalf("an unpublished source was counted as %d read failures", failures)
	}
	if after := traceProfileReloadCount(t, "redis", traceReloadUnpublished); after != before+3 {
		t.Fatalf("unpublished was not counted for the redis source: %v -> %v", before, after)
	}
	if warn, errorLines := logs.lines("warn", "尚未发布"), logs.lines("error", "尚未发布"); warn != 1 || errorLines != 0 {
		t.Fatalf("unpublished logged %d Warn and %d Error lines over three reloads, want one Warn", warn, errorLines)
	}
	recorder, spanErr := runTraceProfileHandler(t, typedTraceBody("trace_span", "system_kapm"))
	if recorder.Code != http.StatusBadRequest || spanErr != nil ||
		!strings.Contains(recorder.Body.String(), traceProfilePublishedEmptyMessage) {
		t.Fatalf("unpublished answered HTTP %d (%s) span error %v", recorder.Code, recorder.Body.String(), spanErr)
	}
	if err := client.Set(ctx, redisTraceDataPrefix+"system", publishedBusiness132, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := reloadTraceProfilePublishedRuntime(); err != nil || traceProfileRuntimeValue.Load() == nil {
		t.Fatalf("publication after an empty start was not loaded: %v", err)
	}
}

// 启动时 Redis 发布源没有任何键：和重载一样只记一条 Warn（未发布）、计数，不记 Error。
func TestStartupWithNoTraceKeysLogsOneWarnAndNoError(t *testing.T) {
	ctx, client, _ := setUpRedisTracePublication(t)
	resetTraceProfileLoadState(t)
	if err := client.Del(ctx, redisTraceDataPrefix+"system").Err(); err != nil {
		t.Fatal(err)
	}
	logs := captureTraceLogs(t)
	before := traceProfileReloadCount(t, "redis", traceReloadUnpublished)
	loadTraceProfileOnStart()
	if reason := traceProfileRuntimeClosedReason(); reason != traceRuntimeUnpublished {
		t.Fatalf("startup with no keys closed direct read as %q, want unpublished", reason)
	}
	if warn, errorLines := logs.lines("warn", "尚未发布"), logs.lines("error", ""); warn != 1 || errorLines != 0 {
		t.Fatalf("startup with no keys logged %d unpublished Warn and %d Error lines, want one Warn and no Error", warn, errorLines)
	}
	if after := traceProfileReloadCount(t, "redis", traceReloadUnpublished); after != before+1 {
		t.Fatalf("startup unpublished was not counted: %v -> %v", before, after)
	}
}

// 启动时发布源不可读、没有可用快照：仍是 Error，且同一次加载只记一条。
func TestStartupReadFailureLogsOneError(t *testing.T) {
	_, _, mr := setUpRedisTracePublication(t)
	resetTraceProfileLoadState(t)
	mr.Close()
	logs := captureTraceLogs(t)
	loadTraceProfileOnStart()
	if errorLines := logs.lines("error", ""); errorLines != 1 {
		t.Fatalf("startup read failure logged %d Error lines, want exactly one", errorLines)
	}
}

func resetTraceProfileLoadState(t *testing.T) {
	t.Helper()
	traceProfileLoad.mu.Lock()
	previousResult, previousFailures := traceProfileLoad.lastResult, traceProfileLoad.failures
	traceProfileLoad.lastResult, traceProfileLoad.failures = "", 0
	traceProfileLoad.mu.Unlock()
	t.Cleanup(func() {
		traceProfileLoad.mu.Lock()
		traceProfileLoad.lastResult, traceProfileLoad.failures = previousResult, previousFailures
		traceProfileLoad.mu.Unlock()
	})
}

// traceLogBuffer collects log lines written during a test.
type traceLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *traceLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *traceLogBuffer) lines(level, substr string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, line := range strings.Split(b.buf.String(), "\n") {
		if strings.Contains(line, "\t"+level+"\t") && strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func captureTraceLogs(t *testing.T) *traceLogBuffer {
	t.Helper()
	logs := &traceLogBuffer{}
	log.SetOutput(logs)
	log.InitTestLogger()
	t.Cleanup(func() { log.SetOutput(nil); log.InitTestLogger() })
	return logs
}

func traceProfileReloadCount(t *testing.T, source, result string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "unify_query_trace_profile_snapshot_reload_total" {
			continue
		}
		for _, sample := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range sample.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["source"] == source && labels["result"] == result {
				return sample.GetCounter().GetValue()
			}
		}
	}
	return 0
}
