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
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goRedis "github.com/go-redis/redis/v8"
	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
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
	config["single_tenant_id"] = "system"
	config["read_target_source"] = "redis"
	delete(config, "read_routes")
	previousConfig, previousSource := viper.Get(TraceProfileConfigPath), viper.Get("storage.source")
	previousBasePath := redisService.KVBasePath
	viper.Set(TraceProfileConfigPath, config)
	viper.Set("storage.source", "redis")
	redisService.KVBasePath = "test:unify-query"
	previousRuntime := traceProfileRuntimeValue.Swap(nil)
	previousConsulRead := consul.GetKVData
	consul.GetKVData = func(string) ([]byte, error) {
		return nil, errors.New("Redis Trace source called Consul")
	}
	t.Cleanup(func() {
		consul.GetKVData = previousConsulRead
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

func TestTracePublishedRoutesLoadFromRedisWhenTraceSourceIsRedis(t *testing.T) {
	_, _, _ = setUpRedisTracePublication(t)
	if !traceProfileUsesPublishedRoutes() {
		t.Fatal("Redis Trace publication was not enabled by trace_profile.read_target_source")
	}
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatalf("Redis publication was not loaded: %v", err)
	}
	runtime := traceProfileRuntimeValue.Load()
	if runtime == nil || runtime.routeCount != 2 {
		t.Fatalf("published business routes were not installed: %+v", runtime)
	}
	if runtime.singleTenantID != "system" {
		t.Fatalf("single tenant binding was lost: %s", runtime.singleTenantID)
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
