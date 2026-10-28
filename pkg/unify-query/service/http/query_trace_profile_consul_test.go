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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
)

const publishedBusiness132 = `{"route_snapshot_version":7,"bk_tenant_id":"system","read_routes":[` +
	`{"bk_tenant_id":"system","bk_biz_id":132,"profile":"trace_span","observation_domain_id":"system_kapm",` +
	`"database":"system_kapm","table":"otel_trace_span_132","cluster_id":18,"cluster_name":"doris4_test",` +
	`"schema_version":"sha256:e6e9af09d86b396e"},` +
	`{"bk_tenant_id":"system","bk_biz_id":132,"profile":"trace_summary","observation_domain_id":"system_kapm",` +
	`"database":"system_kapm","table":"otel_trace_summary_132","cluster_id":18,"cluster_name":"doris4_test",` +
	`"schema_version":"sha256:281052267ec373fc"}],"application_topology_targets":[]}`

const publishedESSummaryTargets = `[{"bk_tenant_id":"system","bk_biz_id":132,"app_id":7,` +
	`"app_name":"checkout","observation_domain_id":"system_kapm",` +
	`"read_result_table_id":"apm_global.trace_summary_v1_7","storage_cluster_id":9,"retention_days":30}]`

func TestPublishedTraceTargetsInstallESSummaryCandidateIntoRuntime(t *testing.T) {
	prefix := "kingeye/metadata/unify-query/data/trace/read_target/"
	publication := strings.Replace(publishedBusiness132, `"application_topology_targets":[]`,
		`"application_topology_targets":[],"es_summary_targets":`+publishedESSummaryTargets, 1)
	snapshot, err := parsePublishedTraceTargets(prefix,
		api.KVPairs{&api.KVPair{Key: prefix + "system", Value: []byte(publication)}}, nil)
	version, routes, esTargets := snapshot.Version, snapshot.Routes, snapshot.ESTargets
	if err != nil || len(snapshot.Invalid) != 0 || len(esTargets) != 1 {
		t.Fatalf("ES Summary publication was lost: targets=%+v err=%v", esTargets, err)
	}
	config := traceProfileRuntimeConfig()
	config["route_snapshot_version"] = version
	config["read_routes"] = routes
	config["es_summary_targets"] = esTargets
	runtime, err := newTraceProfileRuntime(config)
	if err != nil {
		t.Fatalf("ES Summary target did not build a runtime: %v", err)
	}
	defer runtime.close()
	target, err := runtime.executor.ResolveESSummaryTarget("system", 132, 7, "system_kapm")
	if err != nil || target.ResultTableID != "apm_global.trace_summary_v1_7" || target.PublishedVersion != 7 {
		t.Fatalf("ES Summary route identity/version drifted: %+v err=%v", target, err)
	}
}

func TestESSummaryOnlyTenantCanLoadTrustedCandidateWithoutDorisConnection(t *testing.T) {
	config := map[string]any{
		"route_snapshot_version": 7,
		"read_routes":            []any{},
		"readonly_connections":   []any{},
		"es_summary_targets": []any{map[string]any{
			"bk_tenant_id": "system", "bk_biz_id": 132, "app_id": 7,
			"app_name": "checkout", "observation_domain_id": "system_kapm",
			"read_result_table_id": "apm_global.trace_summary_v1_7",
			"storage_cluster_id":   9, "retention_days": 30, "published_route_version": 7,
		}},
	}
	runtime, err := newTraceProfileRuntime(config)
	if err != nil || runtime == nil {
		t.Fatalf("ES-only trusted candidate was not loaded: runtime=%v err=%v", runtime, err)
	}
	target, err := runtime.executor.ResolveESSummaryTarget("system", 132, 7, "system_kapm")
	if err != nil || target.ResultTableID != "apm_global.trace_summary_v1_7" {
		t.Fatalf("ES-only target was lost: %+v err=%v", target, err)
	}
}

func TestPublishedTraceTargetsBuildBusiness132Runtime(t *testing.T) {
	prefix := "kingeye/metadata/unify-query/data/trace/read_target/"
	pairs := api.KVPairs{&api.KVPair{Key: prefix + "system", Value: []byte(publishedBusiness132)}}
	snapshot, err := parsePublishedTraceTargets(prefix, pairs, nil)
	version, routes := snapshot.Version, snapshot.Routes
	if err != nil || len(snapshot.Invalid) != 0 || version <= 0 || len(routes) != 2 {
		t.Fatalf("failed to parse published routes: version=%d routes=%d err=%v", version, len(routes), err)
	}
	if routes[0].BkBizID != 132 || routes[0].Table != "otel_trace_span_132" || routes[0].ClusterID != "18" {
		t.Fatalf("business 132 span target was lost: %+v", routes[0])
	}
	if routes[0].PublishedVersion != 7 || routes[1].PublishedVersion != 7 {
		t.Fatalf("tenant-local publication version was lost: %+v", routes)
	}
	config := traceProfileRuntimeConfig()
	config["route_snapshot_version"] = version
	config["read_routes"] = routes
	runtime, err := newTraceProfileRuntime(config)
	if err != nil {
		t.Fatalf("published routes did not build a runtime: %v", err)
	}
	defer runtime.close()
}

func TestPublishedApplicationSpanRouteSurvivesRuntimeReload(t *testing.T) {
	const database = "tenant_bbc5e661e106c6dc_kapm"
	appRoute := `,{"bk_tenant_id":"system","bk_biz_id":132,"app_id":7,"profile":"trace_span",` +
		`"observation_domain_id":"` + database + `","database":"` + database + `",` +
		`"table":"otel_trace_span","cluster_id":18,"cluster_name":"doris4_test",` +
		`"schema_version":"sha256:e6e9af09d86b396e"}`
	body := strings.Replace(publishedBusiness132, `],"application_topology_targets":[]`, appRoute+`],"application_topology_targets":[]`, 1)
	prefix := "kingeye/metadata/unify-query/data/trace/read_target/"
	pairs := api.KVPairs{&api.KVPair{Key: prefix + "system", Value: []byte(body)}}
	snapshot, err := parsePublishedTraceTargets(prefix, pairs, nil)
	if err != nil || len(snapshot.Routes) != 3 || snapshot.Routes[2].AppID != 7 {
		t.Fatalf("application route lost while loading publication: %+v err=%v", snapshot.Routes, err)
	}
	config := traceProfileRuntimeConfig()
	config["route_snapshot_version"] = snapshot.Version
	config["read_routes"] = snapshot.Routes
	runtime, err := newTraceProfileRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close()
}

func TestPublishedTraceTargetsKeepVersionsPerTenant(t *testing.T) {
	prefix := "kingeye/metadata/unify-query/data/trace/read_target/"
	other := strings.ReplaceAll(publishedBusiness132, "system", "other")
	other = strings.Replace(other, `"route_snapshot_version":7`, `"route_snapshot_version":11`, 1)
	pairs := api.KVPairs{
		&api.KVPair{Key: prefix + "system", Value: []byte(publishedBusiness132)},
		&api.KVPair{Key: prefix + "other", Value: []byte(other)},
	}
	snapshot, err := parsePublishedTraceTargets(prefix, pairs, nil)
	aggregate, routes := snapshot.Version, snapshot.Routes
	if err != nil || len(snapshot.Invalid) != 0 || aggregate <= 0 || len(routes) != 4 {
		t.Fatalf("multi-tenant publication was rejected: version=%d routes=%d err=%v", aggregate, len(routes), err)
	}
	versions := map[string]int64{}
	for _, route := range routes {
		versions[route.BkTenantID] = route.PublishedVersion
	}
	if versions["system"] != 7 || versions["other"] != 11 {
		t.Fatalf("tenant-local versions were replaced by the aggregate: %+v", versions)
	}
}

func TestReloadTraceProfileRuntimeConsumesPublishedConsulRoutes(t *testing.T) {
	prefix := "kingeye/metadata/unify-query/data/trace/read_target"
	versionPath := "kingeye/metadata/unify-query/version/trace/read_target"
	config := traceProfileRuntimeConfig()
	config["single_tenant_id"] = "system"
	delete(config, "read_routes")
	config["read_target_data_path"] = prefix
	config["read_target_version_path"] = versionPath
	previousConfig := viper.Get(TraceProfileConfigPath)
	viper.Set(TraceProfileConfigPath, config)
	resetTraceProfileLoader(t)
	previousRuntime := traceProfileRuntimeValue.Swap(nil)
	previousPrefixReader, previousKeyReader := consul.GetDataWithPrefixContext, consul.GetKVDataContext
	consul.GetDataWithPrefixContext = func(_ context.Context, path string) (api.KVPairs, error) {
		if path != prefix+"/" {
			t.Fatalf("read wrong Consul prefix: %s", path)
		}
		return api.KVPairs{&api.KVPair{Key: path + "system", Value: []byte(publishedBusiness132)}}, nil
	}
	consul.GetKVDataContext = func(_ context.Context, path string) ([]byte, error) {
		if path != versionPath {
			t.Fatalf("read wrong Consul version key: %s", path)
		}
		return []byte(`{"time":1}`), nil
	}
	t.Cleanup(func() {
		consul.GetDataWithPrefixContext, consul.GetKVDataContext = previousPrefixReader, previousKeyReader
		viper.Set(TraceProfileConfigPath, previousConfig)
		if current := traceProfileRuntimeValue.Swap(previousRuntime); current != nil {
			current.close()
		}
	})

	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatalf("published runtime did not load: %v", err)
	}
	runtime := traceProfileRuntimeValue.Load()
	if runtime == nil || runtime.routeCount != 2 || runtime.routeVersion <= 0 {
		t.Fatalf("published business routes were not installed: %+v", runtime)
	}
	if tenant := runtime.singleTenantID; tenant != "system" {
		t.Fatalf("Consul runtime lost the local tenant binding: %s", tenant)
	}
}

func TestReloadTraceProfileRuntimeRevokesOldRouteOnInvalidPublication(t *testing.T) {
	prefix := "kingeye/metadata/unify-query/data/trace/read_target"
	versionPath := "kingeye/metadata/unify-query/version/trace/read_target"
	config := traceProfileRuntimeConfig()
	old, err := newTraceProfileRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	delete(config, "read_routes")
	config["read_target_data_path"] = prefix
	config["read_target_version_path"] = versionPath
	previousConfig := viper.Get(TraceProfileConfigPath)
	viper.Set(TraceProfileConfigPath, config)
	resetTraceProfileLoader(t)
	previousRuntime := traceProfileRuntimeValue.Swap(old)
	previousPrefixReader, previousKeyReader := consul.GetDataWithPrefixContext, consul.GetKVDataContext
	consul.GetDataWithPrefixContext = func(_ context.Context, path string) (api.KVPairs, error) {
		bad := strings.Replace(publishedBusiness132, "otel_trace_span_132", "otel_trace_span_133", 1)
		return api.KVPairs{&api.KVPair{Key: path + "system", Value: []byte(bad)}}, nil
	}
	consul.GetKVDataContext = func(context.Context, string) ([]byte, error) { return []byte(`{"time":2}`), nil }
	t.Cleanup(func() {
		consul.GetDataWithPrefixContext, consul.GetKVDataContext = previousPrefixReader, previousKeyReader
		viper.Set(TraceProfileConfigPath, previousConfig)
		if current := traceProfileRuntimeValue.Swap(previousRuntime); current != nil {
			current.close()
		}
	})

	if err := reloadTraceProfilePublishedRuntime(); err == nil {
		t.Fatal("invalid publication was accepted")
	}
	if traceProfileRuntimeValue.Load() != nil {
		t.Fatal("stale Trace route remained active after invalid publication")
	}
}

func TestTraceProfileVersionWatchLoadsNewBusinessSnapshot(t *testing.T) {
	prefix := "kingeye/metadata/unify-query/data/trace/read_target"
	versionPath := "kingeye/metadata/unify-query/version/trace/read_target"
	config := traceProfileRuntimeConfig()
	delete(config, "read_routes")
	config["read_target_data_path"] = prefix
	config["read_target_version_path"] = versionPath
	previousConfig := viper.Get(TraceProfileConfigPath)
	viper.Set(TraceProfileConfigPath, config)
	resetTraceProfileLoader(t)
	previousRuntime := traceProfileRuntimeValue.Swap(nil)
	previousPrefixReader, previousKeyReader, previousWatch := consul.GetDataWithPrefixContext, consul.GetKVDataContext, consul.WatchChangeContext
	var document, version atomic.Value
	document.Store(publishedBusiness132)
	version.Store(`{"time":1}`)
	consul.GetDataWithPrefixContext = func(_ context.Context, path string) (api.KVPairs, error) {
		return api.KVPairs{&api.KVPair{Key: path + "system", Value: []byte(document.Load().(string))}}, nil
	}
	consul.GetKVDataContext = func(context.Context, string) ([]byte, error) { return []byte(version.Load().(string)), nil }
	watch := make(chan any, 1)
	consul.WatchChangeContext = func(context.Context, string) (<-chan any, error) { return watch, nil }
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		consul.GetDataWithPrefixContext, consul.GetKVDataContext, consul.WatchChangeContext = previousPrefixReader, previousKeyReader, previousWatch
		viper.Set(TraceProfileConfigPath, previousConfig)
		if current := traceProfileRuntimeValue.Swap(previousRuntime); current != nil {
			current.close()
		}
	})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	oldVersion := traceProfileRuntimeValue.Load().routeVersion
	if err := startTraceProfileRouteWatch(ctx, &wg); err != nil {
		t.Fatal(err)
	}
	document.Store(strings.ReplaceAll(strings.Replace(publishedBusiness132,
		`"route_snapshot_version":7`, `"route_snapshot_version":8`, 1), "132", "133"))
	version.Store(`{"time":2}`)
	watch <- struct{}{}
	deadline := time.After(3 * time.Second)
	for {
		if current := traceProfileRuntimeValue.Load(); current != nil && current.routeCount == 2 && current.routeVersion != oldVersion {
			return
		}
		select {
		case <-deadline:
			t.Fatal("Trace version watch did not replace the published business snapshot")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
