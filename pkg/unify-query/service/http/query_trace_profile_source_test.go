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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goRedis "github.com/go-redis/redis/v8"
	"github.com/hashicorp/consul/api"
	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
)

const (
	consulTraceDataPath    = "test/unify-query/data/trace/read_target"
	consulTraceVersionPath = "test/unify-query/version/trace/read_target"
)

// tracePublicationFor 是租户 tenant 的一份完整发布文档：Doris 双 profile 与一个 ES Summary 目标。
func tracePublicationFor(tenant string) string {
	document := strings.Replace(publishedBusiness132, `"application_topology_targets":[]`,
		`"application_topology_targets":[],"es_summary_targets":`+publishedESSummaryTargets, 1)
	return strings.ReplaceAll(document, "system", tenant)
}

// consulTracePublication 用可替换的文档与读错误驱动 Consul 发布源。
type consulTracePublication struct {
	mu        sync.Mutex
	documents map[string]string
	readErr   error
}

func (p *consulTracePublication) set(tenant, document string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if document == "" {
		delete(p.documents, tenant)
		return
	}
	p.documents[tenant] = document
}

func (p *consulTracePublication) fail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readErr = err
}

func withConsulTracePublication(t *testing.T, documents map[string]string) *consulTracePublication {
	t.Helper()
	publication := &consulTracePublication{documents: documents}
	config := traceProfileRuntimeConfig()
	delete(config, "read_routes")
	config["read_target_data_path"] = consulTraceDataPath
	config["read_target_version_path"] = consulTraceVersionPath
	previousConfig, previousStorage := viper.Get(TraceProfileConfigPath), viper.Get("storage.source")
	viper.Set(TraceProfileConfigPath, config)
	viper.Set("storage.source", "consul")
	resetTraceProfileLoader(t)
	previousRuntime := traceProfileRuntimeValue.Swap(nil)
	previousPrefix, previousKey, previousWatch := consul.GetDataWithPrefixContext, consul.GetKVDataContext, consul.WatchChangeContext
	consul.GetDataWithPrefixContext = func(_ context.Context, path string) (api.KVPairs, error) {
		publication.mu.Lock()
		defer publication.mu.Unlock()
		if publication.readErr != nil {
			return nil, publication.readErr
		}
		pairs := make(api.KVPairs, 0, len(publication.documents))
		for tenant, document := range publication.documents {
			pairs = append(pairs, &api.KVPair{Key: path + tenant, Value: []byte(document)})
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].Key < pairs[j].Key })
		return pairs, nil
	}
	consul.GetKVDataContext = func(_ context.Context, path string) ([]byte, error) {
		publication.mu.Lock()
		defer publication.mu.Unlock()
		if publication.readErr != nil {
			return nil, publication.readErr
		}
		return []byte(`{"time":1}`), nil
	}
	t.Cleanup(func() {
		consul.GetDataWithPrefixContext, consul.GetKVDataContext, consul.WatchChangeContext = previousPrefix, previousKey, previousWatch
		viper.Set(TraceProfileConfigPath, previousConfig)
		viper.Set("storage.source", previousStorage)
		if current := traceProfileRuntimeValue.Swap(previousRuntime); current != nil {
			current.close()
		}
	})
	return publication
}

// resetTraceProfileLoader 清空加载器缓存的发布快照与关闭原因，测试结束时恢复。
func resetTraceProfileLoader(t *testing.T) {
	t.Helper()
	traceProfileLoader.mu.Lock()
	previous := traceProfileLoader.publication
	traceProfileLoader.publication = nil
	traceProfileLoader.mu.Unlock()
	previousReason := traceProfileRuntimeClosedReason()
	t.Cleanup(func() {
		traceProfileLoader.mu.Lock()
		traceProfileLoader.publication = previous
		traceProfileLoader.mu.Unlock()
		setTraceProfileRuntimeNilReason(previousReason)
	})
}

func resolveTenantTarget(tenant string) error {
	runtime := traceProfileRuntimeValue.Load()
	if runtime == nil {
		return errors.New("no Trace runtime installed")
	}
	_, err := runtime.executor.ResolveESSummaryTarget(tenant, 132, 7, tenant+"_kapm")
	return err
}

// F4：一次读取失败不能扔掉可用快照。
func TestTraceTransientConsulReadErrorKeepsLastGoodSnapshot(t *testing.T) {
	publication := withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	installed := traceProfileRuntimeValue.Load()
	publication.fail(errors.New("consul unavailable"))
	for range 2 {
		if err := reloadTraceProfilePublishedRuntime(); err == nil {
			t.Fatal("failed read was reported as success")
		}
	}
	if traceProfileRuntimeValue.Load() != installed {
		t.Fatal("a failed read replaced or revoked the last good snapshot")
	}
	if failures := traceProfileLoad.consecutiveFailures(); failures != 2 {
		t.Fatalf("consecutive failures = %d, want 2", failures)
	}
	publication.fail(nil)
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	if failures := traceProfileLoad.consecutiveFailures(); failures != 0 {
		t.Fatalf("a successful read did not reset the failure count: %d", failures)
	}
}

// 读取成功但一个租户的文档非法：只撤销该租户，其余租户照常服务。
func TestInvalidDocumentRevokesOnlyItsTenant(t *testing.T) {
	publication := withConsulTracePublication(t, map[string]string{
		"system": tracePublicationFor("system"),
		"other":  tracePublicationFor("other"),
	})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	if resolveTenantTarget("system") != nil || resolveTenantTarget("other") != nil {
		t.Fatal("both tenants should be served before the invalid publication")
	}
	publication.set("other", strings.Replace(tracePublicationFor("other"), "otel_trace_span_132", "otel_trace_span_133", 1))
	err := reloadTraceProfilePublishedRuntime()
	if err == nil || !strings.Contains(err.Error(), "other") {
		t.Fatalf("invalid document was not reported with its tenant: %v", err)
	}
	if err := resolveTenantTarget("system"); err != nil {
		t.Fatalf("a valid tenant was revoked by another tenant's document: %v", err)
	}
	revoked := resolveTenantTarget("other")
	if revoked == nil || traceprofile.ErrorCode(revoked) != traceprofile.CodeRouteNotFound {
		t.Fatalf("invalid tenant still served or wrong code: %v", revoked)
	}
}

func TestTraceZeroKeysOnSuccessfulReadIsUnpublished(t *testing.T) {
	publication := withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	publication.set("system", "")
	err := reloadTraceProfilePublishedRuntime()
	if !errors.Is(err, errTraceReadTargetsUnpublished) {
		t.Fatalf("zero keys was not reported as unpublished: %v", err)
	}
	if traceProfileRuntimeValue.Load() != nil {
		t.Fatal("an explicit empty publication left the old routes serving")
	}
}

// F13：文档版本不变但内容变化时快照版本随之变化，新内容生效。
func TestTraceContentChangeWithoutVersionBumpRebuilds(t *testing.T) {
	publication := withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	before := traceProfileRuntimeValue.Load().routeVersion
	publication.set("system", strings.Replace(tracePublicationFor("system"), `"retention_days":30`, `"retention_days":7`, 1))
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	runtime := traceProfileRuntimeValue.Load()
	target, err := runtime.executor.ResolveESSummaryTarget("system", 132, 7, "system_kapm")
	if runtime.routeVersion == before || err != nil || target.RetentionDays != 7 {
		t.Fatalf("content change was ignored: version %d -> %d target=%+v err=%v", before, runtime.routeVersion, target, err)
	}
}

func TestTraceWatchErrorAndCloseKeepLastGoodSnapshot(t *testing.T) {
	publication := withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	installed := traceProfileRuntimeValue.Load()
	publication.fail(errors.New("consul unavailable"))
	var watches atomic.Int32
	consul.WatchChangeContext = func(context.Context, string) (<-chan any, error) {
		if watches.Add(1) == 1 {
			closed := make(chan any)
			close(closed)
			return closed, nil
		}
		return nil, errors.New("watch failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	if err := startTraceProfileRouteWatch(ctx, &wg); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	for watches.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("watch was not retried")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	wg.Wait()
	if traceProfileRuntimeValue.Load() != installed {
		t.Fatal("a closed or failed watch revoked the last good snapshot")
	}
}

func TestTraceTransientRedisReadErrorKeepsLastGoodSnapshot(t *testing.T) {
	_, _, mr := setUpRedisTracePublication(t)
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	installed := traceProfileRuntimeValue.Load()
	mr.SetError("LOADING Redis is loading the dataset in memory")
	if err := reloadTraceProfilePublishedRuntime(); err == nil {
		t.Fatal("failed Redis read was reported as success")
	}
	mr.SetError("")
	if traceProfileRuntimeValue.Load() != installed {
		t.Fatal("a failed Redis read replaced or revoked the last good snapshot")
	}
}

// mgetHook 在 MGET 执行前运行 before，模拟扫描与读取之间的发布变化。
type mgetHook struct{ before func() }

func (h mgetHook) BeforeProcess(ctx context.Context, cmd goRedis.Cmder) (context.Context, error) {
	if cmd.Name() == "mget" && h.before != nil {
		h.before()
	}
	return ctx, nil
}
func (mgetHook) AfterProcess(context.Context, goRedis.Cmder) error { return nil }
func (mgetHook) BeforeProcessPipeline(ctx context.Context, _ []goRedis.Cmder) (context.Context, error) {
	return ctx, nil
}
func (mgetHook) AfterProcessPipeline(context.Context, []goRedis.Cmder) error { return nil }

func TestTraceRedisKeyVanishingMidScanSkipsOnlyThatKey(t *testing.T) {
	ctx, client, mr := setUpRedisTracePublication(t)
	if err := client.Set(ctx, redisTraceDataPrefix+"other", strings.ReplaceAll(publishedBusiness132, "system", "other"), 0).Err(); err != nil {
		t.Fatal(err)
	}
	client.AddHook(mgetHook{before: func() { mr.Del(redisTraceDataPrefix + "other") }})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatalf("a key removed between SCAN and read failed the whole load: %v", err)
	}
	runtime := traceProfileRuntimeValue.Load()
	if runtime == nil || runtime.routeCount != 2 {
		t.Fatalf("the remaining tenant was not loaded: %+v", runtime)
	}
}

func TestTraceRedisKeyAddedMidLoadKeepsLastGoodSnapshot(t *testing.T) {
	_, client, mr := setUpRedisTracePublication(t)
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	installed := traceProfileRuntimeValue.Load()
	client.AddHook(mgetHook{before: func() {
		_ = mr.Set(redisTraceDataPrefix+"other", strings.ReplaceAll(publishedBusiness132, "system", "other"))
	}})
	err := reloadTraceProfilePublishedRuntime()
	if err == nil || !strings.Contains(err.Error(), "加载期间发生变化") {
		t.Fatalf("a publication during the load was not detected: %v", err)
	}
	if traceProfileRuntimeValue.Load() != installed {
		t.Fatal("an inconsistent read replaced the last good snapshot")
	}
}
