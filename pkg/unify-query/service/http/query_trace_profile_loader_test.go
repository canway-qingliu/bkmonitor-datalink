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
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	goRedis "github.com/go-redis/redis/v8"
	"github.com/hashicorp/consul/api"
	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/redis"
	redisService "github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/redis"
)

// H2：一个租户的路由指向本地没有只读连接的集群，只撤销这个租户。
func TestUnknownClusterRevokesOnlyThatTenant(t *testing.T) {
	other := strings.ReplaceAll(tracePublicationFor("other"), `"cluster_id":18`, `"cluster_id":19`)
	withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system"), "other": other})
	err := reloadTraceProfilePublishedRuntime()
	if err == nil || !strings.Contains(err.Error(), "other") {
		t.Fatalf("the revoked tenant was not reported: %v", err)
	}
	if installedRouteCount() != 1 {
		t.Fatalf("a tenant on a known cluster was revoked with the other: %d routes", installedRouteCount())
	}
	if failures := traceProfileLoad.consecutiveFailures(); failures != 0 {
		t.Fatalf("a publication-side revocation was counted as a load failure: %d", failures)
	}
}

// H1：授权相关的本地配置在发布源不可读时也立即生效。
func TestLocalConfigChangeAppliesWhileSourceIsDown(t *testing.T) {
	publication := withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	publication.fail(errors.New("consul unavailable"))
	config := viper.Get(TraceProfileConfigPath).(map[string]any)
	config["allowed_bk_app_codes"] = []string{"nobody"}
	loadTraceProfileConfig()
	_ = reloadTraceProfilePublishedRuntime()
	runtime := traceProfileRuntimeValue.Load()
	if runtime == nil || len(runtime.allowedAppCodes) != 1 || runtime.allowedAppCodes[0] != "nobody" {
		t.Fatalf("a removed app code is still authorized while the source is down: %+v", runtime)
	}
	if installedRouteCount() != 1 {
		t.Fatalf("the cached publication was not kept across the config change: %d routes", installedRouteCount())
	}
}

// H1：切换发布源后，新发布源成功读取之前直读关闭，不用旧发布源的快照服务。
func TestSourceSwitchFailsClosedUntilTheNewSourceReads(t *testing.T) {
	withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	mr := miniredis.RunT(t)
	if err := redis.SetInstance(t.Context(), "test:unify-query", "test:spaces", &goRedis.UniversalOptions{Addrs: []string{mr.Addr()}}); err != nil {
		t.Fatal(err)
	}
	previousBase := redisService.KVBasePath
	redisService.KVBasePath = "test:unify-query"
	t.Cleanup(func() { redisService.KVBasePath = previousBase; redis.Close() })
	mr.SetError("LOADING")
	viper.Set("storage.source", "redis")
	loadTraceProfileConfig()
	if traceProfileRuntimeValue.Load() != nil {
		t.Fatal("the old source's snapshot still serves after the source switch")
	}
	for range 3 {
		_ = reloadTraceProfilePublishedRuntime()
	}
	if traceProfileRuntimeValue.Load() != nil {
		t.Fatal("the old source's snapshot came back while the new source fails")
	}
	recorder, spanErr := runTraceProfileHandler(t, typedTraceBody("trace_span", "system_kapm"))
	if recorder.Code != http.StatusServiceUnavailable || spanErr == nil {
		t.Fatalf("an unread new source returned HTTP %d span error %v", recorder.Code, spanErr)
	}
	mr.SetError("")
	if err := mr.Set(redisTraceDataPrefix+"system", publishedBusiness132); err != nil {
		t.Fatal(err)
	}
	if err := reloadTraceProfilePublishedRuntime(); err != nil || traceProfileRuntimeValue.Load() == nil {
		t.Fatalf("the new source did not load once readable: %v", err)
	}
}

// H1：静态路由切到发布源时，发布源成功读取之前直读关闭，不保留静态路由。
func TestStaticToPublishedFailsClosedUntilTheSourceReads(t *testing.T) {
	withTraceProfileRuntime(t, traceProfileRuntimeConfig())
	static := traceProfileRuntimeValue.Load()
	publication := withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	traceProfileRuntimeValue.Store(static)
	publication.fail(errors.New("consul unavailable"))
	loadTraceProfileConfig()
	_ = reloadTraceProfilePublishedRuntime()
	if traceProfileRuntimeValue.Load() == static {
		t.Fatal("the static runtime still serves after switching to a published source")
	}
}

// M2：前缀下一个非字符串键只撤销它自己，不让每次重载都失败。
func TestRedisNonStringKeyIsAnInvalidDocument(t *testing.T) {
	ctx, client, _ := setUpRedisTracePublication(t)
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, redisTraceDataPrefix+"meta", "k", "v").Err(); err != nil {
		t.Fatal(err)
	}
	err := reloadTraceProfilePublishedRuntime()
	if err == nil || !strings.Contains(err.Error(), "不是字符串") || strings.Contains(err.Error(), "加载期间发生变化") {
		t.Fatalf("a non-string key was not reported as its own invalid document: %v", err)
	}
	if traceProfileRuntimeValue.Load() == nil || traceProfileLoad.consecutiveFailures() != 0 {
		t.Fatal("a non-string key stopped the valid tenant or counted as a read failure")
	}
	// 发布方撤销 system 之后必须生效，不被非字符串键冻结。
	if err := client.Del(ctx, redisTraceDataPrefix+"system").Err(); err != nil {
		t.Fatal(err)
	}
	_ = reloadTraceProfilePublishedRuntime()
	if traceProfileRuntimeValue.Load() != nil {
		t.Fatal("a revoked tenant is still served beside a non-string key")
	}
}

// M3：每个租户都被撤销时，结果不是本地配置错误，也不返回 503。
func TestAllTenantsRevokedIsNotALocalConfigError(t *testing.T) {
	publication := withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	publication.set("system", strings.Replace(tracePublicationFor("system"), "otel_trace_span_132", "otel_trace_span_133", 1))
	result, _, _ := loadTraceProfilePublishedRuntime()
	if result == traceReloadInvalidConfig || traceProfileRuntimeValue.Load() != nil {
		t.Fatalf("all tenants revoked was labelled %s", result)
	}
	recorder, spanErr := runTraceProfileHandler(t, typedTraceBody("trace_span", "system_kapm"))
	if recorder.Code != http.StatusBadRequest || spanErr != nil {
		t.Fatalf("all tenants revoked returned HTTP %d span error %v", recorder.Code, spanErr)
	}
}

// M3：Consul 目录键不是一份发布。
func TestConsulFolderKeyIsNotAPublication(t *testing.T) {
	snapshot, err := parsePublishedTraceTargets("data", folderKeyPairs("data/"), nil)
	if !errors.Is(err, errTraceReadTargetsUnpublished) || len(snapshot.Invalid) != 0 {
		t.Fatalf("a folder key was read as a publication: %+v %v", snapshot.Invalid, err)
	}
}

func folderKeyPairs(keys ...string) api.KVPairs {
	pairs := make(api.KVPairs, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, &api.KVPair{Key: key})
	}
	return pairs
}

// runTraceProfileRequestAs 以入站租户 tenant 发起一次 typed Trace 请求。
func runTraceProfileRequestAs(t *testing.T, body, tenant string) *httptest.ResponseRecorder {
	t.Helper()
	metadata.InitMetadata()
	ctx := metadata.InitHashID(context.Background())
	metadata.SetUser(ctx, &metadata.User{TenantID: tenant, SpaceUID: "bkcc__42"})
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/query/ts/raw", bytes.NewBufferString(body)).WithContext(ctx)
	ginContext.Set(verifiedJWTAppCodeContextKey, "kingeye-apm")
	HandlerQueryRaw(ginContext)
	return recorder
}

// R1：切到静态配置后，旧监听的一次重载不能清掉静态运行时。
func TestStaleWatcherAfterSwitchToStaticKeepsStatic(t *testing.T) {
	withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	viper.Set(TraceProfileConfigPath, traceProfileRuntimeConfig())
	loadTraceProfileConfig()
	static := traceProfileRuntimeValue.Load()
	if static == nil || static.published {
		t.Fatalf("static runtime not installed: %+v", static)
	}
	_ = reloadTraceProfilePublishedRuntime()
	if traceProfileRuntimeValue.Load() != static {
		t.Fatal("a stale watcher reload replaced the static runtime")
	}
}

// R1：关闭 Trace 后，旧监听的一次重载不能把 400 变成 503。
func TestStaleWatcherAfterDisableKeepsUnwired(t *testing.T) {
	withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	viper.Set(TraceProfileConfigPath, nil)
	loadTraceProfileConfig()
	_ = reloadTraceProfilePublishedRuntime()
	recorder, _ := runTraceProfileHandler(t, typedTraceBody("trace_span", "system_kapm"))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("disabled Trace answered HTTP %d (reason %q)", recorder.Code, traceProfileRuntimeClosedReason())
	}
}

// R1：读取期间配置钩子运行过，这次读取的结果作废，不覆盖钩子按新配置安装的运行时。
func TestReadStartedBeforeConfigChangeIsDiscarded(t *testing.T) {
	publication := withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	release, entered := make(chan struct{}), make(chan struct{})
	reading := consul.GetDataWithPrefixContext
	consul.GetDataWithPrefixContext = func(ctx context.Context, path string) (api.KVPairs, error) {
		close(entered)
		<-release
		publication.set("system", strings.Replace(tracePublicationFor("system"), `"retention_days":30`, `"retention_days":7`, 1))
		return reading(ctx, path)
	}
	t.Cleanup(func() { consul.GetDataWithPrefixContext = reading })
	done := make(chan error)
	go func() { done <- reloadTraceProfilePublishedRuntime() }()
	<-entered
	config := viper.Get(TraceProfileConfigPath).(map[string]any)
	config["allowed_bk_app_codes"] = []string{"nobody"}
	loadTraceProfileConfig()
	hooked := traceProfileRuntimeValue.Load()
	close(release)
	<-done
	if traceProfileRuntimeValue.Load() != hooked {
		t.Fatal("a read that started before the config change replaced the hook's runtime")
	}
}

// R2：发布源读取挂住时，配置钩子不能等它。
func TestConfigHookDoesNotWaitForASourceRead(t *testing.T) {
	withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	release, entered := make(chan struct{}), make(chan struct{})
	reading := consul.GetDataWithPrefixContext
	consul.GetDataWithPrefixContext = func(ctx context.Context, path string) (api.KVPairs, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, errors.New("late")
	}
	t.Cleanup(func() { consul.GetDataWithPrefixContext = reading })
	reloaded := make(chan struct{})
	go func() { _ = reloadTraceProfilePublishedRuntime(); close(reloaded) }()
	<-entered
	hooked := make(chan struct{})
	go func() { loadTraceProfileConfig(); close(hooked) }()
	select {
	case <-hooked:
	case <-time.After(500 * time.Millisecond):
		close(release)
		<-reloaded
		t.Fatal("the config hook waited for a source read")
	}
	close(release)
	<-reloaded
}

// R3：读发布源所需的路径为空是本地配置错误，不是"发布源不可用"。
func TestEmptyReadPathIsInvalidConfig(t *testing.T) {
	withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	viper.Set(TraceProfileReadTargetVersionPathConfigPath, "")
	t.Cleanup(func() { viper.Set(TraceProfileReadTargetVersionPathConfigPath, nil) })
	result, _, stale := loadTraceProfilePublishedRuntime()
	if stale || result != traceReloadInvalidConfig || traceProfileRuntimeClosedReason() != traceRuntimeInvalidConfig {
		t.Fatalf("an empty version path was reported as %q (stale=%v, reason %q)", result, stale, traceProfileRuntimeClosedReason())
	}
}

// R4：全部租户被撤销时，重载结果不是 success。
func TestAllTenantsRevokedReportsRevoked(t *testing.T) {
	publication := withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	publication.set("system", strings.Replace(tracePublicationFor("system"), "otel_trace_span_132", "otel_trace_span_133", 1))
	if result, _, _ := loadTraceProfilePublishedRuntime(); result != traceReloadRevoked {
		t.Fatalf("all tenants revoked reported %q", result)
	}
}
