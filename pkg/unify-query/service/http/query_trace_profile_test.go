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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
)

// mirrors middleware.verifiedJWTAppCodeKey：只有 JWT 中间件验证通过后才会写入。
const verifiedJWTAppCodeContextKey = "unify_query_verified_jwt_app_code"

func typedTraceBody(profile, domain string) string {
	return `{"trace_profile":"` + profile + `","trace_contract_version":"trace_query_v2","trace_request":{` +
		`"observation_domain_id":"` + domain + `",` +
		`"expected_scope_fingerprint":"` + strings.Repeat("a", 64) + `",` +
		`"start_time":"1758585600000000","end_time":"1758672000000000",` +
		`"filters":{"field_list":[],"condition_list":[]},"order_by":[],"limit":100}}`
}

func TestTypedCountAggregationUsesAggregateCompiler(t *testing.T) {
	withTraceProfileRuntime(t, traceProfileRuntimeConfig())
	withTraceProfileAuth(t, true, "kingeye-apm")
	body := strings.Replace(typedTraceBody("trace_span", "system_kapm"), `"trace_query_v2"`, `"trace_query_v3"`, 1)
	body = strings.Replace(body, `"limit":100}}`,
		`"limit":100,"aggregation":{"method":"count","interval_seconds":60,`+
			`"timezone_offset_seconds":28800,"group_by":["resource"]}}}`, 1)
	if code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, "kingeye-apm")); code != traceprofile.CodeUnsupportedSchemaCapability {
		t.Fatalf("typed aggregate did not enforce scalar dimension whitelist: %s", code)
	}
}

func TestDefaultTraceProfileBudgetCoversThirtyDayInclusiveWindow(t *testing.T) {
	scope, err := traceprofile.ResolveScope("system", "bkcc__42", "system_kapm")
	if err != nil {
		t.Fatal(err)
	}
	end := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC).UnixMicro()
	start := end - 30*24*int64(time.Hour/time.Microsecond) - 1000
	request := traceprofile.Request{
		Profile: metadata.TraceProfileSpan, StartUS: start, EndUS: end,
		ObservationDomainID: "system_kapm", Limit: 50,
	}
	target := traceprofile.DorisTarget{ClusterID: "18", Database: "system_kapm", Table: "otel_trace_span_42"}
	limits := traceprofile.Limits{MaxRangeUS: traceProfileMaxRangeUS(defaultTraceProfileMaxRangeDays)}
	if _, err := traceprofile.CompileList(metadata.TraceProfileSpan, target, scope, request, limits); err != nil {
		t.Fatalf("30-day inclusive product window was rejected: %v", err)
	}
	request.StartUS--
	if _, err := traceprofile.CompileList(metadata.TraceProfileSpan, target, scope, request, limits); err == nil {
		t.Fatal("window beyond the product budget was accepted")
	}
}

func TestDefaultTraceProfileLimitAcceptsBoundedDetailRead(t *testing.T) {
	config := traceProfileRuntimeConfig()
	delete(config, "max_limit")
	runtime, err := newTraceProfileRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close()
	body := strings.Replace(typedTraceBody("trace_span", "system_kapm"), `"limit":100`, `"limit":10000`, 1)
	if _, err := traceprofile.ParseRequest([]byte(body), runtime.maxBodyBytes, runtime.maxLimit); err != nil {
		t.Fatalf("bounded legacy detail read was rejected: %v", err)
	}
}

func withTraceProfileRuntime(t *testing.T, raw any) {
	t.Helper()
	runtime, err := newTraceProfileRuntime(raw)
	if err != nil {
		t.Fatalf("newTraceProfileRuntime returned error: %v", err)
	}
	if runtime == nil {
		t.Fatal("expected a configured runtime")
	}
	// 保存并恢复外层运行时，子测试内的重复安装不会把全局状态清空。
	previous := traceProfileRuntimeValue.Swap(runtime)
	t.Cleanup(func() {
		if current := traceProfileRuntimeValue.Swap(previous); current != nil && current != runtime {
			current.close()
		}
		runtime.close()
	})
}

func withTraceProfileAuth(t *testing.T, enabled bool, allowed ...string) {
	t.Helper()
	previousEnabled := JwtEnabled
	JwtEnabled = enabled
	runtime := traceProfileRuntimeValue.Load()
	var previousAllowed []string
	if runtime != nil {
		previousAllowed = runtime.allowedAppCodes
		runtime.allowedAppCodes = allowed
	}
	t.Cleanup(func() {
		JwtEnabled = previousEnabled
		if runtime != nil {
			runtime.allowedAppCodes = previousAllowed
		}
	})
}

func traceProfileRuntimeConfig() map[string]any {
	return map[string]any{
		"route_snapshot_version":             7,
		"allowed_bk_app_codes":               []string{"kingeye-apm"},
		"member_lookup_allowed_bk_app_codes": []string{"kingeye-apm"},
		"max_body_bytes":                     65536,
		"max_limit":                          100,
		"max_offset":                         1000,
		"max_range_days":                     7,
		"read_routes": []any{
			map[string]any{
				"bk_tenant_id": "system", "bk_biz_id": 42, "profile": "trace_span", "observation_domain_id": "system_kapm",
				"cluster_id": "18", "schema_version": "2026-09-24", "published_route_version": 7,
			},
			map[string]any{
				"bk_tenant_id": "system", "bk_biz_id": 42, "profile": "trace_summary", "observation_domain_id": "system_kapm",
				"cluster_id": "18", "schema_version": "2026-09-24", "published_route_version": 7,
			},
		},
		"readonly_connections": []any{
			map[string]any{
				"cluster_id": "18", "host": "127.0.0.1", "port": 1, "username": "read_only",
				"connect_timeout": "1s", "query_timeout": "1s",
			},
		},
	}
}

func runTraceProfileRequest(t *testing.T, body string, verifiedAppCode string) *httptest.ResponseRecorder {
	t.Helper()
	metadata.InitMetadata()
	ctx := metadata.InitHashID(context.Background())
	metadata.SetUser(ctx, &metadata.User{TenantID: "system", SpaceUID: "bkcc__42"})
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/query/ts/raw", bytes.NewBufferString(body)).WithContext(ctx)
	if verifiedAppCode != "" {
		ginContext.Set(verifiedJWTAppCodeContextKey, verifiedAppCode)
	}
	HandlerQueryRaw(ginContext)
	return recorder
}

func decodeTraceProfileError(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %s", recorder.Body.String())
	}
	if len(body) != 2 {
		t.Fatalf("error body must be closed to code/message: %s", recorder.Body.String())
	}
	code, _ := body["code"].(string)
	if _, present := body["message"]; !present {
		t.Fatalf("error body missing message: %s", recorder.Body.String())
	}
	return code
}

func TestProbeTraceEnvelopeDetectsOnlyTypedTopLevelKeys(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{name: "typed span envelope", body: typedTraceBody("trace_span", "system_kapm"), want: true},
		{name: "typed summary envelope", body: typedTraceBody("trace_summary", "system_kapm"), want: true},
		{name: "nested key", body: `{"query_list":[{"trace_request":{}}]}`, want: false},
		{name: "value named like the key", body: `{"table_id":"trace_profile"}`, want: false},
		{name: "legacy body", body: `{"query_list":[{"table_id":"t1"}],"space_uid":"bkcc__42"}`, want: false},
		{name: "not json", body: `not json at all`, want: false},
		{name: "empty", body: ``, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := prefixHasTraceEnvelope([]byte(tc.body)); got != tc.want {
				t.Fatalf("prefixHasTraceEnvelope(%s) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

func TestProbeTraceEnvelopeLeavesLegacyBodyIntact(t *testing.T) {
	padding := strings.Repeat("x", traceProfileProbeBytes*2)
	legacyBody := `{"query_list":[{"table_id":"t1"}],"padding":"` + padding + `"}`

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/query/ts/raw", bytes.NewBufferString(legacyBody))

	if probeTraceEnvelope(ginContext) {
		t.Fatal("legacy body must not be treated as a typed Trace envelope")
	}
	restored, err := io.ReadAll(ginContext.Request.Body)
	if err != nil {
		t.Fatalf("read restored body: %v", err)
	}
	if string(restored) != legacyBody {
		t.Fatalf("legacy body was not restored byte for byte: %d bytes restored", len(restored))
	}
}

func TestNewTraceProfileRuntimeParsesOnlyCompleteConfiguration(t *testing.T) {
	t.Run("absent config stays disabled", func(t *testing.T) {
		for _, raw := range []any{nil, map[string]any{}} {
			runtime, err := newTraceProfileRuntime(raw)
			if err != nil || runtime != nil {
				t.Fatalf("expected disabled runtime, got runtime=%v err=%v", runtime, err)
			}
		}
	})

	t.Run("valid config freezes routes and budgets", func(t *testing.T) {
		runtime, err := newTraceProfileRuntime(traceProfileRuntimeConfig())
		if err != nil {
			t.Fatalf("newTraceProfileRuntime returned error: %v", err)
		}
		defer runtime.close()
		if runtime.routeVersion != 7 || runtime.routeCount != 2 || runtime.maxLimit != 100 || len(runtime.connections) != 1 {
			t.Fatalf("unexpected runtime: %+v", runtime)
		}
	})

	cases := []struct {
		name    string
		mutate  func(config map[string]any)
		wantErr string
	}{
		{
			name:   "routes without connections",
			mutate: func(config map[string]any) { delete(config, "readonly_connections") },
		},
		{
			name: "connections without routes",
			mutate: func(config map[string]any) {
				delete(config, "read_routes")
			},
		},
		{
			name: "unknown key",
			mutate: func(config map[string]any) {
				config["readonly_username"] = "root"
			},
		},
		{
			name: "unsupported profile",
			mutate: func(config map[string]any) {
				config["read_routes"].([]any)[0].(map[string]any)["profile"] = "trace_metrics"
			},
		},
		{
			name: "observation domain is not an identifier",
			mutate: func(config map[string]any) {
				config["read_routes"].([]any)[0].(map[string]any)["observation_domain_id"] = "system-kapm"
			},
		},
		{
			name: "duplicate cluster connection",
			mutate: func(config map[string]any) {
				connections := config["readonly_connections"].([]any)
				config["readonly_connections"] = append(connections, connections[0])
			},
		},
		{
			name: "route cluster without readonly connection",
			mutate: func(config map[string]any) {
				config["read_routes"].([]any)[0].(map[string]any)["cluster_id"] = "19"
			},
		},
		{
			name:   "zero snapshot version",
			mutate: func(config map[string]any) { config["route_snapshot_version"] = 0 },
		},
		{
			name:   "zero offset budget",
			mutate: func(config map[string]any) { config["max_offset"] = 0 },
		},
		{
			name:   "negative offset budget",
			mutate: func(config map[string]any) { config["max_offset"] = -1 },
		},
		{
			name: "duration must be a string",
			mutate: func(config map[string]any) {
				config["readonly_connections"].([]any)[0].(map[string]any)["query_timeout"] = 30
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := traceProfileRuntimeConfig()
			tc.mutate(config)
			runtime, err := newTraceProfileRuntime(config)
			if err == nil {
				if runtime != nil {
					runtime.close()
				}
				t.Fatal("expected configuration rejected")
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestTraceProfileDirectReadRequiresVerifiedAllowlistedWorkload(t *testing.T) {
	withTraceProfileRuntime(t, traceProfileRuntimeConfig())
	body := typedTraceBody("trace_span", "system_kapm")

	t.Run("verified workload works while legacy JWT is optional", func(t *testing.T) {
		withTraceProfileAuth(t, false, "kingeye-apm")
		code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, "kingeye-apm"))
		if code != traceprofile.CodePartialFailure {
			t.Fatalf("got %s, want %s", code, traceprofile.CodePartialFailure)
		}
	})

	t.Run("unverified header with legacy JWT optional", func(t *testing.T) {
		withTraceProfileAuth(t, false, "kingeye-apm")
		code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, ""))
		if code != traceprofile.CodeUnauthorizedMember {
			t.Fatalf("got %s, want %s", code, traceprofile.CodeUnauthorizedMember)
		}
	})

	t.Run("foreign workload", func(t *testing.T) {
		withTraceProfileAuth(t, true, "kingeye-apm")
		code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, "other-app"))
		if code != traceprofile.CodeUnauthorizedMember {
			t.Fatalf("got %s, want %s", code, traceprofile.CodeUnauthorizedMember)
		}
	})
}

func TestTraceMemberLookupRequiresItsOwnWorkloadAllowlist(t *testing.T) {
	withTraceProfileRuntime(t, traceProfileRuntimeConfig())
	withTraceProfileAuth(t, true, "kingeye-apm")
	traceProfileRuntimeValue.Load().memberLookupAllowedAppCodes = nil
	body := strings.Replace(typedTraceBody("trace_summary", "system_kapm"), `"limit":100}}`,
		`"limit":100,"scene":"global","member_lookup":{`+
			`"trace_id":"0123456789abcdef0123456789abcdef","expected_route_version":7,`+
			`"allowed_pairs":[{"app_id":7,"bk_biz_id":42}]}}}`, 1)
	code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, "kingeye-apm"))
	if code != traceprofile.CodeUnauthorizedMember {
		t.Fatalf("member allow set escaped the dedicated workload gate: %s", code)
	}
	traceProfileRuntimeValue.Load().memberLookupAllowedAppCodes = []string{"kingeye-apm"}
	code = decodeTraceProfileError(t, runTraceProfileRequest(t, body, "kingeye-apm"))
	if code != traceprofile.CodePartialFailure {
		t.Fatalf("authorized member lookup was blocked before the configured readonly query: %s", code)
	}
}

func TestESMemberLookupRejectsRangeBudgetBeforeIO(t *testing.T) {
	config := traceProfileRuntimeConfig()
	config["max_range_days"] = 1
	config["es_summary_targets"] = []any{map[string]any{
		"bk_tenant_id": "system", "bk_biz_id": 42, "app_id": 7,
		"app_name": "checkout", "observation_domain_id": "system_kapm",
		"read_result_table_id": "apm_global.trace_summary_v1_7",
		"storage_cluster_id":   9, "retention_days": 30, "published_route_version": 7,
	}}
	withTraceProfileRuntime(t, config)
	withTraceProfileAuth(t, true, "kingeye-apm")
	called := false
	traceProfileRuntimeValue.Load().esMemberReader = func(context.Context, *structured.QueryTs) (
		int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error,
	) {
		called = true
		return 0, nil, nil, nil, nil
	}
	body := strings.Replace(typedTraceBody("trace_summary", "system_kapm"),
		`"end_time":"1758672000000000"`, `"end_time":"1758758400000000"`, 1)
	body = strings.Replace(body, `"limit":100}}`,
		`"limit":100,"scene":"global","member_lookup":{`+
			`"backend":"elasticsearch","trace_id":"0123456789abcdef0123456789abcdef",`+
			`"expected_route_version":7,"allowed_pairs":[{"app_id":7,"bk_biz_id":42}]}}}`, 1)
	code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, "kingeye-apm"))
	if code != traceprofile.CodeUnboundedRange || called {
		t.Fatalf("range budget must reject before ES I/O: code=%s called=%v", code, called)
	}
}

func TestESMemberLookupChecksPublishedTargetAndVersionBeforeExecution(t *testing.T) {
	withTraceProfileAuth(t, true, "kingeye-apm")
	body := strings.Replace(typedTraceBody("trace_summary", "system_kapm"), `"limit":100}}`,
		`"limit":100,"scene":"global","member_lookup":{`+
			`"backend":"elasticsearch","trace_id":"0123456789abcdef0123456789abcdef",`+
			`"expected_route_version":7,"allowed_pairs":[{"app_id":7,"bk_biz_id":42}]}}}`, 1)
	config := traceProfileRuntimeConfig()
	withTraceProfileRuntime(t, config)
	if code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, "kingeye-apm")); code != traceprofile.CodeRouteNotFound {
		t.Fatalf("missing ES target used Doris route: %s", code)
	}
	for _, tc := range []struct {
		version int64
		want    string
	}{
		{8, traceprofile.CodeScopePartial},
		{7, "success"},
	} {
		config := traceProfileRuntimeConfig()
		config["es_summary_targets"] = []any{map[string]any{
			"bk_tenant_id": "system", "bk_biz_id": 42, "app_id": 7,
			"app_name": "checkout", "observation_domain_id": "system_kapm",
			"read_result_table_id": "apm_global.trace_summary_v1_7",
			"storage_cluster_id":   9, "retention_days": 7,
			"published_route_version": tc.version,
		}}
		withTraceProfileRuntime(t, config)
		if tc.want == "success" {
			traceProfileRuntimeValue.Load().esMemberReader = func(_ context.Context, _ *structured.QueryTs) (
				int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error,
			) {
				return 0, nil, nil, []metadata.RouteInfo{{
					TableID: "apm_global.trace_summary_v1_7", StorageType: metadata.ElasticsearchStorageType,
					StorageID: "9",
				}}, nil
			}
			recorder := runTraceProfileRequest(t, body, "kingeye-apm")
			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"total":0`) {
				t.Fatalf("trusted ES member route did not execute typed query: %d %s", recorder.Code, recorder.Body.String())
			}
			continue
		}
		if code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, "kingeye-apm")); code != tc.want {
			t.Fatalf("ES version=%d got %s, want %s", tc.version, code, tc.want)
		}
	}
}

func TestESSummaryListUsesPublishedTargetInsteadOfDorisRoute(t *testing.T) {
	withTraceProfileAuth(t, true, "kingeye-apm")
	config := traceProfileRuntimeConfig()
	config["es_summary_targets"] = []any{map[string]any{
		"bk_tenant_id": "system", "bk_biz_id": 42, "app_id": 7,
		"app_name": "checkout", "observation_domain_id": "system_kapm",
		"read_result_table_id": "apm_global.trace_summary_v1_7",
		"storage_cluster_id":   9, "retention_days": 7, "published_route_version": 7,
	}}
	withTraceProfileRuntime(t, config)
	body := strings.Replace(typedTraceBody("trace_summary", "system_kapm"),
		`"order_by":[]`, `"order_by":["-min_start_time","trace_id"]`, 1)
	body = strings.Replace(body, `"limit":100}}`,
		`"limit":2,"offset":1,"scene":"global","es_summary_list":{`+
			`"backend":"elasticsearch","app_id":7,"bk_biz_id":42,"expected_route_version":7}}}`, 1)
	called := false
	traceProfileRuntimeValue.Load().esMemberReader = func(_ context.Context, query *structured.QueryTs) (
		int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error,
	) {
		called = true
		if query.From != 1 || query.QueryList[0].TableID != "apm_global.trace_summary_v1_7" {
			t.Fatalf("ES list did not use published target: %+v", query)
		}
		return 1, nil, nil, []metadata.RouteInfo{{
			TableID: "apm_global.trace_summary_v1_7", StorageType: metadata.ElasticsearchStorageType, StorageID: "9",
		}}, nil
	}
	recorder := runTraceProfileRequest(t, body, "kingeye-apm")
	if !called || recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"total":1`) {
		t.Fatalf("published ES list did not execute: called=%v status=%d body=%s", called, recorder.Code, recorder.Body.String())
	}
}

func TestTraceProfileDirectReadEnforcesAuthorizedScope(t *testing.T) {
	withTraceProfileRuntime(t, traceProfileRuntimeConfig())
	withTraceProfileAuth(t, true, "kingeye-apm")

	t.Run("domain outside tenant", func(t *testing.T) {
		body := typedTraceBody("trace_span", "other_kapm")
		code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, "kingeye-apm"))
		if code != traceprofile.CodeUnauthorizedDomain {
			t.Fatalf("got %s, want %s", code, traceprofile.CodeUnauthorizedDomain)
		}
	})

	t.Run("tenant without route", func(t *testing.T) {
		config := traceProfileRuntimeConfig()
		routes := config["read_routes"].([]any)
		for _, raw := range routes {
			// 观测域与租户同源，改租户必须连同观测域一起改，否则配置本身非法。
			route := raw.(map[string]any)
			route["bk_tenant_id"] = "other"
			route["observation_domain_id"] = "other_kapm"
		}
		withTraceProfileRuntime(t, config)
		code := decodeTraceProfileError(t, runTraceProfileRequest(t, typedTraceBody("trace_span", "system_kapm"), "kingeye-apm"))
		if code != traceprofile.CodeRouteNotFound {
			t.Fatalf("got %s, want %s", code, traceprofile.CodeRouteNotFound)
		}
	})

	t.Run("malformed typed request", func(t *testing.T) {
		code := decodeTraceProfileError(t, runTraceProfileRequest(t, `{"trace_profile":"trace_span"}`, "kingeye-apm"))
		if code != traceprofile.CodeUnsupportedSchemaCapability {
			t.Fatalf("got %s, want %s", code, traceprofile.CodeUnsupportedSchemaCapability)
		}
	})

	t.Run("storage failure is explicit", func(t *testing.T) {
		code := decodeTraceProfileError(t, runTraceProfileRequest(t, typedTraceBody("trace_span", "system_kapm"), "kingeye-apm"))
		if code != traceprofile.CodePartialFailure {
			t.Fatalf("got %s, want %s", code, traceprofile.CodePartialFailure)
		}
	})
}

func TestTraceProfileDirectReadDisabledKeepsPinnedRejection(t *testing.T) {
	if previous := traceProfileRuntimeValue.Swap(nil); previous != nil {
		previous.close()
	}
	recorder := runTraceProfileRequest(t, typedTraceBody("trace_summary", "system_kapm"), "kingeye-apm")
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), traceProfileUnwiredMessage) {
		t.Fatalf("unconfigured direct read must keep the pinned rejection: %d %s",
			recorder.Code, recorder.Body.String())
	}
	if code := decodeTraceProfileError(t, recorder); code != traceprofile.CodeRouteNotFound {
		t.Fatalf("got %s, want %s", code, traceprofile.CodeRouteNotFound)
	}
}
