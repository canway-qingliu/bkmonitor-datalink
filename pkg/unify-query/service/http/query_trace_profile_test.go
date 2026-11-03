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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
)

// mirrors middleware.verifiedJWTAppCodeKey：只有 JWT 中间件验证通过后才会写入。
const verifiedJWTAppCodeContextKey = "unify_query_verified_jwt_app_code"

func typedTraceBody(profile, domain string) string {
	return `{"trace_profile":"` + profile + `","trace_contract_version":"trace_query_v2","trace_request":{` +
		`` +
		`"expected_scope_fingerprint":"` + strings.Repeat("a", 64) + `",` +
		`"start_time":"1758585600000000","end_time":"1758672000000000",` +
		`"filters":{"field_list":[{"field_name":"app_id","op":"eq","value":["7"]}],"condition_list":[]},"order_by":[],"limit":100}}`
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
	scope, err := traceprofile.ResolveScope("system", "bkcc__42")
	if err != nil {
		t.Fatal(err)
	}
	end := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC).UnixMicro()
	start := end - 30*24*int64(time.Hour/time.Microsecond) - 1000
	request := traceprofile.Request{
		Profile: metadata.TraceProfileSpan, StartUS: start, EndUS: end,
		Limit: 50, Filters: traceprofile.Filters{FieldList: []traceprofile.FilterField{{FieldName: "app_id", Operator: "eq", Value: []string{"7"}}}},
	}
	target := traceprofile.DorisTarget{ClusterID: "18", Database: "system_kapm", Table: "cw_trace_span_app_7"}
	limits := traceprofile.Limits{MaxRangeUS: traceprofile.MaxWindowUS(defaultTraceProfileMaxRangeDays)}
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
		"route_snapshot_version": 7,
		"allowed_bk_app_codes":   []string{"kingeye-apm"},
		"max_body_bytes":         65536,
		"max_limit":              100,
		"max_offset":             1000,
		"max_range_days":         7,
		"read_routes": []any{
			map[string]any{
				"bk_tenant_id": "system", "bk_biz_id": 42, "app_id": 7, "profile": "trace_span",
				"database": "system_kapm", "table": "cw_trace_span_app_7", "cluster_id": "18", "schema_version": "2026-09-24", "published_route_version": 7,
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
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %s", recorder.Body.String())
	}
	if len(body) != 2 {
		t.Fatalf("error body must be closed to code/message: %s", recorder.Body.String())
	}
	code, _ := body["code"].(string)
	// 后端失败是 502，其余拒绝是 400。
	want := http.StatusBadRequest
	if code == traceprofile.CodePartialFailure {
		want = http.StatusBadGateway
	}
	if recorder.Code != want {
		t.Fatalf("code %s expected HTTP %d, got %d: %s", code, want, recorder.Code, recorder.Body.String())
	}
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
		{name: "retired topology envelope", body: `{"application_topology_contract_version":"application_topology_range_v2","application_topology_request":{}}`, want: false},
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
		if runtime.routeVersion != 7 || runtime.routeCount != 1 || runtime.maxLimit != 100 || len(runtime.connections) != 1 {
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
				config["read_routes"].([]any)[0].(map[string]any)["database"] = "system-kapm"
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

func TestNewTraceProfileRuntimeRejectsBusinessRoutesAndImplicitTargets(t *testing.T) {
	for _, tc := range []struct {
		name  string
		appID int64
		table string
	}{
		{name: "implicit business target"},
		{name: "explicit business target", table: "cw_trace_span_42"},
		{name: "business target with app", appID: 7, table: "cw_trace_span_42"},
		{name: "shared target without app", table: "cw_trace_span"},
		{name: "exclusive target without app", table: "cw_trace_span_app_7"},
		{name: "implicit application target", appID: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := traceProfileRuntimeConfig()
			route := config["read_routes"].([]any)[0].(map[string]any)
			route["app_id"] = tc.appID
			route["table"] = tc.table
			runtime, err := newTraceProfileRuntime(config)
			if runtime != nil {
				runtime.close()
			}
			if err == nil || runtime != nil {
				t.Fatalf("retired or implicit target built a runtime: err=%v", err)
			}
		})
	}
}

func TestStaticTraceRouteSourceIdentityIsOptionalAndPaired(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields map[string]any
		valid  bool
	}{
		{name: "absent metadata", valid: true},
		{name: "paired metadata", fields: map[string]any{"result_table_id": "system.apm_trace_span", "data_id": 901}, valid: true},
		{name: "missing DataID", fields: map[string]any{"result_table_id": "system.apm_trace_span"}},
		{name: "missing RT", fields: map[string]any{"data_id": 901}},
		{name: "empty pair", fields: map[string]any{"result_table_id": "", "data_id": 0}},
		{name: "null pair", fields: map[string]any{"result_table_id": nil, "data_id": nil}},
		{name: "negative DataID", fields: map[string]any{"result_table_id": "system.apm_trace_span", "data_id": -1}},
		{name: "fractional DataID", fields: map[string]any{"result_table_id": "system.apm_trace_span", "data_id": 901.5}},
		{name: "string DataID", fields: map[string]any{"result_table_id": "system.apm_trace_span", "data_id": "901"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := traceProfileRuntimeConfig()
			route := config["read_routes"].([]any)[0].(map[string]any)
			for key, value := range tc.fields {
				route[key] = value
			}
			runtime, err := newTraceProfileRuntime(config)
			if runtime != nil {
				t.Cleanup(runtime.close)
			}
			if tc.valid {
				if err != nil || runtime == nil {
					t.Fatalf("valid optional source identity was rejected: %v", err)
				}
			} else if err == nil || runtime != nil {
				t.Fatalf("invalid source identity built a runtime: %v", err)
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

func TestTraceProfileDirectReadEnforcesAuthorizedScope(t *testing.T) {
	withTraceProfileRuntime(t, traceProfileRuntimeConfig())
	withTraceProfileAuth(t, true, "kingeye-apm")

	t.Run("retired domain input", func(t *testing.T) {
		body := strings.Replace(typedTraceBody("trace_span", ""), `"trace_request":{`, `"trace_request":{"observation_domain_id":"other_kapm",`, 1)
		code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, "kingeye-apm"))
		if code != traceprofile.CodeUnsupportedSchemaCapability {
			t.Fatalf("got %s, want %s", code, traceprofile.CodeUnsupportedSchemaCapability)
		}
	})

	for _, fields := range []string{`"result_table_id":"system.apm_trace_span",`, `"data_id":901,`} {
		t.Run("source metadata is not client input/"+fields, func(t *testing.T) {
			body := strings.Replace(typedTraceBody("trace_span", ""), `"trace_request":{`, `"trace_request":{`+fields, 1)
			if code := decodeTraceProfileError(t, runTraceProfileRequest(t, body, "kingeye-apm")); code != traceprofile.CodeUnsupportedSchemaCapability {
				t.Fatalf("client source metadata was accepted: %s", code)
			}
		})
	}

	t.Run("tenant without route", func(t *testing.T) {
		config := traceProfileRuntimeConfig()
		routes := config["read_routes"].([]any)
		for _, raw := range routes {
			// 受信租户和物理库必须同时换到另一租户。
			route := raw.(map[string]any)
			route["bk_tenant_id"] = "other"
			route["database"] = "other_kapm"
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
	recorder := runTraceProfileRequest(t, typedTraceBody("trace_span", "system_kapm"), "kingeye-apm")
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), traceProfileUnwiredMessage) {
		t.Fatalf("unconfigured direct read must keep the pinned rejection: %d %s",
			recorder.Code, recorder.Body.String())
	}
	if code := decodeTraceProfileError(t, recorder); code != traceprofile.CodeRouteNotFound {
		t.Fatalf("got %s, want %s", code, traceprofile.CodeRouteNotFound)
	}
}
