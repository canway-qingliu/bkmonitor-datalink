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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/dorisclient"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
)

type tenantScopeQuerier func(context.Context, string, ...any) (dorisclient.Result, error)

func (query tenantScopeQuerier) Query(ctx context.Context, statement string, args ...any) (dorisclient.Result, error) {
	return query(ctx, statement, args...)
}

func TestTraceProfileSingleTenantIDKeepsExistingSecurityChecks(t *testing.T) {
	data, err := os.ReadFile("../../query/traceprofile/testdata/trace-profile-row-golden-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			PhysicalRow map[string]any `json:"physical_row"`
		} `json:"cases"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&fixture); err != nil || len(fixture.Cases) == 0 {
		t.Fatalf("invalid row fixture: %v", err)
	}
	for _, tc := range []struct {
		name, incoming, space, app, domain, target, binding, code string
		multiTenant, unconfigured, unverified, reloadMode         bool
		bizID                                                     int64
	}{
		{name: "mapped request"},
		{name: "configurable target", target: "tenant_new"},
		{name: "new business only needs authorization and published route", bizID: 43},
		{name: "real tenant", incoming: "system"},
		{name: "multi tenant preserves identity", incoming: "system", multiTenant: true},
		{name: "multi tenant disables alias", multiTenant: true, code: traceprofile.CodeUnauthorizedDomain},
		{name: "single tenant runtime survives mode reload", reloadMode: true},
		{name: "multi tenant runtime survives mode reload", multiTenant: true, reloadMode: true, code: traceprofile.CodeUnauthorizedDomain},
		{name: "unconfigured", unconfigured: true, code: traceprofile.CodeUnauthorizedDomain},
		{name: "unverified caller", unverified: true, code: traceprofile.CodeUnauthorizedMember},
		{name: "unauthorized app", app: "other", code: traceprofile.CodeUnauthorizedMember},
		{name: "space without published route", space: "bkcc__43", code: traceprofile.CodeRouteNotFound},
		{name: "other incoming tenant", incoming: "foreign", code: traceprofile.CodeUnauthorizedDomain},
		{name: "body cannot choose tenant", domain: "foreign_kapm", code: traceprofile.CodeUnauthorizedDomain},
		{name: "invalid configured tenant", binding: "../system", code: traceprofile.CodeUnauthorizedMember},
		{name: "stale target", binding: "tenant_new", domain: "tenant_new_kapm", code: traceprofile.CodeRouteNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.bizID == 0 {
				tc.bizID = 42
			}
			if tc.incoming == "" {
				tc.incoming = "default"
			}
			if tc.space == "" {
				tc.space = fmt.Sprintf("bkcc__%d", tc.bizID)
			}
			if tc.app == "" {
				tc.app = "kingeye-apm"
			}
			if tc.target == "" {
				tc.target = "system"
			}
			if tc.binding == "" {
				tc.binding = tc.target
			}
			if tc.domain == "" {
				tc.domain = tc.target + "_kapm"
			}
			if tc.code == "" {
				tc.code = "OK"
			}
			previousMode := influxdb.MultiTenantMode
			previousModeConfig := viper.Get(influxdb.MultiTenantModeConfigPath)
			influxdb.MultiTenantMode = tc.multiTenant
			viper.Set(influxdb.MultiTenantModeConfigPath, tc.multiTenant)
			t.Cleanup(func() {
				influxdb.MultiTenantMode = previousMode
				viper.Set(influxdb.MultiTenantModeConfigPath, previousModeConfig)
			})
			config := traceProfileRuntimeConfig()
			for _, raw := range config["read_routes"].([]any) {
				route := raw.(map[string]any)
				route["bk_tenant_id"], route["observation_domain_id"] = tc.target, tc.target+"_kapm"
				route["bk_biz_id"] = tc.bizID
			}
			if !tc.unconfigured {
				config["single_tenant_id"] = tc.binding
			}
			withTraceProfileRuntime(t, config)
			withTraceProfileAuth(t, true, "kingeye-apm")
			target, err := traceprofile.NewDorisTarget("18", tc.target+"_kapm", tc.bizID, metadata.TraceProfileSpan, "2026-09-24")
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := traceprofile.NewRouteSnapshot(7, []traceprofile.ReadRoute{{BkTenantID: tc.target, BkBizID: tc.bizID, Profile: metadata.TraceProfileSpan, ObservationDomainID: tc.target + "_kapm", Doris: &target}})
			if err != nil {
				t.Fatal(err)
			}
			called := false
			querier := tenantScopeQuerier(func(ctx context.Context, statement string, args ...any) (dorisclient.Result, error) {
				called = true
				if metadata.GetUser(ctx).TenantID != tc.incoming {
					t.Fatal("shared request tenant was mutated")
				}
				if !strings.Contains(statement, "`bk_tenant_id` = ?") || !strings.Contains(statement, "`observation_domain_id` = ?") {
					t.Fatal("isolation filters were lost")
				}
				tenantFound, domainFound := false, false
				for _, arg := range args {
					tenantFound = tenantFound || arg == tc.target
					domainFound = domainFound || arg == tc.target+"_kapm"
				}
				if !tenantFound || !domainFound {
					t.Fatalf("query lost data identity: %v", args)
				}
				row := make(map[string]any, len(fixture.Cases[0].PhysicalRow)+1)
				for key, value := range fixture.Cases[0].PhysicalRow {
					row[key] = value
				}
				for _, key := range []string{"resource", "attributes", "events", "links"} {
					encoded, err := json.Marshal(row[key])
					if err != nil {
						t.Fatal(err)
					}
					row[key] = string(encoded)
				}
				row["bk_tenant_id"], row["observation_domain_id"], row["total"] = tc.target, tc.target+"_kapm", int64(1)
				row["bk_biz_id"], row["space_uid"] = tc.bizID, fmt.Sprintf("bkcc__%d", tc.bizID)
				return dorisclient.Result{Rows: []map[string]any{row}}, nil
			})
			executor, err := traceprofile.NewExecutor(snapshot, map[string]traceprofile.Querier{"18": querier}, traceprofile.Limits{MaxRangeUS: traceprofile.MaxWindowUS(7), MaxOffset: 1000})
			if err != nil {
				t.Fatal(err)
			}
			traceProfileRuntimeValue.Load().executor = executor
			if tc.reloadMode {
				influxdb.MultiTenantMode = !tc.multiTenant
				viper.Set(influxdb.MultiTenantModeConfigPath, !tc.multiTenant)
			}
			metadata.InitMetadata()
			ctx := metadata.InitHashID(context.Background())
			metadata.SetUser(ctx, &metadata.User{TenantID: tc.incoming, SpaceUID: tc.space})
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			body := typedTraceBody("trace_span", tc.domain)
			body = strings.Replace(body, "1758585600000000", "1789646400000000", 1)
			body = strings.Replace(body, "1758672000000000", "1789646460000000", 1)
			c.Request = httptest.NewRequest(http.MethodPost, "/query/ts/raw", bytes.NewBufferString(body)).WithContext(ctx)
			c.Request.Header.Set("X-Bkapi-Authorization", `{"bk_app_code":"kingeye-apm"}`)
			if !tc.unverified {
				c.Set(verifiedJWTAppCodeContextKey, tc.app)
			}
			HandlerQueryRaw(c)
			if tc.code == "OK" {
				var response traceprofile.Response
				if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &response) != nil || response.Total != 1 || len(response.Items) != 1 {
					t.Fatalf("mapped query did not return a complete page: %s", recorder.Body.String())
				}
				member := response.Items[0]["member"].(map[string]any)
				if member["bk_tenant_id"] != tc.target || member["observation_domain_id"] != tc.target+"_kapm" {
					t.Fatal("response lost the data tenant")
				}
			} else if code := decodeTraceProfileError(t, recorder); code != tc.code {
				t.Fatalf("code=%s,want %s", code, tc.code)
			}
			if called != (tc.code == "OK") {
				t.Fatal("rejected request reached storage")
			}
		})
	}
}
