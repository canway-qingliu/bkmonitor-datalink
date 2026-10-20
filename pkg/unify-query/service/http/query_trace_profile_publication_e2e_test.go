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
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
)

// esSummaryListBody 是一个 ES Summary 列表请求；filters 为 field_list/condition_list 的 JSON 片段。
func esSummaryListBody(startUS, endUS, filters, parentPairs string) string {
	body := `{"trace_profile":"trace_summary","trace_contract_version":"trace_query_v2","trace_request":{` +
		`"observation_domain_id":"system_kapm","expected_scope_fingerprint":"` + strings.Repeat("a", 64) + `",` +
		`"start_time":"` + startUS + `","end_time":"` + endUS + `",` +
		`"filters":` + filters + `,"order_by":["-min_start_time","trace_id"],"limit":2,"scene":"global",` +
		`"es_summary_list":{"backend":"elasticsearch","app_id":7,"bk_biz_id":42,"expected_route_version":7`
	if parentPairs != "" {
		body += `,"parent_allowed_pairs":` + parentPairs
	}
	return body + `}}}`
}

// 发布文档 -> 运行时 -> 请求：发布 entry_index_version=2 的目标后，跨应用父关系筛选必须在运行时生效。
func TestPublishedEntryIndexVersionReachesESListAtRuntime(t *testing.T) {
	document := strings.ReplaceAll(tracePublicationFor("system"), "132", "42")
	document = strings.Replace(document, `"retention_days":30}`, `"retention_days":30,"entry_index_version":2}`, 1)
	withConsulTracePublication(t, map[string]string{"system": document})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	withTraceProfileAuth(t, true, "kingeye-apm")
	traceProfileRuntimeValue.Load().memberLookupAllowedAppCodes = []string{"kingeye-apm"}
	ownerKeysPushed := false
	traceProfileRuntimeValue.Load().esMemberReader = func(_ context.Context, query *structured.QueryTs) (
		int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error,
	) {
		for _, field := range query.QueryList[0].Conditions.FieldList {
			if field.DimensionName == "entry_parent_owner_keys" {
				ownerKeysPushed = true
			}
		}
		return 0, nil, nil, []metadata.RouteInfo{{
			TableID: "apm_global.trace_summary_v1_7", StorageType: metadata.ElasticsearchStorageType, StorageID: "9",
		}}, nil
	}
	filters := `{"field_list":[{"field_name":"entry_parent_state","value":["observed"],"op":"eq"}],"condition_list":[]}`
	body := esSummaryListBody("1758585600000000", "1758672000000000", filters, `[{"app_id":7,"bk_biz_id":42},{"app_id":8,"bk_biz_id":42}]`)
	recorder := runTraceProfileRequest(t, body, "kingeye-apm")
	if recorder.Code != http.StatusOK || !ownerKeysPushed {
		t.Fatalf("published entry index version 2 did not enable parent filtering: status=%d pushed=%v body=%s",
			recorder.Code, ownerKeysPushed, recorder.Body.String())
	}
}

// F3：处理入口与下游检查使用同一个窗口函数，产品最大窗口（30 天加 1 ms）不被入口拒绝。
func TestESListAcceptsTheMaximumProductWindow(t *testing.T) {
	document := strings.ReplaceAll(tracePublicationFor("system"), "132", "42")
	withConsulTracePublication(t, map[string]string{"system": document})
	config := viper.Get(TraceProfileConfigPath).(map[string]any)
	config["max_range_days"] = 30
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	withTraceProfileAuth(t, true, "kingeye-apm")
	traceProfileRuntimeValue.Load().memberLookupAllowedAppCodes = []string{"kingeye-apm"}
	called := false
	traceProfileRuntimeValue.Load().esMemberReader = func(_ context.Context, query *structured.QueryTs) (
		int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error,
	) {
		called = true
		return 0, nil, nil, []metadata.RouteInfo{{
			TableID: "apm_global.trace_summary_v1_7", StorageType: metadata.ElasticsearchStorageType, StorageID: "9",
		}}, nil
	}
	start := int64(1758585600000000)
	end := start + 30*86_400_000_000 + 1000
	body := esSummaryListBody(strconv.FormatInt(start, 10), strconv.FormatInt(end, 10),
		`{"field_list":[],"condition_list":[]}`, "")
	recorder := runTraceProfileRequest(t, body, "kingeye-apm")
	if recorder.Code != http.StatusOK || !called {
		t.Fatalf("the maximum product window was rejected: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

// F9：ES Summary 的 offset 上限来自运行时配置预算，只在一处检查。
func TestESListOffsetLimitComesFromRuntimeConfig(t *testing.T) {
	document := strings.ReplaceAll(tracePublicationFor("system"), "132", "42")
	withConsulTracePublication(t, map[string]string{"system": document})
	if err := reloadTraceProfilePublishedRuntime(); err != nil {
		t.Fatal(err)
	}
	withTraceProfileAuth(t, true, "kingeye-apm")
	traceProfileRuntimeValue.Load().memberLookupAllowedAppCodes = []string{"kingeye-apm"}
	body := esSummaryListBody("1758585600000000", "1758672000000000", `{"field_list":[],"condition_list":[]}`, "")
	body = strings.Replace(body, `"limit":2,`, `"limit":2,"offset":1001,`, 1)
	recorder := runTraceProfileRequest(t, body, "kingeye-apm")
	if code := decodeTraceProfileError(t, recorder); code != "TRACE_LIMIT_EXCEEDED" {
		t.Fatalf("offset past the configured budget returned %s", code)
	}
}
