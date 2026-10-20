// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package traceprofile

import "testing"

func TestWindowFitsTheInclusiveMillisecondEnd(t *testing.T) {
	start := int64(1_758_585_600_000_000)
	// 调用方发 end_us=(end_ms+1)*1000：整 30 天再加 1 ms。
	if !WindowFits(start, start+30*dayUS+1000, MaxWindowUS(30)) {
		t.Fatal("the maximum product window was rejected")
	}
	if WindowFits(start, start+30*dayUS+1001, MaxWindowUS(30)) || WindowFits(start, start, MaxWindowUS(30)) {
		t.Fatal("a window past the allowance or an empty window was accepted")
	}
}

func TestESSummaryLimitsTakeTheSmallerOfConfigAndBackendCap(t *testing.T) {
	limits := NewESSummaryLimits(10000, 1000)
	if limits.MaxRows != ESSummaryMaxRows || limits.MaxOffset != 1000 {
		t.Fatalf("effective ES Summary limits drifted: %+v", limits)
	}
}

func TestESSummaryListLimitAndSortErrorsAreNotAuthorizationErrors(t *testing.T) {
	request, scope, target, row := esListFixture(t)
	request.Limit = ESSummaryMaxRows + 1
	_, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, NewESSummaryLimits(ESSummaryMaxRows, ESSummaryMaxOffset))
	if ErrorCode(err) != CodeLimitExceeded {
		t.Fatalf("row budget error code = %s, want %s", ErrorCode(err), CodeLimitExceeded)
	}
	request, scope, target, row = esListFixture(t)
	request.OrderBy = []string{"-trace_id", "min_start_time"}
	_, err = ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, NewESSummaryLimits(ESSummaryMaxRows, ESSummaryMaxOffset))
	if ErrorCode(err) != CodeUnsupportedSchemaCapability {
		t.Fatalf("sort error code = %s, want %s", ErrorCode(err), CodeUnsupportedSchemaCapability)
	}
	request, scope, target, row = esListFixture(t)
	request.EndUS = request.StartUS + MaxWindowUS(target.RetentionDays) + 1
	_, err = ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, NewESSummaryLimits(ESSummaryMaxRows, ESSummaryMaxOffset))
	if ErrorCode(err) != CodeScopePartial {
		t.Fatalf("retention window error code = %s, want %s", ErrorCode(err), CodeScopePartial)
	}
}

func TestParseRequestKeepsTheParsedESListPredicate(t *testing.T) {
	body := `{"trace_profile":"trace_summary","trace_contract_version":"trace_query_v2","trace_request":{` +
		`"observation_domain_id":"system_kapm","expected_scope_fingerprint":"` +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + `",` +
		`"start_time":"1758585600000000","end_time":"1758672000000000",` +
		`"filters":{"field_list":[{"field_name":"entry_type","value":["request"],"op":"eq"}],"condition_list":[]},` +
		`"order_by":["-min_start_time","trace_id"],"limit":2,"scene":"global",` +
		`"es_summary_list":{"backend":"elasticsearch","app_id":7,"bk_biz_id":42,"expected_route_version":7}}}`
	request, err := ParseRequest([]byte(body), 65536, 100)
	if err != nil {
		t.Fatal(err)
	}
	// 解析后的筛选随请求携带，后续阶段不再重新解析 Filters。
	request.Filters = Filters{}
	predicate, err := request.ListPredicate()
	if err != nil || len(predicate.Types) != 1 || predicate.Types[0] != "request" {
		t.Fatalf("the parsed predicate was not reused: %+v err=%v", predicate, err)
	}
}
