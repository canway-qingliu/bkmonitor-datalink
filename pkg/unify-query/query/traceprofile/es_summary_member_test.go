// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package traceprofile

import (
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

const esMemberStartUS int64 = 1721024820000000

func esMemberFixture(t *testing.T) (Request, Scope, ESSummaryTarget, map[string]any) {
	t.Helper()
	traceID := strings.Repeat("a", 32)
	scope, err := ResolveScope("system", "bkcc__42", "system_kapm")
	if err != nil {
		t.Fatal(err)
	}
	request := Request{
		Profile: metadata.TraceProfileSummary, Scene: "global",
		StartUS: esMemberStartUS, EndUS: esMemberStartUS + 60000000, Limit: 2,
		MemberLookup: &MemberLookup{
			Backend: MemberBackendElasticsearch, TraceID: traceID,
			ExpectedRouteVersion: 7, AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 42}},
		},
	}
	target := ESSummaryTarget{
		BkTenantID: "system", BkBizID: 42, AppID: 7, AppName: "checkout",
		ObservationDomainID: "system_kapm", ResultTableID: "apm_global.trace_summary_v1_7",
		StorageClusterID: 9, RetentionDays: 30, PublishedVersion: 7,
	}
	row := map[string]any{
		"bk_tenant_id": "system", "observation_domain_id": "system_kapm",
		"biz_id": "42", "app_id": int64(7), "app_name": "checkout", "trace_id": traceID,
		"trace_date": "2024-07-15", "min_start_time": esMemberStartUS,
		"max_end_time": esMemberStartUS + 1000, "trace_duration": int64(1000),
		"span_count": int64(1), "error_count": int64(0), "service_count": int64(1),
		"hierarchy_count": int64(0), "span_min_duration": int64(1000), "span_max_duration": int64(1000),
		"event_time_ms": int64(0), "root_service": "checkout",
		"root_service_span_id": "1112131415161718", "root_service_span_name": "GET /",
		"root_service_kind": int64(2), "root_service_category": "http", "root_service_status_code": int64(200),
		"root_span_id": "1112131415161718", "root_span_name": "GET /",
		"root_span_service": "checkout", "root_span_kind": int64(2),
		"category_statistics": map[string]any{
			"http": int64(1), "rpc": int64(0), "db": int64(0),
			"messaging": int64(0), "async_backend": int64(0), "other": int64(0),
		},
		"kind_statistics": map[string]any{
			"async": int64(0), "sync": int64(1), "interval": int64(0), "unspecified": int64(0),
		},
		"partial": true, "time": esMemberStartUS + 1000000,
	}
	return request, scope, target, row
}

func TestShapeESSummaryMemberPageRestoresCompleteTypedSummary(t *testing.T) {
	request, scope, target, row := esMemberFixture(t)
	page, err := ShapeESSummaryMemberPage([]map[string]any{row}, 1, request, scope, target)
	if err != nil || page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("valid ES member was not shaped: page=%+v err=%v", page, err)
	}
	item := page.Items[0]
	values := item["values"].(map[string]any)
	if item["app_id"] != "7" || values["bk_biz_id"] != int64(42) ||
		values["trace_start_time_us"] != esMemberStartUS || values["trace_date"] != "2024-07-15" ||
		values["computed_at_ms"] != (esMemberStartUS+1000000)/1000 || values["partial"] != true {
		t.Fatalf("ES member lost typed Summary fields: %+v", item)
	}
}

func TestShapeESSummaryMemberPageRestoresRawUQFlattenedStatistics(t *testing.T) {
	request, scope, target, row := esMemberFixture(t)
	for _, field := range []string{"category_statistics", "kind_statistics"} {
		statistics := row[field].(map[string]any)
		delete(row, field)
		for key, value := range statistics {
			row[field+"."+key] = value
		}
	}
	page, err := ShapeESSummaryMemberPage([]map[string]any{row}, 1, request, scope, target)
	if err != nil {
		t.Fatalf("real raw UQ flattened statistics were rejected: %v", err)
	}
	values := page.Items[0]["values"].(map[string]any)
	if len(values["category_statistics"].(map[string]any)) != 6 ||
		len(values["kind_statistics"].(map[string]any)) != 4 {
		t.Fatalf("flattened ES statistics were truncated: %+v", values)
	}
}

func TestShapeESSummaryMemberPageRejectsTruncatedOrConflictingStatistics(t *testing.T) {
	request, scope, target, row := esMemberFixture(t)
	delete(row["category_statistics"].(map[string]any), "other")
	if _, err := ShapeESSummaryMemberPage([]map[string]any{row}, 1, request, scope, target); err == nil {
		t.Fatal("incomplete ES category statistics were accepted")
	}
	_, _, _, row = esMemberFixture(t)
	row["kind_statistics.sync"] = int64(1)
	if _, err := ShapeESSummaryMemberPage([]map[string]any{row}, 1, request, scope, target); err == nil {
		t.Fatal("nested and flattened ES statistics conflict was accepted")
	}
}

func TestShapeESSummaryMemberPageRejectsIncompleteOrForeignRows(t *testing.T) {
	request, scope, target, row := esMemberFixture(t)
	for name, mutate := range map[string]func(map[string]any){
		"wrong tenant":    func(value map[string]any) { value["bk_tenant_id"] = "other" },
		"wrong app":       func(value map[string]any) { value["app_id"] = int64(8) },
		"wrong date":      func(value map[string]any) { value["trace_date"] = "2024-07-16" },
		"out of window":   func(value map[string]any) { value["min_start_time"] = request.EndUS },
		"unversioned":     func(value map[string]any) { value["time"] = int64(0) },
		"missing partial": func(value map[string]any) { delete(value, "partial") },
	} {
		t.Run(name, func(t *testing.T) {
			value := make(map[string]any, len(row))
			for field, item := range row {
				value[field] = item
			}
			mutate(value)
			if _, err := ShapeESSummaryMemberPage([]map[string]any{value}, 1, request, scope, target); err == nil {
				t.Fatal("foreign or incomplete ES Summary row was accepted")
			}
		})
	}
	if _, err := ShapeESSummaryMemberPage([]map[string]any{row}, 2, request, scope, target); err == nil {
		t.Fatal("incomplete exact ES member page was accepted")
	}
	if _, err := ShapeESSummaryMemberPage([]map[string]any{row}, 3, request, scope, target); err == nil {
		t.Fatal("over-budget exact ES member page was accepted")
	}
}
