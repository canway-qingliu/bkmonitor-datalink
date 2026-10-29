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
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	uqjson "github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/internal/json"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

func completeSpanRow() map[string]any {
	row := make(map[string]any, len(spanProjection)+3)
	for _, field := range spanProjection {
		row[field] = "value"
	}
	row["app_id"] = "app-a"
	row["span_date"] = "2026-09-17"
	row["trace_id"] = strings.Repeat("a", 32)
	row["span_id"] = strings.Repeat("b", 16)
	row["space_uid"] = "bkcc__42"
	row["bk_biz_id"] = float64(42)
	row["data_id"] = json.Number("101")
	row["parent_span_id"] = ""
	row["trace_state"] = ""
	row["span_kind"] = float64(3)
	row["status_code"] = float64(2)
	row["start_time_us"] = json.Number("1789646400000000")
	row["end_time_us"] = json.Number("1789646400025000")
	row["duration_us"] = float64(25000)
	row["event_time_ms"] = nil
	row["service_namespace"] = nil
	row["service_instance_id"] = nil
	row["deployment_environment"] = nil
	row["resource"] = map[string]any{"counter": json.Number("9007199254740993")}
	row["attributes"] = map[string]any{"http.route": "/orders"}
	row["events"] = []any{}
	row["links"] = []any{}
	row["ingest_time"] = int64(1789646400000)
	row[metadata.KeyIndex] = "42_otel_trace_span"
	row[metadata.KeyTableID] = "42_otel_trace_span.doris"
	row[metadata.KeyDataLabel] = ""
	row["_value_"] = float64(1789646400000000)
	row["_timestamp_"] = float64(1789646400000000)
	return row
}

func completeSummaryRow() map[string]any {
	row := make(map[string]any, len(summaryProjection)+3)
	for _, field := range summaryProjection {
		row[field] = "value"
	}
	row["trace_date"] = "2026-09-17"
	row["app_id"] = "app-a"
	row["trace_id"] = strings.Repeat("a", 32)
	row["bk_biz_id"] = float64(42)
	for _, field := range []string{
		"trace_start_time_us", "trace_end_time_us", "duration_us", "span_count", "error_count",
		"service_count", "hierarchy_count", "span_min_duration_us", "span_max_duration_us",
		"event_time_ms", "root_service_kind", "root_span_kind", "computed_at_ms",
	} {
		row[field] = float64(1)
	}
	row["root_service_name"] = nil
	row["root_span_service_name"] = nil
	row["root_service_status_code"] = json.Number("503")
	row["category_statistics"] = map[string]any{"http": json.Number("2")}
	row["kind_statistics"] = map[string]any{"sync": json.Number("2")}
	row["partial"] = true
	row[metadata.KeyIndex] = "42_otel_trace_summary"
	row[metadata.KeyTableID] = "42_otel_trace_summary.doris"
	row["_value_"] = float64(1)
	row["_timestamp_"] = float64(1)
	return row
}

func TestShapeRowsBuildsClosedKAPMSpanAndSummaryItems(t *testing.T) {
	spanInput := completeSpanRow()
	spanRows, err := ShapeRows(metadata.TraceProfileSpan, []map[string]any{spanInput}, "system")
	if err != nil || len(spanRows) != 1 {
		t.Fatalf("Span row shaping failed: rows=%+v err=%v", spanRows, err)
	}
	span := spanRows[0]
	if len(span) != 4 || span["trace_id"] != spanInput["trace_id"] || span["span_id"] != spanInput["span_id"] {
		t.Fatalf("Span logical row keys are not closed: %+v", span)
	}
	member, ok := span["member"].(map[string]any)
	if !ok || !reflect.DeepEqual(member, map[string]any{
		"bk_tenant_id": "system",
		"data_id":      int64(101), "bk_biz_id": int64(42), "app_id": "app-a",
	}) {
		t.Fatalf("Span member identity was not typed correctly: %+v", span)
	}
	spanValues, ok := span["values"].(map[string]any)
	if !ok || spanValues["duration_us"] != int64(25000) || spanValues["resource"] == nil {
		t.Fatalf("Span values were not preserved: %+v", span)
	}
	if _, leaked := spanValues[metadata.KeyIndex]; leaked {
		t.Fatal("UQ internal field leaked into Span values")
	}
	if _, original := spanInput[metadata.KeyIndex]; !original {
		t.Fatal("row shaper mutated its input")
	}
	wire, err := uqjson.Marshal(span)
	if err != nil || !strings.Contains(string(wire), `"counter":9007199254740993`) {
		t.Fatalf("large nested integer was not serialized exactly: %s err=%v", wire, err)
	}

	summaryRows, err := ShapeRows(metadata.TraceProfileSummary, []map[string]any{completeSummaryRow()}, "system")
	if err != nil || len(summaryRows) != 1 {
		t.Fatalf("Summary row shaping failed: rows=%+v err=%v", summaryRows, err)
	}
	summary := summaryRows[0]
	if len(summary) != 3 || summary["app_id"] != "app-a" || summary["trace_id"] != strings.Repeat("a", 32) {
		t.Fatalf("Summary logical row keys are not closed: %+v", summary)
	}
	values, ok := summary["values"].(map[string]any)
	if !ok || values["partial"] != true || values["bk_biz_id"] != int64(42) || values["span_count"] != int64(1) {
		t.Fatalf("Summary values lost native types: %+v", summary)
	}
	if _, leaked := values[metadata.KeyTableID]; leaked {
		t.Fatal("UQ internal field leaked into Summary values")
	}
}

func TestShapeRowsFailsWholeBatchOnMissingOrInvalidPhysicalValues(t *testing.T) {
	valid := completeSummaryRow()
	invalid := completeSummaryRow()
	delete(invalid, "span_count")
	if rows, err := ShapeRows(metadata.TraceProfileSummary, []map[string]any{valid, invalid}, "system"); err == nil || rows != nil {
		t.Fatalf("partial Summary batch escaped: rows=%+v err=%v", rows, err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"unexpected physical field":  func(row map[string]any) { row["secret"] = "x" },
		"imprecise large integer":    func(row map[string]any) { row["span_count"] = float64(9007199254740994) },
		"invalid partial":            func(row map[string]any) { row["partial"] = "true" },
		"wrong JSON object":          func(row map[string]any) { row["category_statistics"] = []any{1} },
		"null summary event time":    func(row map[string]any) { row["event_time_ms"] = nil },
		"whitespace app identity":    func(row map[string]any) { row["app_id"] = " app-a " },
		"retired observation domain": func(row map[string]any) { row["observation_domain_id"] = "other_kapm" },
		"negative span count":        func(row map[string]any) { row["span_count"] = int64(-1) },
	} {
		t.Run(name, func(t *testing.T) {
			row := completeSummaryRow()
			mutate(row)
			if _, err := ShapeRows(metadata.TraceProfileSummary, []map[string]any{row}, "system"); err == nil {
				t.Fatal("invalid Summary row was accepted")
			}
		})
	}
}

func TestShapeSpanRejectsBusinessSpaceMismatch(t *testing.T) {
	row := completeSpanRow()
	row["space_uid"] = "bkcc__43"
	if _, err := ShapeRows(metadata.TraceProfileSpan, []map[string]any{row}, "system"); err == nil {
		t.Fatal("Span row with a foreign business space was accepted")
	}
}
