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
	"context"
	"math"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/dorisclient"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

func TestCompileAggregateKeepsTrustedScopeAndExactCount(t *testing.T) {
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "app"
	request.Limit = 10000
	request.Filters = Filters{FieldList: []FilterField{{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"}}}
	request.Aggregation = &CountAggregation{
		Method: "count", IntervalSeconds: 60, TimezoneOffsetSeconds: 8 * 3600,
		GroupBy: []string{"root_service_name"},
	}
	compiled, err := CompileAggregate(request.Profile, testTarget(request.Profile), testScope(t), request, Limits{})
	if err != nil {
		t.Fatalf("valid aggregate rejected: %v", err)
	}
	for _, required := range []string{
		"COUNT(*) AS `count`", "`t1_kapm`.`otel_trace_summary_7`", "`trace_start_time_us`",
		"`bk_tenant_id` = ?", "`bk_biz_id` = ?", "`app_id` = ?", "`root_service_name`", "LIMIT 10001",
	} {
		if !strings.Contains(compiled.Statement, required) {
			t.Fatalf("aggregate SQL missing %q: %s", required, compiled.Statement)
		}
	}
	if len(compiled.Args) < 8 {
		t.Fatalf("expected bound scope, time and bucket inputs: %v", compiled.Args)
	}
	if err := dorisclient.ValidateStatement(compiled.Statement); err != nil {
		t.Fatalf("aggregate SQL was rejected by the readonly client: %v", err)
	}
}

func TestCompileAggregateRejectsUnsupportedDimension(t *testing.T) {
	request := testRequest(metadata.TraceProfileSpan)
	request.Aggregation = &CountAggregation{
		Method: "count", IntervalSeconds: 60, GroupBy: []string{"resource"},
	}
	if _, err := CompileAggregate(request.Profile, testTarget(request.Profile), testScope(t), request, Limits{}); err == nil {
		t.Fatal("JSON dimension was accepted")
	}
}

func TestExecuteAggregateReturnsExactBucketRows(t *testing.T) {
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "app"
	request.Aggregation = &CountAggregation{
		Method: "count", IntervalSeconds: 60, TimezoneOffsetSeconds: 8 * 3600,
		GroupBy: []string{"root_service_name"},
	}
	querier := &fakeQuerier{rows: []map[string]any{{
		"bucket_start_us": request.StartUS, "count": int64(3), "root_service_name": "svc-a",
	}}}
	rows, err := testExecutor(t, querier).ExecuteAggregate(context.Background(), request, testScope(t))
	if err != nil {
		t.Fatalf("aggregate execution rejected trusted rows: %v", err)
	}
	if len(rows) != 1 || rows[0].BucketStartMS != request.StartUS/1000 || rows[0].Count != 3 ||
		rows[0].Dimensions["root_service_name"] != "svc-a" || len(querier.statements) != 1 {
		t.Fatalf("aggregate rows or query count drifted: rows=%+v statements=%v", rows, querier.statements)
	}
}

func TestExecuteAggregateWithoutDimensionsRejectsTruncatedPoints(t *testing.T) {
	request := testRequest(metadata.TraceProfileSpan)
	request.Limit = 1
	request.Aggregation = &CountAggregation{
		Method: "count", IntervalSeconds: 60, GroupBy: []string{},
	}
	querier := &fakeQuerier{rows: []map[string]any{
		{"bucket_start_us": request.StartUS, "count": int64(2)},
		{"bucket_start_us": request.StartUS + 60_000_000, "count": int64(1)},
	}}
	if _, err := testExecutor(t, querier).ExecuteAggregate(context.Background(), request, testScope(t)); err == nil {
		t.Fatal("more buckets than the requested budget were returned as a partial series")
	}
	if !strings.Contains(querier.statements[0], "GROUP BY 1 ORDER BY 1 LIMIT 2") {
		t.Fatalf("un-grouped count SQL is invalid: %s", querier.statements[0])
	}
}

func TestExecuteAggregateRejectsNonFiniteDimension(t *testing.T) {
	request := testRequest(metadata.TraceProfileSpan)
	request.Aggregation = &CountAggregation{
		Method: "count", IntervalSeconds: 60, GroupBy: []string{"duration_us"},
	}
	querier := &fakeQuerier{rows: []map[string]any{{
		"bucket_start_us": request.StartUS, "count": int64(1), "duration_us": math.NaN(),
	}}}
	if _, err := testExecutor(t, querier).ExecuteAggregate(context.Background(), request, testScope(t)); err == nil {
		t.Fatal("non-finite dimension escaped into the JSON response")
	}
}
