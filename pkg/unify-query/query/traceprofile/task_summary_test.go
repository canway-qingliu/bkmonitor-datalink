// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package traceprofile

import (
	"context"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/dorisclient"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

func taskSummaryRequest() Request {
	request := testRequest(metadata.TraceProfileSpan)
	request.Limit = 5000
	request.Aggregation = &CountAggregation{Method: "task_summary"}
	request.Filters = Filters{FieldList: []FilterField{
		{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"},
		{FieldName: "service_name", Value: []string{"svc-a"}, Operator: "eq"},
	}, ConditionList: []string{"and"}}
	return request
}

func TestParseRequestAcceptsOnlyClosedTaskSummary(t *testing.T) {
	body := strings.Replace(validRequestJSON(), `"trace_summary"`, `"trace_span"`, 1)
	body = strings.Replace(body, `"trace_query_v2"`, `"trace_query_v3"`, 1)
	body = strings.Replace(body, `"scene":"app",`, "", 1)
	body = strings.Replace(body, `"order_by":["-timestamp","trace_id"],"limit":25,"offset":0`,
		`"order_by":[],"limit":5000,"aggregation":{"method":"task_summary"}`, 1)
	request, err := ParseRequest([]byte(body), 1<<20, 10000)
	if err != nil || request.Aggregation == nil || request.Aggregation.Method != "task_summary" {
		t.Fatalf("valid task summary rejected: request=%+v err=%v", request, err)
	}
	for name, invalid := range map[string]string{
		"extra SQL":       strings.Replace(body, `"method":"task_summary"`, `"method":"task_summary","sql":"SELECT *"`, 1),
		"zero interval":   strings.Replace(body, `"method":"task_summary"`, `"method":"task_summary","interval_seconds":0`, 1),
		"null group":      strings.Replace(body, `"method":"task_summary"`, `"method":"task_summary","group_by":null`, 1),
		"missing app":     strings.Replace(body, `[{"field_name":"app_id","value":["7"],"op":"eq"}]`, `[]`, 1),
		"invalid app":     strings.Replace(body, `"value":["7"]`, `"value":["unknown"]`, 1),
		"wrong profile":   strings.Replace(body, `"trace_span"`, `"trace_summary"`, 1),
		"too many groups": strings.Replace(body, `"limit":5000`, `"limit":5001`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseRequest([]byte(invalid), 1<<20, 10000); err == nil {
				t.Fatal("unsafe task summary request was accepted")
			}
		})
	}
}

func TestCompileTaskSummaryKeepsTrustedScopeAndFixedClassifier(t *testing.T) {
	request := taskSummaryRequest()
	compiled, err := CompileTaskSummary(testTarget(request.Profile), testScope(t), request, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"`span_date` >= ?", "`start_time_us` < ?", "`bk_tenant_id` = ?", "`app_id` = ?",
		"`service_name` = ?", `JSON_EXTRACT_STRING(attributes, '$."plugind.action_uid"')`,
		`JSON_EXTRACT_STRING(attributes, '$."celery.action"')`, "ROW_NUMBER() OVER", "LIMIT 5001",
	} {
		if !strings.Contains(compiled.Statement, fragment) {
			t.Fatalf("task summary SQL missing %q: %s", fragment, compiled.Statement)
		}
	}
	if strings.Contains(compiled.Statement, "svc-a") || len(compiled.Args) < 10 {
		t.Fatalf("service filter was interpolated or trusted predicates missing: args=%v", compiled.Args)
	}
	if err := dorisclient.ValidateStatement(compiled.Statement); err != nil {
		t.Fatalf("readonly SQL rejected: %v", err)
	}
	offset := int64(0)
	request.Offset = &offset
	if _, err := CompileTaskSummary(testTarget(request.Profile), testScope(t), request, Limits{}); err == nil {
		t.Fatal("task summary compiler accepted an unparsed offset")
	}
}

type taskSummaryQuerier struct {
	rows    []map[string]any
	queries int
}

func (q *taskSummaryQuerier) Query(_ context.Context, _ string, _ ...any) (dorisclient.Result, error) {
	q.queries++
	return dorisclient.Result{Rows: q.rows}, nil
}

func TestExecuteTaskSummaryReturnsExactMetricsAndRejectsOverflow(t *testing.T) {
	request := taskSummaryRequest()
	request.Limit = 1
	querier := &taskSummaryQuerier{rows: []map[string]any{{
		"service_name": "svc-a", "task_type": "async", "task_name": "plugind.action.reconcile",
		"execution_count": int64(2), "failure_count": int64(1), "duration_sum_us": int64(400000),
		"duration_max_us": int64(300000), "last_called_us": request.StartUS,
		"latest_trace_id": "aabb", "invalid_count": int64(0),
	}}}
	rows, err := testExecutor(t, querier).ExecuteTaskSummary(context.Background(), request, testScope(t))
	if err != nil || len(rows) != 1 || rows[0].ExecutionCount != 2 || rows[0].AvgDurationMs != 200 ||
		rows[0].MaxDurationMs != 300 || rows[0].FailureCount != 1 || rows[0].LatestTraceID != "aabb" || querier.queries != 1 {
		t.Fatalf("task summary mismatch: rows=%+v queries=%d err=%v", rows, querier.queries, err)
	}
	querier.rows = append(querier.rows, querier.rows[0])
	if _, err := testExecutor(t, querier).ExecuteTaskSummary(context.Background(), request, testScope(t)); err == nil {
		t.Fatal("truncated task groups were accepted")
	}
}
