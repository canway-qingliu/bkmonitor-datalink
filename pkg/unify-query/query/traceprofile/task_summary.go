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
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

const taskSummaryMaxGroups = 5000

type TaskSummaryRow struct {
	ServiceName    string  `json:"service_name"`
	TaskType       string  `json:"task_type"`
	TaskName       string  `json:"task_name"`
	ExecutionCount int64   `json:"execution_count"`
	FailureCount   int64   `json:"failure_count"`
	AvgDurationMs  float64 `json:"avg_duration_ms"`
	MaxDurationMs  float64 `json:"max_duration_ms"`
	LastCalledMs   int64   `json:"last_called_ms"`
	LatestTraceID  string  `json:"latest_trace_id"`
}

func validTaskSummaryFilters(filters Filters) bool {
	if len(filters.FieldList) < 1 || len(filters.FieldList) > 2 ||
		len(filters.ConditionList) != len(filters.FieldList)-1 {
		return false
	}
	app := filters.FieldList[0]
	if app.FieldName != "app_id" || app.Operator != "eq" || len(app.Value) != 1 || app.Value[0] == "" ||
		app.IsWildcard || app.IsPrefix || app.IsSuffix {
		return false
	}
	appID, err := strconv.ParseInt(app.Value[0], 10, 64)
	if err != nil || appID <= 0 || strconv.FormatInt(appID, 10) != app.Value[0] {
		return false
	}
	if len(filters.FieldList) == 1 {
		return true
	}
	service := filters.FieldList[1]
	if filters.ConditionList[0] != "and" || service.FieldName != "service_name" ||
		service.Operator != "eq" || len(service.Value) < 1 || len(service.Value) > 20 ||
		service.IsWildcard || service.IsPrefix || service.IsSuffix {
		return false
	}
	for _, value := range service.Value {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	return true
}

// CompileTaskSummary keeps the published Doris route and buildWhere's complete
// tenant/business/app/time scope. The task classifier is fixed server-side.
func CompileTaskSummary(target DorisTarget, scope Scope, request Request, limits Limits) (CompiledAggregate, error) {
	if request.Profile != metadata.TraceProfileSpan || request.Aggregation == nil ||
		request.Aggregation.Method != "task_summary" || request.Limit < 1 || request.Limit > taskSummaryMaxGroups ||
		request.Offset != nil || len(request.OrderBy) != 0 || request.MemberLookup != nil || request.ESSummaryList != nil ||
		request.Aggregation.IntervalSeconds != 0 || request.Aggregation.TimezoneOffsetSeconds != 0 ||
		request.Aggregation.GroupBy != nil ||
		request.EndUS <= request.StartUS || !validTaskSummaryFilters(request.Filters) ||
		!WindowFits(request.StartUS, request.EndUS, MaxWindowUS(1)) ||
		limits.MaxRangeUS > 0 && !WindowFits(request.StartUS, request.EndUS, limits.MaxRangeUS) {
		return CompiledAggregate{}, reject(CodePredicateConflict, "后台任务聚合范围或字段非法")
	}
	if !matchesPublishedPhysicalTable(request.Profile, target, scope, request) {
		return CompiledAggregate{}, reject(CodeRouteNotFound, "后台任务聚合读目标与 Span profile 不匹配")
	}
	where, args, err := buildWhere(request.Profile, scope, request)
	if err != nil {
		return CompiledAggregate{}, err
	}
	statement := fmt.Sprintf(taskSummarySQL, quoteIdentifier(target.Database)+"."+quoteIdentifier(target.Table), where, request.Limit+1)
	return CompiledAggregate{Statement: statement, Args: args}, nil
}

const taskSummarySQL = `WITH scoped AS (
  SELECT service_name, trace_id, span_name, span_kind, parent_span_id, status_code, start_time_us, duration_us,
    JSON_EXTRACT_STRING(attributes, '$."plugind.action_uid"') AS action_uid,
    JSON_EXTRACT_STRING(attributes, '$."plugind.resource_uid"') AS resource_uid,
    JSON_EXTRACT_STRING(attributes, '$."celery.action"') AS celery_action,
    JSON_EXTRACT_STRING(attributes, '$."celery.task_name"') AS celery_task_name,
    JSON_EXTRACT_STRING(attributes, '$."scheduling.system"') AS scheduling_system,
    JSON_EXTRACT_STRING(attributes, '$."scheduling.operation"') AS scheduling_operation,
    JSON_EXTRACT_STRING(attributes, '$."job.system"') AS job_system,
    JSON_EXTRACT_STRING(attributes, '$."job.name"') AS job_name
  FROM %s WHERE %s AND span_kind NOT IN (2, 3, 4)
), marked AS (
  SELECT scoped.*, CASE
    WHEN span_kind = 1 AND (TRIM(parent_span_id) IN ('', '-1') OR TRIM(parent_span_id) REGEXP '^0+$')
      AND TRIM(COALESCE(action_uid, '')) <> '' THEN 'plugind_action'
    WHEN span_kind = 1 AND (TRIM(parent_span_id) IN ('', '-1') OR TRIM(parent_span_id) REGEXP '^0+$')
      AND TRIM(COALESCE(resource_uid, '')) <> '' THEN 'plugind_resource'
    WHEN LOWER(COALESCE(celery_action, '')) = 'run' THEN 'celery'
    WHEN span_kind = 5 THEN ''
    WHEN scheduling_system = 'celery' AND scheduling_operation = 'tick' THEN ''
    WHEN LOWER(COALESCE(NULLIF(scheduling_system, ''), job_system, '')) IN
      ('apscheduler', 'spring', 'spring-scheduling', 'spring_scheduling', 'quartz', 'elasticjob', 'xxl-job')
      OR LOWER(COALESCE(span_name, '')) REGEXP
      '^(apscheduler|springscheduled|springscheduling|quartz-scheduler|quartz|elasticjob|xxl-job)([/.: ]|$)'
      THEN 'scheduled'
    ELSE '' END AS task_class
  FROM scoped
), classified AS (
  SELECT marked.*,
    CASE WHEN task_class IN ('plugind_action', 'plugind_resource', 'celery') THEN 'async'
      WHEN task_class = 'scheduled' THEN 'scheduled' ELSE '' END AS task_type,
    CASE task_class
      WHEN 'plugind_action' THEN COALESCE(NULLIF(TRIM(span_name), ''), 'plugind.action')
      WHEN 'plugind_resource' THEN COALESCE(NULLIF(TRIM(span_name), ''), 'plugind.resource')
      WHEN 'celery' THEN COALESCE(NULLIF(TRIM(celery_task_name), ''), TRIM(span_name))
      WHEN 'scheduled' THEN COALESCE(NULLIF(TRIM(job_name), ''), TRIM(span_name))
      ELSE '' END AS task_name
  FROM marked
), ranked AS (
  SELECT classified.*, ROW_NUMBER() OVER (
    PARTITION BY service_name, task_type, task_name ORDER BY start_time_us DESC, trace_id DESC
  ) AS latest_rank
  FROM classified WHERE task_type <> '' AND task_name <> ''
)
SELECT service_name, task_type, task_name, COUNT(*) AS execution_count,
  SUM(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) AS failure_count,
  SUM(duration_us) AS duration_sum_us, MAX(duration_us) AS duration_max_us,
  MAX(CASE WHEN latest_rank = 1 THEN start_time_us END) AS last_called_us,
  MAX(CASE WHEN latest_rank = 1 THEN trace_id END) AS latest_trace_id,
  SUM(CASE WHEN duration_us < 0 THEN 1 ELSE 0 END) AS invalid_count
FROM ranked GROUP BY service_name, task_type, task_name LIMIT %d`

// ExecuteTaskSummary rejects incomplete or malformed buckets instead of
// presenting a partial task list as an exact total.
func (e *Executor) ExecuteTaskSummary(ctx context.Context, request Request, scope Scope) ([]TaskSummaryRow, error) {
	if e == nil || e.snapshot == nil {
		return nil, reject(CodeRouteNotFound, "后台任务聚合读路由尚未配置")
	}
	// 1. 只在受信应用所属的已发布 Doris Span 目标上执行。
	route, err := e.snapshot.ResolveForApplication(
		scope.BkTenantID, scope.BkBizID, exactSpanRouteAppID(request), request.Profile, scope.ObservationDomainID,
	)
	if err != nil {
		return nil, err
	}
	connection, ok := e.connections[route.Doris.ClusterID]
	if !ok {
		return nil, reject(CodeRouteNotFound, "后台任务聚合目标集群缺少只读连接")
	}
	compiled, err := CompileTaskSummary(*route.Doris, scope, request, e.limits)
	if err != nil {
		return nil, err
	}
	// 2. 一次 SQL 返回有界全集，超过预算时不展示近似分组。
	result, err := connection.Query(ctx, compiled.Statement, compiled.Args...)
	if err != nil {
		return nil, queryFailure(err)
	}
	if len(result.Rows) > int(request.Limit) {
		return nil, reject(CodeLimitExceeded, "后台任务聚合分组超过预算")
	}
	// 3. 逐桶验证身份、次数和微秒时间后才组装与 ES 相同的响应行。
	rows := make([]TaskSummaryRow, 0, len(result.Rows))
	for index, raw := range result.Rows {
		if len(raw) != 10 {
			return nil, reject(CodePartialFailure, "后台任务聚合第 %d 行字段数量非法", index)
		}
		service, serviceOK := raw["service_name"].(string)
		taskType, typeOK := raw["task_type"].(string)
		name, nameOK := raw["task_name"].(string)
		traceID, traceOK := raw["latest_trace_id"].(string)
		count, countErr := exactTraceInt64(raw["execution_count"])
		failures, failureErr := exactTraceInt64(raw["failure_count"])
		sum, sumErr := exactTraceInt64(raw["duration_sum_us"])
		maximum, maxErr := exactTraceInt64(raw["duration_max_us"])
		latest, latestErr := exactTraceInt64(raw["last_called_us"])
		invalid, invalidErr := exactTraceInt64(raw["invalid_count"])
		if !serviceOK || service == "" || !typeOK || taskType != "async" && taskType != "scheduled" ||
			!nameOK || name == "" || !traceOK || traceID == "" || countErr != nil || count < 1 ||
			failureErr != nil || failures < 0 || failures > count || sumErr != nil || sum < 0 ||
			maxErr != nil || maximum < 0 || latestErr != nil || latest < request.StartUS || latest >= request.EndUS ||
			invalidErr != nil || invalid != 0 {
			return nil, reject(CodePartialFailure, "后台任务聚合第 %d 行值非法", index)
		}
		rows = append(rows, TaskSummaryRow{
			ServiceName: service, TaskType: taskType, TaskName: name, ExecutionCount: count,
			FailureCount: failures, AvgDurationMs: math.RoundToEven(float64(sum)/float64(count)/10) / 100,
			MaxDurationMs: math.RoundToEven(float64(maximum)/10) / 100,
			LastCalledMs:  latest / 1000, LatestTraceID: traceID,
		})
	}
	return rows, nil
}

type TaskSummaryResponse struct {
	TraceContractVersion string                `json:"trace_contract_version"`
	TraceProfile         metadata.TraceProfile `json:"trace_profile"`
	ScopeFingerprint     string                `json:"scope_fingerprint"`
	ProductState         any                   `json:"product_state"`
	Buckets              []TaskSummaryRow      `json:"buckets"`
}

func NewTaskSummaryResponse(request Request, rows []TaskSummaryRow) TaskSummaryResponse {
	if rows == nil {
		rows = []TaskSummaryRow{}
	}
	return TaskSummaryResponse{
		TraceContractVersion: aggregateContractVersion, TraceProfile: request.Profile,
		ScopeFingerprint: request.ScopeFingerprint, ProductState: nil, Buckets: rows,
	}
}
