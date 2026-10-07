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
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/dorisclient"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

// Querier 是只读查询的最小依赖面，便于用假实现固定执行语义。
type Querier interface {
	Query(ctx context.Context, statement string, args ...any) (dorisclient.Result, error)
}

// Page 是直连读取的一页：行已按冻结行合同整形，total 是同条件真实总数。
type Page struct {
	Items []map[string]any
	Total int64
}

// Executor 在可信读路由快照与只读连接之间做封闭的执行编排：
// 路由缺失、目标集群无连接、编译失败、查询失败、行合同不符都明确失败。
type Executor struct {
	snapshot    *RouteSnapshot
	connections map[string]Querier
	limits      Limits
}

// NewExecutor 冻结读路由、连接与预算；连接必须覆盖路由引用到的集群。
func NewExecutor(snapshot *RouteSnapshot, connections map[string]Querier, limits Limits) (*Executor, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("Trace 读路由尚未配置")
	}
	if len(connections) == 0 && (len(snapshot.routes) > 0 || len(snapshot.esSummaryRoutes) == 0) {
		return nil, fmt.Errorf("Trace Doris 只读连接尚未配置")
	}
	frozen := make(map[string]Querier, len(connections))
	for clusterID, connection := range connections {
		if clusterID == "" || connection == nil {
			return nil, fmt.Errorf("Trace Doris 只读连接非法")
		}
		frozen[clusterID] = connection
	}
	return &Executor{snapshot: snapshot, connections: frozen, limits: limits}, nil
}

// RouteVersion 是当前执行所依据的路由快照版本，用于回执与排障。
func (e *Executor) RouteVersion() int64 {
	return e.snapshot.Version()
}

// ResolveESSummaryTarget 暴露同一版本快照中的 ES Summary 应用目标。
func (e *Executor) ResolveESSummaryTarget(tenantID string, bizID, appID int64, observationDomainID string) (ESSummaryTarget, error) {
	if e == nil || e.snapshot == nil {
		return ESSummaryTarget{}, reject(CodeRouteNotFound, "ES Summary 读路由尚未配置")
	}
	return e.snapshot.ResolveESSummary(tenantID, bizID, appID, observationDomainID)
}

// Execute 执行一次列表查询。
func (e *Executor) Execute(ctx context.Context, request Request, scope Scope) (Page, error) {
	if request.MemberLookup != nil && request.MemberLookup.Backend == MemberBackendElasticsearch {
		return Page{}, reject(CodeUnsupportedSchemaCapability, "ES Summary 成员查询不得进入 Doris 执行器")
	}
	route, err := e.snapshot.Resolve(scope.BkTenantID, scope.BkBizID, request.Profile, scope.ObservationDomainID)
	if err != nil {
		return Page{}, err
	}
	if request.MemberLookup != nil && (route.PublishedVersion == 0 ||
		request.MemberLookup.ExpectedRouteVersion != route.PublishedVersion) {
		return Page{}, reject(CodeScopePartial, "Summary 成员查询的单租户发布路由版本尚未在 UQ 生效")
	}
	connection, ok := e.connections[route.Doris.ClusterID]
	if !ok {
		return Page{}, reject(CodeRouteNotFound, "Trace 目标集群缺少只读连接: %s", route.Doris.ClusterID)
	}
	compiled, err := CompileList(request.Profile, *route.Doris, scope, request, e.limits)
	if err != nil {
		return Page{}, err
	}
	if request.MemberLookup == nil {
		return executeSnapshotList(ctx, request, connection, compiled)
	}

	// 精确成员查询先读 total，超预算时不读取成员行。
	countResult, err := connection.Query(ctx, compiled.CountStatement, compiled.CountArgs...)
	if err != nil {
		return Page{}, queryFailure(err)
	}
	total, err := readTotal(countResult)
	if err != nil {
		return Page{}, err
	}
	if total > request.Limit {
		return Page{}, reject(CodeLimitExceeded, "Summary 精确成员命中超过行预算")
	}

	snapshotResult, err := connection.Query(ctx, compiled.SnapshotStatement, compiled.SnapshotArgs...)
	if err != nil {
		return Page{}, queryFailure(err)
	}
	snapshotTotal, pageResult, err := readSnapshotRows(request.Profile, snapshotResult)
	if err != nil {
		return Page{}, err
	}
	if snapshotTotal > request.Limit {
		return Page{}, reject(CodeLimitExceeded, "Summary 精确成员在读取时超过行预算")
	}
	if snapshotTotal != total {
		return Page{}, reject(CodePartialFailure, "Summary 精确成员预检与读取快照 total 不一致")
	}
	rows, err := decodeTraceRows(request.Profile, pageResult)
	if err != nil {
		return Page{}, err
	}
	if int64(len(rows)) != snapshotTotal {
		return Page{}, reject(CodePartialFailure, "Summary 精确成员行数与真实 total 不一致")
	}
	if err := verifyMemberRows(rows, request, scope); err != nil {
		return Page{}, err
	}
	items, err := ShapeRows(request.Profile, rows)
	if err != nil {
		return Page{}, reject(CodePartialFailure, "Trace 结果不符合行合同: %s", err)
	}
	return Page{Items: items, Total: snapshotTotal}, nil
}

func executeSnapshotList(ctx context.Context, request Request, connection Querier, compiled CompiledList) (Page, error) {
	result, err := connection.Query(ctx, compiled.SnapshotStatement, compiled.SnapshotArgs...)
	if err != nil {
		return Page{}, queryFailure(err)
	}
	total, pageResult, err := readSnapshotRows(request.Profile, result)
	if err != nil {
		return Page{}, err
	}
	rows, err := decodeTraceRows(request.Profile, pageResult)
	if err != nil {
		return Page{}, err
	}
	offset := int64(0)
	if request.Offset != nil {
		offset = *request.Offset
	}
	expected := int64(0)
	if offset < total {
		expected = total - offset
		if expected > request.Limit {
			expected = request.Limit
		}
	}
	if int64(len(rows)) != expected {
		return Page{}, reject(CodePartialFailure, "Trace 同次快照 total/Page 行数不一致")
	}
	items, err := ShapeRows(request.Profile, rows)
	if err != nil {
		return Page{}, reject(CodePartialFailure, "Trace 结果不符合行合同: %s", err)
	}
	return Page{Items: items, Total: total}, nil
}

func readSnapshotRows(profile metadata.TraceProfile, result dorisclient.Result) (int64, dorisclient.Result, error) {
	if len(result.Rows) == 0 {
		return 0, dorisclient.Result{}, reject(CodePartialFailure, "Trace 同次快照缺少计数行")
	}
	var total int64
	page := dorisclient.Result{Rows: make([]map[string]any, 0, len(result.Rows))}
	for index, row := range result.Rows {
		value, present := row["total"]
		parsed, err := exactTraceInt64(value)
		if !present || err != nil || parsed < 0 || index > 0 && parsed != total {
			return 0, dorisclient.Result{}, reject(CodePartialFailure, "Trace 同次快照 total 缺失或不一致")
		}
		if index == 0 {
			total = parsed
		}
		tenant, present := row["bk_tenant_id"]
		if !present {
			return 0, dorisclient.Result{}, reject(CodePartialFailure, "Trace 同次快照缺少租户列")
		}
		if tenant == nil {
			if len(result.Rows) != 1 {
				return 0, dorisclient.Result{}, reject(CodePartialFailure, "Trace 空页哨兵与业务行混杂")
			}
			for _, column := range projectionForProfile(profile) {
				if value, exists := row[column]; !exists || value != nil {
					return 0, dorisclient.Result{}, reject(CodePartialFailure, "Trace 空页哨兵字段不完整")
				}
			}
			return total, page, nil
		}
		plain := make(map[string]any, len(row)-1)
		for column, value := range row {
			if column != "total" {
				plain[column] = value
			}
		}
		page.Rows = append(page.Rows, plain)
	}
	return total, page, nil
}

func verifyMemberRows(rows []map[string]any, request Request, scope Scope) error {
	allowed := make(map[string]int64, len(request.MemberLookup.AllowedPairs))
	for _, pair := range request.MemberLookup.AllowedPairs {
		allowed[strconv.FormatInt(pair.AppID, 10)] = pair.BkBizID
	}
	for _, row := range rows {
		appID, ok := row["app_id"].(string)
		bizID, err := exactTraceInt64(row["bk_biz_id"])
		expectedBiz, allowedApp := allowed[appID]
		if !ok || !allowedApp || err != nil || expectedBiz != bizID || bizID != scope.BkBizID ||
			row["bk_tenant_id"] != scope.BkTenantID ||
			row["observation_domain_id"] != scope.ObservationDomainID ||
			row["trace_id"] != request.MemberLookup.TraceID {
			return reject(CodeUnauthorizedMember, "Summary 精确成员行超出受信允许集")
		}
	}
	return nil
}

func queryFailure(err error) error {
	return reject(CodePartialFailure, "Trace 存储查询失败或超时: %s", err)
}

func readTotal(result dorisclient.Result) (int64, error) {
	if len(result.Rows) != 1 {
		return 0, reject(CodePartialFailure, "Trace 计数结果行数非法: %d", len(result.Rows))
	}
	value, present := result.Rows[0]["total"]
	if !present {
		return 0, reject(CodePartialFailure, "Trace 计数结果缺少 total")
	}
	total, err := exactTraceInt64(value)
	if err != nil || total < 0 {
		return 0, reject(CodePartialFailure, "Trace 计数结果非法")
	}
	return total, nil
}

// decodeTraceRows 只做 Doris → 行合同的类型桥接：JSON/VARIANT 文本转结构、
// BOOLEAN 转 bool，其余原样交给 ShapeRows 终审。
func decodeTraceRows(profile metadata.TraceProfile, result dorisclient.Result) ([]map[string]any, error) {
	rows := make([]map[string]any, 0, len(result.Rows))
	for index, row := range result.Rows {
		decoded := make(map[string]any, len(row))
		for field, value := range row {
			normalized, err := decodeTraceValue(profile, field, value)
			if err != nil {
				return nil, reject(CodePartialFailure, "Trace 第 %d 行字段 %s 解码失败: %s", index, field, err)
			}
			decoded[field] = normalized
		}
		rows = append(rows, decoded)
	}
	return rows, nil
}

func decodeTraceValue(profile metadata.TraceProfile, field string, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if field == "partial" {
		return decodeTraceBool(value)
	}
	if _, isJSON := jsonProjectionColumns[profile][field]; isJSON {
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("JSON/VARIANT 列不是文本")
		}
		if strings.TrimSpace(text) == "" {
			return nil, nil
		}
		return decodeTraceJSONText(text)
	}
	return value, nil
}

func decodeTraceBool(value any) (bool, error) {
	switch typed := value.(type) {
	case bool:
		return typed, nil
	case int64:
		return typed != 0, nil
	case string:
		return exactTraceBool(typed)
	default:
		return false, fmt.Errorf("不是布尔值")
	}
}

// decodeTraceJSONText 用 UseNumber 保留大整数精度；非 JSON 文本按标量字符串放行，
// 由行合同决定该列是否允许标量（如 root_service_status_code 允许、resource 不允许）。
func decodeTraceJSONText(text string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return text, nil
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("JSON 列含多余内容")
	}
	return decoded, nil
}
