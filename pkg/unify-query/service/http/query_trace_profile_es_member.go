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
	"errors"
	"strconv"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
)

type esMemberRawReader func(context.Context, *structured.QueryTs) (
	int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error,
)

var esMemberColumns = structured.KeepColumns{
	"bk_tenant_id", "observation_domain_id", "biz_id", "app_id", "app_name", "trace_id",
	"trace_date", "min_start_time", "max_end_time", "trace_duration", "span_count",
	"error_count", "service_count", "hierarchy_count", "span_min_duration", "span_max_duration",
	"event_time_ms", "root_service", "root_service_span_id", "root_service_span_name",
	"root_service_kind", "root_service_category", "root_service_status_code", "root_span_id",
	"root_span_name", "root_span_service", "root_span_kind", "category_statistics",
	"kind_statistics", "partial", "time",
}

func esMemberReject(code, message string) error {
	return &traceprofile.RejectError{Code: code, Message: message}
}

// queryESMemberRawWithTimeField 在任何 ES I/O 前读取实际 RT 路由的时间字段。
// 默认 dtEventTimeStamp 会漏查新 Summary；不能靠零行或后置行校验发现。
func queryESMemberRawWithTimeField(ctx context.Context, query *structured.QueryTs) (
	int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error,
) {
	resolved, err := query.ToQueryReference(ctx)
	if err != nil {
		return 0, nil, nil, nil, esMemberReject(traceprofile.CodePartialFailure, "ES Summary RT 解析失败")
	}
	if resolved.Count() == 0 {
		return 0, nil, nil, nil, esMemberReject(traceprofile.CodeRouteNotFound, "ES Summary RT 没有可用路由")
	}
	for _, metrics := range resolved {
		for _, metric := range metrics {
			if metric == nil || len(metric.QueryList) == 0 {
				return 0, nil, nil, nil, esMemberReject(traceprofile.CodeRouteNotFound, "ES Summary RT 路由不完整")
			}
			for _, route := range metric.QueryList {
				if route == nil || route.TimeField.Name != "min_start_time" ||
					route.TimeField.Type != "long" || route.TimeField.Unit != "microsecond" {
					return 0, nil, nil, nil, esMemberReject(
						traceprofile.CodeScopePartial, "ES Summary RT 时间字段不是微秒级 min_start_time",
					)
				}
			}
		}
	}
	return queryRawWithResolvedInstance(ctx, query, resolved)
}

// executeESMemberLookup 只在已发布目标上构造精确 raw ES 查询；实际路由和完整行再复核。
func executeESMemberLookup(
	ctx context.Context, request traceprofile.Request, scope traceprofile.Scope,
	target traceprofile.ESSummaryTarget, reader esMemberRawReader,
) (traceprofile.Page, error) {
	lookup := request.MemberLookup
	if reader == nil || lookup == nil || lookup.Backend != traceprofile.MemberBackendElasticsearch ||
		len(lookup.AllowedPairs) != 1 || lookup.AllowedPairs[0].BkBizID != scope.BkBizID ||
		lookup.AllowedPairs[0].AppID != target.AppID || target.BkBizID != scope.BkBizID ||
		target.BkTenantID != scope.BkTenantID || target.ObservationDomainID != scope.ObservationDomainID {
		return traceprofile.Page{}, esMemberReject(traceprofile.CodeUnauthorizedMember, "ES Summary 成员作用域与发布目标不一致")
	}
	if target.PublishedVersion <= 0 || target.PublishedVersion != lookup.ExpectedRouteVersion {
		return traceprofile.Page{}, esMemberReject(traceprofile.CodeScopePartial, "ES Summary 发布版本未生效")
	}
	if request.Limit < 1 || request.Limit > 1000 || request.EndUS <= request.StartUS {
		return traceprofile.Page{}, esMemberReject(traceprofile.CodeLimitExceeded, "ES Summary 成员时间或行预算非法")
	}
	fields := []structured.ConditionField{
		{DimensionName: "trace_id", Value: []string{lookup.TraceID}, Operator: "eq"},
		{DimensionName: "bk_tenant_id", Value: []string{scope.BkTenantID}, Operator: "eq"},
		{DimensionName: "observation_domain_id", Value: []string{scope.ObservationDomainID}, Operator: "eq"},
		{DimensionName: "biz_id", Value: []string{strconv.FormatInt(scope.BkBizID, 10)}, Operator: "eq"},
		{DimensionName: "app_id", Value: []string{strconv.FormatInt(target.AppID, 10)}, Operator: "eq"},
		{DimensionName: "min_start_time", Value: []string{strconv.FormatInt(request.StartUS, 10)}, Operator: "gte"},
		{DimensionName: "min_start_time", Value: []string{strconv.FormatInt(request.EndUS-1, 10)}, Operator: "lte"},
	}
	conditions := make([]string, len(fields)-1)
	for index := range conditions {
		conditions[index] = structured.ConditionAnd
	}
	query := &structured.QueryTs{
		SpaceUid: scope.SpaceUID,
		Start:    strconv.FormatInt(request.StartUS/1_000_000, 10),
		End:      strconv.FormatInt((request.EndUS-1)/1_000_000+1, 10),
		Step:     "60s", Limit: int(request.Limit), From: 0,
		QueryList: []*structured.Query{{
			DataSource: "bkapm", TableID: structured.TableID(target.ResultTableID),
			ReferenceName: "a", QueryString: "*", ExactTotal: true,
			Limit: int(request.Limit), From: 0,
			Conditions:  structured.Conditions{FieldList: fields, ConditionList: conditions},
			KeepColumns: append(structured.KeepColumns(nil), esMemberColumns...),
		}},
	}
	total, rows, _, routes, err := reader(ctx, query)
	if err != nil {
		var rejected *traceprofile.RejectError
		if errors.As(err, &rejected) {
			return traceprofile.Page{}, err
		}
		return traceprofile.Page{}, esMemberReject(traceprofile.CodePartialFailure, "ES Summary 原始查询失败")
	}
	if status := metadata.GetStatus(ctx); status != nil && status.Code != "" {
		return traceprofile.Page{}, esMemberReject(traceprofile.CodePartialFailure, "ES Summary 原始查询存在不完整路由")
	}
	if !exactTotalAttested(query, routes) {
		return traceprofile.Page{}, esMemberReject(traceprofile.CodePartialFailure, "ES Summary 缺少精确 total 路由回执")
	}
	for _, route := range routes {
		if route.TableID != target.ResultTableID || route.StorageType != metadata.ElasticsearchStorageType ||
			route.StorageID != strconv.FormatInt(target.StorageClusterID, 10) {
			return traceprofile.Page{}, esMemberReject(traceprofile.CodeRouteNotFound, "ES Summary 逻辑 RT 或存储集群漂移")
		}
	}
	return traceprofile.ShapeESSummaryMemberPage(rows, total, request, scope, target)
}
