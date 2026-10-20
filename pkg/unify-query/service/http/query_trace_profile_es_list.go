// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package http

import (
	"context"
	"errors"
	"strconv"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
)

var esListColumns = append(append(structured.KeepColumns(nil), esMemberColumns...),
	"entry_span_id", "entry_parent_span_id", "entry_type", "entry_subtype", "entry_index_version",
	"entry_parent_owner_keys", "entry_parent_owner_count", "entry_has_parent",
	"entry_service_name", "entry_span_name", "entry_kind", "error")

// executeESList 仅查询发布的单应用 Summary RT，并复核 raw ES 的完整路由与 total。
func executeESList(
	ctx context.Context, request traceprofile.Request, scope traceprofile.Scope,
	target traceprofile.ESSummaryTarget, reader esMemberRawReader, limits traceprofile.ESSummaryLimits,
) (traceprofile.Page, error) {
	list := request.ESSummaryList
	if reader == nil || list == nil || request.Profile != metadata.TraceProfileSummary || request.Scene != "global" ||
		request.MemberLookup != nil || request.Aggregation != nil ||
		list.Backend != traceprofile.MemberBackendElasticsearch || list.AppID != target.AppID ||
		list.BkBizID != target.BkBizID || target.BkBizID != scope.BkBizID ||
		target.BkTenantID != scope.BkTenantID || target.ObservationDomainID != scope.ObservationDomainID ||
		target.ResultTableID == "" || target.StorageClusterID <= 0 {
		return traceprofile.Page{}, esMemberReject(traceprofile.CodeUnauthorizedMember, "ES Summary 列表身份与发布目标不一致")
	}
	if target.PublishedVersion <= 0 || target.PublishedVersion != list.ExpectedRouteVersion {
		return traceprofile.Page{}, esMemberReject(traceprofile.CodeScopePartial, "ES Summary 目标发布版本未生效")
	}
	predicate, err := request.ListPredicate()
	if err != nil {
		return traceprofile.Page{}, err
	}
	if err := traceprofile.ValidateESSummaryListRequest(request, predicate, target, limits); err != nil {
		return traceprofile.Page{}, err
	}
	offset := int64(0)
	if request.Offset != nil {
		offset = *request.Offset
	}
	baseFields := []structured.ConditionField{
		{DimensionName: "bk_tenant_id", Value: []string{scope.BkTenantID}, Operator: "eq"},
		{DimensionName: "observation_domain_id", Value: []string{scope.ObservationDomainID}, Operator: "eq"},
		{DimensionName: "biz_id", Value: []string{strconv.FormatInt(scope.BkBizID, 10)}, Operator: "eq"},
		{DimensionName: "app_id", Value: []string{strconv.FormatInt(target.AppID, 10)}, Operator: "eq"},
		{DimensionName: "min_start_time", Value: []string{strconv.FormatInt(request.StartUS, 10)}, Operator: "gte"},
		{DimensionName: "min_start_time", Value: []string{strconv.FormatInt(request.EndUS-1, 10)}, Operator: "lte"},
	}
	if target.EntryIndexVersion == 2 {
		baseFields = append(baseFields, structured.ConditionField{
			DimensionName: "entry_index_version", Value: []string{"2"}, Operator: structured.ConditionEqual,
		}, structured.ConditionField{
			DimensionName: "entry_parent_owner_count", Value: []string{"0"}, Operator: structured.ConditionGte,
		})
	}
	parentOwnerKeys, err := traceprofile.ESParentOwnerKeys(*list, predicate.ParentState)
	if err != nil {
		return traceprofile.Page{}, esMemberReject(traceprofile.CodeUnsupportedSchemaCapability,
			"ES Summary 跨应用父节点授权集合非法")
	}
	if predicate.TraceID != nil {
		baseFields = append(baseFields, structured.ConditionField{
			DimensionName: "trace_id", Value: []string{*predicate.TraceID}, Operator: "eq",
		})
	}
	if predicate.ParentID != nil {
		baseFields = append(baseFields, structured.ConditionField{
			DimensionName: "entry_parent_span_id", Value: []string{*predicate.ParentID}, Operator: "eq",
		})
	}
	if len(predicate.Services) > 0 {
		baseFields = append(baseFields, structured.ConditionField{
			DimensionName: "entry_service_name", Value: predicate.Services, Operator: structured.ConditionEqual,
		})
	}
	if len(predicate.Names) > 0 {
		baseFields = append(baseFields, structured.ConditionField{
			DimensionName: "entry_span_name", Value: predicate.Names, Operator: structured.ConditionEqual,
		})
	}
	if len(predicate.Kinds) > 0 {
		baseFields = append(baseFields, structured.ConditionField{
			DimensionName: "entry_kind", Value: predicate.Kinds, Operator: structured.ConditionEqual,
		})
	}
	if predicate.DurationGte != nil {
		baseFields = append(baseFields, structured.ConditionField{
			DimensionName: "trace_duration", Value: []string{strconv.FormatInt(*predicate.DurationGte, 10)},
			Operator: structured.ConditionGte,
		})
	}
	if predicate.DurationLte != nil {
		baseFields = append(baseFields, structured.ConditionField{
			DimensionName: "trace_duration", Value: []string{strconv.FormatInt(*predicate.DurationLte, 10)},
			Operator: structured.ConditionLte,
		})
	}
	if predicate.Error != nil {
		baseFields = append(baseFields, structured.ConditionField{
			DimensionName: "error", Value: []string{strconv.FormatBool(*predicate.Error)}, Operator: structured.ConditionEqual,
		})
	}
	switch predicate.ParentState {
	case "root":
		if target.EntryIndexVersion == 2 {
			baseFields = append(baseFields, structured.ConditionField{
				DimensionName: "entry_has_parent", Value: []string{"false"}, Operator: structured.ConditionEqual,
			})
		} else {
			baseFields = append(baseFields, structured.ConditionField{
				DimensionName: "entry_parent_span_id", Value: []string{""}, Operator: structured.ConditionEqual,
			})
		}
	case "observed", "missing":
		operator := structured.ConditionEqual
		if predicate.ParentState == "missing" {
			operator = structured.ConditionNotEqual
		}
		baseFields = append(baseFields,
			structured.ConditionField{DimensionName: "entry_has_parent", Value: []string{"true"},
				Operator: structured.ConditionEqual},
			structured.ConditionField{DimensionName: "entry_parent_owner_keys", Value: parentOwnerKeys,
				Operator: operator})
	}
	groups := make([][]structured.ConditionField, 0, 2)
	if len(predicate.Subtypes) == 0 {
		group := append([]structured.ConditionField(nil), baseFields...)
		if len(predicate.Types) > 0 {
			group = append(group, structured.ConditionField{
				DimensionName: "entry_type", Value: predicate.Types, Operator: "eq",
			})
		}
		groups = append(groups, group)
	} else {
		otherTypes := make([]string, 0, len(predicate.Types)-1)
		for _, entryType := range predicate.Types {
			if entryType != "background" {
				otherTypes = append(otherTypes, entryType)
			}
		}
		if len(otherTypes) > 0 {
			groups = append(groups, append(append([]structured.ConditionField(nil), baseFields...),
				structured.ConditionField{DimensionName: "entry_type", Value: otherTypes, Operator: "eq"}))
		}
		groups = append(groups, append(append([]structured.ConditionField(nil), baseFields...),
			structured.ConditionField{DimensionName: "entry_type", Value: []string{"background"}, Operator: "eq"},
			structured.ConditionField{DimensionName: "entry_subtype", Value: predicate.Subtypes, Operator: "eq"}))
	}
	fields := make([]structured.ConditionField, 0, len(baseFields)*len(groups)+3)
	conditions := make([]string, 0, cap(fields)-1)
	for groupIndex, group := range groups {
		for fieldIndex, field := range group {
			if len(fields) > 0 {
				connector := structured.ConditionAnd
				if groupIndex > 0 && fieldIndex == 0 {
					connector = structured.ConditionOr
				}
				conditions = append(conditions, connector)
			}
			fields = append(fields, field)
		}
	}
	order := structured.OrderBy(request.OrderBy)
	query := &structured.QueryTs{
		SpaceUid: scope.SpaceUID,
		Start:    strconv.FormatInt(request.StartUS/1_000_000, 10),
		End:      strconv.FormatInt((request.EndUS-1)/1_000_000+1, 10),
		Step:     "60s", Limit: int(request.Limit), From: int(offset), OrderBy: order,
		QueryList: []*structured.Query{{
			DataSource: "bkapm", TableID: structured.TableID(target.ResultTableID),
			ReferenceName: "a", QueryString: "*", ExactTotal: true,
			Limit: int(request.Limit), From: int(offset), OrderBy: order,
			Conditions:  structured.Conditions{FieldList: fields, ConditionList: conditions},
			KeepColumns: append(structured.KeepColumns(nil), esListColumns...),
		}},
	}
	total, rows, _, routes, err := reader(ctx, query)
	if err != nil {
		var rejected *traceprofile.RejectError
		if errors.As(err, &rejected) {
			return traceprofile.Page{}, err
		}
		return traceprofile.Page{}, esMemberReject(traceprofile.CodePartialFailure, "ES Summary 列表原始查询失败")
	}
	if status := metadata.GetStatus(ctx); status != nil && status.Code != "" {
		return traceprofile.Page{}, esMemberReject(traceprofile.CodePartialFailure, "ES Summary 列表原始查询存在不完整路由")
	}
	if !exactTotalAttested(query, routes) {
		return traceprofile.Page{}, esMemberReject(traceprofile.CodePartialFailure, "ES Summary 列表缺少精确 total 回执")
	}
	for _, route := range routes {
		if route.TableID != target.ResultTableID || route.StorageType != metadata.ElasticsearchStorageType ||
			route.StorageID != strconv.FormatInt(target.StorageClusterID, 10) {
			return traceprofile.Page{}, esMemberReject(traceprofile.CodeRouteNotFound, "ES Summary 列表 RT 或集群漂移")
		}
	}
	return traceprofile.ShapeESSummaryListPage(rows, total, request, scope, target, limits)
}
