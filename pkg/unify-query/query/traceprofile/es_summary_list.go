// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package traceprofile

import (
	"math"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

// ShapeESSummaryListPage 拒绝截断页、越权行和不稳定排序；基础 Summary 字段沿用成员行合同。
func ShapeESSummaryListPage(raw []map[string]any, total int64, request Request, scope Scope, target ESSummaryTarget) (Page, error) {
	list := request.ESSummaryList
	if request.Profile != metadata.TraceProfileSummary || request.Scene != "global" || list == nil ||
		request.MemberLookup != nil || request.Aggregation != nil ||
		list.Backend != MemberBackendElasticsearch || list.AppID != target.AppID ||
		list.BkBizID != target.BkBizID || list.BkBizID != scope.BkBizID ||
		list.ExpectedRouteVersion <= 0 || list.ExpectedRouteVersion != target.PublishedVersion ||
		target.RetentionDays < 1 || target.RetentionDays > 30 || target.BkTenantID != scope.BkTenantID ||
		target.ObservationDomainID != scope.ObservationDomainID ||
		request.Limit <= 0 || request.Limit > 1000 || request.StartUS >= request.EndUS ||
		request.EndUS-request.StartUS > target.RetentionDays*86_400_000_000 ||
		len(request.OrderBy) != 2 || request.OrderBy[0] != "-min_start_time" || request.OrderBy[1] != "trace_id" {
		return Page{}, reject(CodeUnauthorizedMember, "ES Summary 列表作用域或已发布保留期不一致")
	}
	offset := int64(0)
	if request.Offset != nil {
		offset = *request.Offset
	}
	if total < 0 || offset < 0 || offset > math.MaxInt64-request.Limit {
		return Page{}, reject(CodeLimitExceeded, "ES Summary 列表分页预算非法")
	}
	expected := int64(0)
	if total > offset {
		expected = total - offset
		if expected > request.Limit {
			expected = request.Limit
		}
	}
	if int64(len(raw)) != expected {
		return Page{}, reject(CodePartialFailure, "ES Summary 列表页不完整")
	}
	predicate, err := ParseESSummaryListPredicate(request.Filters)
	if err != nil {
		return Page{}, err
	}
	page := Page{Items: make([]map[string]any, 0, len(raw)), Total: total}
	var previousStart int64
	var previousTrace string
	for index, row := range raw {
		if row == nil {
			return Page{}, reject(CodePartialFailure, "ES Summary 列表行为空")
		}
		traceID, ok := row["trace_id"].(string)
		if !ok || !traceIDPattern.MatchString(traceID) {
			return Page{}, reject(CodePartialFailure, "ES Summary 列表 Trace ID 非法")
		}
		start, err := exactTraceInt64(row["min_start_time"])
		if err != nil || index > 0 && (start > previousStart || start == previousStart && traceID <= previousTrace) {
			return Page{}, reject(CodePartialFailure, "ES Summary 列表排序不稳定")
		}
		previousStart, previousTrace = start, traceID
		entrySpan, spanOK := row["entry_span_id"].(string)
		entryParent, parentOK := row["entry_parent_span_id"].(string)
		entryType, typeOK := row["entry_type"].(string)
		entrySubtype, subtypeOK := row["entry_subtype"].(string)
		if !spanOK || !spanIDPattern.MatchString(entrySpan) || !parentOK ||
			(entryParent != "" && !spanIDPattern.MatchString(entryParent)) || !typeOK || !subtypeOK ||
			(entryType != "request" && entryType != "background" && entryType != "other") ||
			(entrySubtype != "" && entrySubtype != "scheduled" && entrySubtype != "async" && entrySubtype != "messaging") ||
			(entryType != "background" && entrySubtype != "") {
			return Page{}, reject(CodePartialFailure, "ES Summary 入口字段缺失或非法")
		}
		if !predicate.Matches(traceID, entryParent, entryType, entrySubtype) {
			return Page{}, reject(CodePartialFailure, "ES Summary 列表行不满足过滤条件")
		}
		memberRequest := request
		memberRequest.ESSummaryList = nil
		memberRequest.Filters = Filters{}
		memberRequest.OrderBy = nil
		memberRequest.Offset = nil
		memberRequest.Limit = 1
		memberRequest.MemberLookup = &MemberLookup{
			Backend: MemberBackendElasticsearch, TraceID: traceID,
			ExpectedRouteVersion: list.ExpectedRouteVersion,
			AllowedPairs:         []MemberPair{{AppID: target.AppID, BkBizID: target.BkBizID}},
		}
		memberPage, err := ShapeESSummaryMemberPage([]map[string]any{row}, 1, memberRequest, scope, target)
		if err != nil {
			return Page{}, err
		}
		item := memberPage.Items[0]
		values := item["values"].(map[string]any)
		values["entry_span_id"] = entrySpan
		values["entry_parent_span_id"] = entryParent
		values["entry_type"] = entryType
		values["entry_subtype"] = entrySubtype
		page.Items = append(page.Items, item)
	}
	return page, nil
}
