// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package traceprofile

import (
	"math"
	"strconv"
	"strings"

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
		request.EndUS-request.StartUS > target.RetentionDays*86_400_000_000+1000 ||
		!validESSummaryListOrder(request.OrderBy) {
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
	if (predicate.ParentState == "observed" || predicate.ParentState == "missing") &&
		(target.EntryIndexVersion != 2 || target.RetentionDays != 30) {
		return Page{}, reject(CodeUnsupportedSchemaCapability, "ES Summary 跨应用父关系索引尚未发布")
	}
	allowedOwnerKeys, err := ESParentOwnerKeys(*list, predicate.ParentState)
	if err != nil {
		return Page{}, reject(CodeUnauthorizedMember, "ES Summary 跨应用父节点授权集合非法")
	}
	page := Page{Items: make([]map[string]any, 0, len(raw)), Total: total}
	var previousSort int64
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
		if err != nil {
			return Page{}, reject(CodePartialFailure, "ES Summary 列表开始时间非法")
		}
		sortValue := start
		if request.OrderBy[0] == "-trace_duration" || request.OrderBy[0] == "trace_duration" {
			sortValue, err = exactTraceInt64(row["trace_duration"])
		}
		descending := request.OrderBy[0][0] == '-'
		if err != nil || index > 0 && (descending && sortValue > previousSort ||
			!descending && sortValue < previousSort || sortValue == previousSort && traceID <= previousTrace) {
			return Page{}, reject(CodePartialFailure, "ES Summary 列表排序不稳定")
		}
		previousSort, previousTrace = sortValue, traceID
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
		var ownerKeys []string
		if target.EntryIndexVersion == 2 {
			version, versionErr := exactTraceInt64(row["entry_index_version"])
			if versionErr != nil || version != 2 {
				return Page{}, reject(CodePartialFailure, "ES Summary 入口证据版本缺失或非法")
			}
			hasParent, hasParentOK := row["entry_has_parent"].(bool)
			if !hasParentOK || hasParent != (entryParent != "") {
				return Page{}, reject(CodePartialFailure, "ES Summary 父节点存在标记与父 ID 不一致")
			}
			ownerKeys, err = parseESParentOwnerKeys(row["entry_parent_owner_keys"])
			if err != nil {
				return Page{}, reject(CodePartialFailure, "ES Summary 父节点归属字段缺失或非法")
			}
			ownerCount, countErr := exactTraceInt64(row["entry_parent_owner_count"])
			if countErr != nil || ownerCount != int64(len(ownerKeys)) {
				return Page{}, reject(CodePartialFailure, "ES Summary 父节点归属数量与证据不一致")
			}
		}
		if !predicate.MatchesWithParent(traceID, entryParent, entryType, entrySubtype, ownerKeys, allowedOwnerKeys) ||
			!predicate.MatchesIndexedFields(row) {
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

func parseESParentOwnerKeys(value any) ([]string, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, reject(CodePartialFailure, "ES Summary 父节点归属字段类型非法")
	}
	keys := make([]string, 0, len(items))
	previous := ""
	for _, item := range items {
		key, ok := item.(string)
		parts := strings.Split(key, ":")
		if !ok || len(parts) != 2 || key <= previous {
			return nil, reject(CodePartialFailure, "ES Summary 父节点归属值非法")
		}
		for _, part := range parts {
			number, err := strconv.ParseInt(part, 10, 64)
			if err != nil || number <= 0 || strconv.FormatInt(number, 10) != part {
				return nil, reject(CodePartialFailure, "ES Summary 父节点归属值非法")
			}
		}
		keys = append(keys, key)
		previous = key
	}
	return keys, nil
}
