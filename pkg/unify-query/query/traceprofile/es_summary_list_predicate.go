// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package traceprofile

import "strconv"

// ESSummaryListPredicate 是入口筛选的闭合语义：子类型仅限制后台任务。
// 由 UQ 编译查询并复核返回行，调用方不能自行提交 OR 布尔表达式。
type ESSummaryListPredicate struct {
	Types       []string
	Subtypes    []string
	Services    []string
	Names       []string
	Kinds       []string
	DurationGte *int64
	DurationLte *int64
	Error       *bool
	ParentID    *string
	ParentState string
	TraceID     *string
}

func ParseESSummaryListPredicate(filters Filters) (ESSummaryListPredicate, error) {
	result := ESSummaryListPredicate{}
	if len(filters.ConditionList) != max(len(filters.FieldList)-1, 0) {
		return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 列表条件连接符数量非法")
	}
	for _, connector := range filters.ConditionList {
		if connector != "and" {
			return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 列表只接受服务端生成的组合条件")
		}
	}
	seenFields := make(map[string]struct{}, len(filters.FieldList))
	for _, filter := range filters.FieldList {
		if _, duplicate := seenFields[filter.FieldName]; duplicate ||
			filter.IsWildcard || filter.IsPrefix || filter.IsSuffix || len(filter.Value) == 0 {
			return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 列表入口条件非法或重复")
		}
		seenFields[filter.FieldName] = struct{}{}
		allowed := map[string]bool{}
		maximum := 1
		switch filter.FieldName {
		case "entry_type":
			allowed = map[string]bool{"request": true, "background": true, "other": true}
			maximum = 3
		case "entry_subtype":
			allowed = map[string]bool{"scheduled": true, "async": true, "messaging": true}
			maximum = 3
		case "entry_parent_span_id":
			allowed = nil
		case "entry_parent_state":
			allowed = map[string]bool{"root": true, "observed": true, "missing": true}
		case "trace_id":
			allowed = nil
		case "entry_service_name", "entry_span_name":
			allowed = nil
			maximum = 64
		case "entry_kind":
			allowed = map[string]bool{"0": true, "1": true, "2": true, "3": true, "4": true, "5": true}
			maximum = 6
		case "trace_duration_gte", "trace_duration_lte", "error":
			allowed = nil
		default:
			return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 列表过滤字段非法")
		}
		wantOperator := "eq"
		if filter.FieldName == "trace_duration_gte" {
			wantOperator = "gte"
		}
		if filter.FieldName == "trace_duration_lte" {
			wantOperator = "lte"
		}
		if filter.Operator != wantOperator {
			return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 列表过滤操作符非法")
		}
		if len(filter.Value) > maximum {
			return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 列表入口选择超过枚举范围")
		}
		seenValues := make(map[string]struct{}, len(filter.Value))
		for _, value := range filter.Value {
			if _, duplicate := seenValues[value]; duplicate {
				return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 列表入口选择重复")
			}
			seenValues[value] = struct{}{}
			if allowed != nil && !allowed[value] {
				return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 列表入口类型或子类型非法")
			}
		}
		switch filter.FieldName {
		case "entry_type":
			result.Types = append([]string(nil), filter.Value...)
		case "entry_subtype":
			result.Subtypes = append([]string(nil), filter.Value...)
		case "entry_parent_span_id":
			value := filter.Value[0]
			if value != "" && !spanIDPattern.MatchString(value) {
				return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 父 Span ID 非法")
			}
			result.ParentID = &value
		case "entry_parent_state":
			result.ParentState = filter.Value[0]
		case "trace_id":
			value := filter.Value[0]
			if !traceIDPattern.MatchString(value) {
				return result, reject(CodeUnsupportedSchemaCapability, "ES Summary Trace ID 非法")
			}
			result.TraceID = &value
		case "entry_service_name":
			if !validEntryNames(filter.Value) {
				return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 入口服务非法")
			}
			result.Services = append([]string(nil), filter.Value...)
		case "entry_span_name":
			if !validEntryNames(filter.Value) {
				return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 入口方法非法")
			}
			result.Names = append([]string(nil), filter.Value...)
		case "entry_kind":
			result.Kinds = append([]string(nil), filter.Value...)
		case "trace_duration_gte", "trace_duration_lte":
			duration, parseErr := strconv.ParseInt(filter.Value[0], 10, 64)
			if parseErr != nil || duration < 0 || strconv.FormatInt(duration, 10) != filter.Value[0] {
				return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 耗时边界非法")
			}
			if filter.FieldName == "trace_duration_gte" {
				result.DurationGte = &duration
			} else {
				result.DurationLte = &duration
			}
		case "error":
			if filter.Value[0] != "true" && filter.Value[0] != "false" {
				return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 错误状态非法")
			}
			value := filter.Value[0] == "true"
			result.Error = &value
		}
	}
	if result.DurationGte != nil && result.DurationLte != nil && *result.DurationGte > *result.DurationLte {
		return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 耗时范围逆序")
	}
	if len(result.Subtypes) > 0 && !containsEntryChoice(result.Types, "background") {
		return result, reject(CodeUnsupportedSchemaCapability, "后台子类型需要明确选择后台任务")
	}
	return result, nil
}

func validEntryNames(values []string) bool {
	for _, value := range values {
		if value == "" || len(value) > 512 {
			return false
		}
	}
	return true
}

func (p ESSummaryListPredicate) MatchesIndexedFields(row map[string]any) bool {
	if len(p.Services) > 0 {
		value, ok := row["entry_service_name"].(string)
		if !ok || !containsEntryChoice(p.Services, value) {
			return false
		}
	}
	if len(p.Names) > 0 {
		value, ok := row["entry_span_name"].(string)
		if !ok || !containsEntryChoice(p.Names, value) {
			return false
		}
	}
	if len(p.Kinds) > 0 {
		value, err := exactTraceInt64(row["entry_kind"])
		if err != nil || !containsEntryChoice(p.Kinds, strconv.FormatInt(value, 10)) {
			return false
		}
	}
	if p.DurationGte != nil || p.DurationLte != nil {
		value, err := exactTraceInt64(row["trace_duration"])
		if err != nil || value < 0 || p.DurationGte != nil && value < *p.DurationGte ||
			p.DurationLte != nil && value > *p.DurationLte {
			return false
		}
	}
	if p.Error != nil {
		value, ok := row["error"].(bool)
		if !ok || value != *p.Error {
			return false
		}
	}
	return true
}

func (p ESSummaryListPredicate) MatchesWithParent(traceID, entryParent, entryType, entrySubtype string,
	parentOwnerKeys, allowedOwnerKeys []string) bool {
	if !p.Matches(traceID, entryParent, entryType, entrySubtype) {
		return false
	}
	switch p.ParentState {
	case "root":
		return entryParent == ""
	case "observed", "missing":
		if entryParent == "" {
			return false
		}
		allowed := make(map[string]struct{}, len(allowedOwnerKeys))
		for _, key := range allowedOwnerKeys {
			allowed[key] = struct{}{}
		}
		observed := false
		for _, key := range parentOwnerKeys {
			if _, ok := allowed[key]; ok {
				observed = true
				break
			}
		}
		return observed == (p.ParentState == "observed")
	default:
		return true
	}
}

func containsEntryChoice(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (p ESSummaryListPredicate) Matches(traceID, entryParent, entryType, entrySubtype string) bool {
	if p.TraceID != nil && traceID != *p.TraceID || p.ParentID != nil && entryParent != *p.ParentID {
		return false
	}
	if len(p.Types) > 0 && !containsEntryChoice(p.Types, entryType) {
		return false
	}
	return entryType != "background" || len(p.Subtypes) == 0 || containsEntryChoice(p.Subtypes, entrySubtype)
}
