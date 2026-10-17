// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package traceprofile

// ESSummaryListPredicate 是入口筛选的闭合语义：子类型仅限制后台任务。
// 由 UQ 编译查询并复核返回行，调用方不能自行提交 OR 布尔表达式。
type ESSummaryListPredicate struct {
	Types    []string
	Subtypes []string
	ParentID *string
	TraceID  *string
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
		if _, duplicate := seenFields[filter.FieldName]; duplicate || filter.Operator != "eq" ||
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
		case "trace_id":
			allowed = nil
		default:
			return result, reject(CodeUnsupportedSchemaCapability, "ES Summary 列表过滤字段非法")
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
		case "trace_id":
			value := filter.Value[0]
			if !traceIDPattern.MatchString(value) {
				return result, reject(CodeUnsupportedSchemaCapability, "ES Summary Trace ID 非法")
			}
			result.TraceID = &value
		}
	}
	if len(result.Subtypes) > 0 && !containsEntryChoice(result.Types, "background") {
		return result, reject(CodeUnsupportedSchemaCapability, "后台子类型需要明确选择后台任务")
	}
	return result, nil
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
