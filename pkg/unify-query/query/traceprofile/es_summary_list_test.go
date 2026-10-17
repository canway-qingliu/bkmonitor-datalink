// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package traceprofile

import "testing"

func esListFixture(t *testing.T) (Request, Scope, ESSummaryTarget, map[string]any) {
	request, scope, target, row := esMemberFixture(t)
	request.MemberLookup = nil
	request.ESSummaryList = &ESSummaryList{
		Backend: MemberBackendElasticsearch, AppID: 7, BkBizID: 42, ExpectedRouteVersion: 7,
	}
	request.OrderBy = []string{"-min_start_time", "trace_id"}
	request.Filters = Filters{FieldList: []FilterField{{FieldName: "entry_type", Value: []string{"request"}, Operator: "eq"}}}
	target.RetentionDays = 7
	row["entry_span_id"] = "1112131415161718"
	row["entry_parent_span_id"] = ""
	row["entry_type"] = "request"
	row["entry_subtype"] = ""
	return request, scope, target, row
}

func TestShapeESSummaryListPagePreservesEntryFieldsAndExactTotal(t *testing.T) {
	request, scope, target, row := esListFixture(t)
	page, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target)
	if err != nil || page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("complete ES Summary list was rejected: page=%+v err=%v", page, err)
	}
	values := page.Items[0]["values"].(map[string]any)
	if values["entry_span_id"] != "1112131415161718" || values["entry_parent_span_id"] != "" ||
		values["entry_type"] != "request" || values["entry_subtype"] != "" {
		t.Fatalf("entry identity was lost: %+v", values)
	}
}

func TestShapeESSummaryListPageRejectsIncompleteOrForeignRows(t *testing.T) {
	request, scope, target, original := esListFixture(t)
	for name, mutate := range map[string]func(map[string]any){
		"wrong app":                       func(row map[string]any) { row["app_id"] = int64(8) },
		"missing parent":                  func(row map[string]any) { delete(row, "entry_parent_span_id") },
		"bad parent":                      func(row map[string]any) { row["entry_parent_span_id"] = "not-hex" },
		"request with background subtype": func(row map[string]any) { row["entry_subtype"] = "scheduled" },
		"wrong filter":                    func(row map[string]any) { row["entry_type"] = "background" },
	} {
		t.Run(name, func(t *testing.T) {
			row := make(map[string]any, len(original))
			for key, value := range original {
				row[key] = value
			}
			mutate(row)
			if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target); err == nil {
				t.Fatal("incomplete or foreign ES Summary row was accepted")
			}
		})
	}
	if _, err := ShapeESSummaryListPage([]map[string]any{original}, 2, request, scope, target); err == nil {
		t.Fatal("truncated ES Summary page was accepted")
	}
}

func TestShapeESSummaryListPageAppliesSubtypeOnlyToBackground(t *testing.T) {
	request, scope, target, original := esListFixture(t)
	request.Filters = Filters{
		FieldList: []FilterField{
			{FieldName: "entry_type", Value: []string{"request", "background"}, Operator: "eq"},
			{FieldName: "entry_subtype", Value: []string{"scheduled", "messaging"}, Operator: "eq"},
		},
		ConditionList: []string{"and"},
	}
	if _, err := ShapeESSummaryListPage([]map[string]any{original}, 1, request, scope, target); err != nil {
		t.Fatalf("request must remain visible when background subtype is selected: %v", err)
	}
	background := make(map[string]any, len(original))
	for key, value := range original {
		background[key] = value
	}
	background["entry_type"] = "background"
	background["entry_subtype"] = "scheduled"
	if _, err := ShapeESSummaryListPage([]map[string]any{background}, 1, request, scope, target); err != nil {
		t.Fatalf("selected scheduled background was rejected: %v", err)
	}
	background["entry_subtype"] = "async"
	if _, err := ShapeESSummaryListPage([]map[string]any{background}, 1, request, scope, target); err == nil {
		t.Fatal("unselected background subtype was accepted")
	}
}

func TestShapeESSummaryListPageUsesPublishedRetentionInsteadOfFixedSevenDays(t *testing.T) {
	request, scope, target, row := esListFixture(t)
	request.EndUS = request.StartUS + 10*86_400_000_000
	target.RetentionDays = 14
	if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target); err != nil {
		t.Fatalf("published fourteen-day target rejected ten-day request: %v", err)
	}
	target.RetentionDays = 7
	if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target); err == nil {
		t.Fatal("seven-day published target accepted ten-day request")
	}
}
