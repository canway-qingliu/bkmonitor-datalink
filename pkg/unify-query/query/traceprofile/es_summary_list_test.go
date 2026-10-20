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
	page, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, DefaultESSummaryLimits())
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
			if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, DefaultESSummaryLimits()); err == nil {
				t.Fatal("incomplete or foreign ES Summary row was accepted")
			}
		})
	}
	if _, err := ShapeESSummaryListPage([]map[string]any{original}, 2, request, scope, target, DefaultESSummaryLimits()); err == nil {
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
	if _, err := ShapeESSummaryListPage([]map[string]any{original}, 1, request, scope, target, DefaultESSummaryLimits()); err != nil {
		t.Fatalf("request must remain visible when background subtype is selected: %v", err)
	}
	background := make(map[string]any, len(original))
	for key, value := range original {
		background[key] = value
	}
	background["entry_type"] = "background"
	background["entry_subtype"] = "scheduled"
	if _, err := ShapeESSummaryListPage([]map[string]any{background}, 1, request, scope, target, DefaultESSummaryLimits()); err != nil {
		t.Fatalf("selected scheduled background was rejected: %v", err)
	}
	background["entry_subtype"] = "async"
	if _, err := ShapeESSummaryListPage([]map[string]any{background}, 1, request, scope, target, DefaultESSummaryLimits()); err == nil {
		t.Fatal("unselected background subtype was accepted")
	}
}

func TestShapeESSummaryListPageUsesPublishedRetentionInsteadOfFixedSevenDays(t *testing.T) {
	request, scope, target, row := esListFixture(t)
	request.EndUS = request.StartUS + 10*86_400_000_000
	target.RetentionDays = 14
	if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, DefaultESSummaryLimits()); err != nil {
		t.Fatalf("published fourteen-day target rejected ten-day request: %v", err)
	}
	target.RetentionDays = 7
	if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, DefaultESSummaryLimits()); err == nil {
		t.Fatal("seven-day published target accepted ten-day request")
	}
	target.RetentionDays = 30
	request.EndUS = request.StartUS + 30*86_400_000_000 + 1000
	if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, DefaultESSummaryLimits()); err != nil {
		t.Fatalf("inclusive thirty-day UI window was rejected: %v", err)
	}
	request.EndUS++
	if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, DefaultESSummaryLimits()); err == nil {
		t.Fatal("window beyond inclusive thirty-day boundary was accepted")
	}
}

func TestShapeESSummaryListPageRechecksIndexedEntryNameDurationAndError(t *testing.T) {
	request, scope, target, original := esListFixture(t)
	request.Filters = Filters{FieldList: []FilterField{
		{FieldName: "entry_service_name", Value: []string{"checkout"}, Operator: "eq"},
		{FieldName: "entry_span_name", Value: []string{"GET /pay"}, Operator: "eq"},
		{FieldName: "entry_kind", Value: []string{"2"}, Operator: "eq"},
		{FieldName: "trace_duration_gte", Value: []string{"100"}, Operator: "gte"},
		{FieldName: "trace_duration_lte", Value: []string{"1000"}, Operator: "lte"},
		{FieldName: "error", Value: []string{"false"}, Operator: "eq"},
	}, ConditionList: []string{"and", "and", "and", "and", "and"}}
	original["entry_service_name"] = "checkout"
	original["entry_span_name"] = "GET /pay"
	original["entry_kind"] = int64(2)
	original["trace_duration"] = int64(500)
	original["error"] = false
	if _, err := ShapeESSummaryListPage([]map[string]any{original}, 1, request, scope, target, DefaultESSummaryLimits()); err != nil {
		t.Fatalf("supported entry attributes rejected: %v", err)
	}
	for _, mutation := range []func(map[string]any){
		func(row map[string]any) { row["entry_service_name"] = "other" },
		func(row map[string]any) { row["entry_kind"] = int64(3) },
		func(row map[string]any) { row["trace_duration"] = int64(1001) },
		func(row map[string]any) { row["error"] = true },
	} {
		row := make(map[string]any, len(original))
		for key, value := range original {
			row[key] = value
		}
		mutation(row)
		if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, DefaultESSummaryLimits()); err == nil {
			t.Fatal("row not satisfying indexed entry filters was accepted")
		}
	}
}

func TestShapeESSummaryListPageAcceptsDurationOrderAndRejectsWrongPageOrder(t *testing.T) {
	request, scope, target, first := esListFixture(t)
	request.OrderBy = []string{"-trace_duration", "trace_id"}
	first["trace_duration"] = int64(1000)
	second := make(map[string]any, len(first))
	for key, value := range first {
		second[key] = value
	}
	second["trace_id"] = "ffffffffffffffffffffffffffffffff"
	second["trace_duration"] = int64(500)
	if _, err := ShapeESSummaryListPage([]map[string]any{first, second}, 2, request, scope, target, DefaultESSummaryLimits()); err != nil {
		t.Fatalf("duration order rejected: %v", err)
	}
	if _, err := ShapeESSummaryListPage([]map[string]any{second, first}, 2, request, scope, target, DefaultESSummaryLimits()); err == nil {
		t.Fatal("unstable duration page order was accepted")
	}
}

func TestESSummaryListPredicateIntersectsObservedParentOwnersWithAuthorizedApps(t *testing.T) {
	filters := Filters{FieldList: []FilterField{{FieldName: "entry_parent_state", Value: []string{"observed"}, Operator: "eq"}}}
	predicate, err := ParseESSummaryListPredicate(filters)
	if err != nil {
		t.Fatalf("observed relation was rejected: %v", err)
	}
	parent := "1112131415161718"
	if !predicate.MatchesWithParent("0123456789abcdef0123456789abcdef", parent,
		"request", "", []string{"2:55", "3:77"}, []string{"3:77"}) {
		t.Fatal("parent observed in an authorized app was lost")
	}
	if predicate.MatchesWithParent("0123456789abcdef0123456789abcdef", parent,
		"request", "", []string{"2:55", "3:77"}, []string{"9:99"}) {
		t.Fatal("unrelated authorized app incorrectly made parent observed")
	}
	filters.FieldList[0].Value = []string{"missing"}
	predicate, err = ParseESSummaryListPredicate(filters)
	if err != nil || !predicate.MatchesWithParent("0123456789abcdef0123456789abcdef", parent,
		"request", "", []string{"2:55"}, []string{"3:77"}) {
		t.Fatalf("parent in an unauthorized app must be missing in the current scope: %v", err)
	}
}

func TestShapeESSummaryListPageRechecksCrossAppParentOwnersAndRejectsOldRows(t *testing.T) {
	request, scope, target, original := esListFixture(t)
	request.Filters = Filters{FieldList: []FilterField{
		{FieldName: "entry_parent_state", Value: []string{"observed"}, Operator: "eq"},
	}}
	request.ESSummaryList.ParentAllowedPairs = []MemberPair{{AppID: 7, BkBizID: 42}}
	target.RetentionDays = 30
	target.EntryIndexVersion = 2
	original["entry_parent_span_id"] = "2122232425262728"
	original["entry_has_parent"] = true
	original["entry_index_version"] = float64(2)
	original["entry_parent_owner_keys"] = []any{"42:7", "99:99"}
	original["entry_parent_owner_count"] = int64(2)
	if _, err := ShapeESSummaryListPage([]map[string]any{original}, 1, request, scope, target, DefaultESSummaryLimits()); err != nil {
		t.Fatalf("authorized parent owner was not accepted: %v", err)
	}
	oldTarget := target
	oldTarget.EntryIndexVersion = 0
	if _, err := ShapeESSummaryListPage([]map[string]any{original}, 1, request, scope, oldTarget, DefaultESSummaryLimits()); err == nil {
		t.Fatal("unpublished entry index v2 accepted cross-app relation")
	}
	for name, mutate := range map[string]func(map[string]any){
		"unrelated owner":     func(row map[string]any) { row["entry_parent_owner_keys"] = []any{"99:99"} },
		"missing owner field": func(row map[string]any) { delete(row, "entry_parent_owner_keys") },
		"missing owner count": func(row map[string]any) { delete(row, "entry_parent_owner_count") },
		"wrong owner count":   func(row map[string]any) { row["entry_parent_owner_count"] = int64(1) },
		"old index version":   func(row map[string]any) { delete(row, "entry_index_version") },
		"missing parent flag": func(row map[string]any) { delete(row, "entry_has_parent") },
		"wrong parent flag":   func(row map[string]any) { row["entry_has_parent"] = false },
		"malformed owner":     func(row map[string]any) { row["entry_parent_owner_keys"] = []any{"unknown"} },
	} {
		t.Run(name, func(t *testing.T) {
			row := make(map[string]any, len(original))
			for key, value := range original {
				row[key] = value
			}
			mutate(row)
			if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, DefaultESSummaryLimits()); err == nil {
				t.Fatal("ES Summary relation row escaped authorization or schema recheck")
			}
		})
	}
}

func TestShapeESSummaryListPageV2RootRequiresExplicitNoParentFlag(t *testing.T) {
	request, scope, target, row := esListFixture(t)
	request.Filters = Filters{FieldList: []FilterField{{FieldName: "entry_parent_state", Value: []string{"root"}, Operator: "eq"}}}
	target.RetentionDays = 30
	target.EntryIndexVersion = 2
	row["entry_index_version"] = int64(2)
	row["entry_has_parent"] = false
	row["entry_parent_owner_keys"] = []any{}
	row["entry_parent_owner_count"] = int64(0)
	if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, DefaultESSummaryLimits()); err != nil {
		t.Fatalf("V2 root row rejected: %v", err)
	}
	row["entry_has_parent"] = true
	if _, err := ShapeESSummaryListPage([]map[string]any{row}, 1, request, scope, target, DefaultESSummaryLimits()); err == nil {
		t.Fatal("V2 root with parent flag was accepted")
	}
}
