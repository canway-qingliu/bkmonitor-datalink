// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT

package http

import (
	"context"
	"strconv"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
)

func esListHTTPFixture(t *testing.T) (traceprofile.Request, traceprofile.Scope, traceprofile.ESSummaryTarget) {
	request, scope, target := esMemberHTTPFixture(t)
	request.MemberLookup = nil
	request.ESSummaryList = &traceprofile.ESSummaryList{
		Backend: traceprofile.MemberBackendElasticsearch, AppID: 7, BkBizID: 42, ExpectedRouteVersion: 7,
	}
	request.OrderBy = []string{"-min_start_time", "trace_id"}
	request.Filters = traceprofile.Filters{FieldList: []traceprofile.FilterField{{FieldName: "entry_type", Value: []string{"request"}, Operator: "eq"}}}
	offset := int64(1)
	request.Offset = &offset
	target.RetentionDays = 7
	return request, scope, target
}

func TestESListExecutionBuildsClosedExactPageAndChecksRoute(t *testing.T) {
	request, scope, target := esListHTTPFixture(t)
	reader := func(_ context.Context, query *structured.QueryTs) (int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error) {
		if query.SpaceUid != scope.SpaceUID || query.From != 1 || query.Limit != 2 ||
			len(query.QueryList) != 1 || query.QueryList[0].From != 1 || !query.QueryList[0].ExactTotal ||
			string(query.QueryList[0].TableID) != target.ResultTableID ||
			len(query.QueryList[0].OrderBy) != 2 || query.QueryList[0].OrderBy[0] != "-min_start_time" ||
			query.QueryList[0].OrderBy[1] != "trace_id" {
			t.Fatalf("ES list query lost trusted pagination, route or order: %+v", query)
		}
		wanted := map[string]string{
			"biz_id": "42", "app_id": "7", "entry_type": "request",
		}
		for _, field := range query.QueryList[0].Conditions.FieldList {
			if value, ok := wanted[field.DimensionName]; ok {
				if field.Operator != "eq" || len(field.Value) != 1 || field.Value[0] != value {
					t.Fatalf("ES list identity/filter changed: %+v", field)
				}
				delete(wanted, field.DimensionName)
			}
		}
		if len(wanted) != 0 {
			t.Fatalf("ES list missed identity/filter: %+v", wanted)
		}
		return 1, nil, nil, []metadata.RouteInfo{{
			TableID: target.ResultTableID, StorageType: metadata.ElasticsearchStorageType,
			StorageID: strconv.FormatInt(target.StorageClusterID, 10),
		}}, nil
	}
	page, err := executeESList(context.Background(), request, scope, target, reader, traceprofile.NewESSummaryLimits(traceprofile.ESSummaryMaxRows, traceprofile.ESSummaryMaxOffset))
	if err != nil || page.Total != 1 || len(page.Items) != 0 {
		t.Fatalf("valid ES list empty offset page failed: page=%+v err=%v", page, err)
	}
}

func TestESListExecutionRejectsVersionDriftPartialAndWrongRoute(t *testing.T) {
	request, scope, target := esListHTTPFixture(t)
	goodRoute := metadata.RouteInfo{TableID: target.ResultTableID, StorageType: metadata.ElasticsearchStorageType, StorageID: "9"}
	for name, mutate := range map[string]func(*traceprofile.ESSummaryTarget, *metadata.RouteInfo){
		"version":   func(target *traceprofile.ESSummaryTarget, _ *metadata.RouteInfo) { target.PublishedVersion++ },
		"retention": func(target *traceprofile.ESSummaryTarget, _ *metadata.RouteInfo) { target.RetentionDays = 31 },
		"cluster":   func(_ *traceprofile.ESSummaryTarget, route *metadata.RouteInfo) { route.StorageID = "8" },
		"table":     func(_ *traceprofile.ESSummaryTarget, route *metadata.RouteInfo) { route.TableID = "other.summary" },
	} {
		t.Run(name, func(t *testing.T) {
			changedTarget, route := target, goodRoute
			mutate(&changedTarget, &route)
			reader := func(_ context.Context, _ *structured.QueryTs) (int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error) {
				return 1, nil, nil, []metadata.RouteInfo{route}, nil
			}
			if _, err := executeESList(context.Background(), request, scope, changedTarget, reader, traceprofile.NewESSummaryLimits(traceprofile.ESSummaryMaxRows, traceprofile.ESSummaryMaxOffset)); err == nil {
				t.Fatal("drifted ES list target or route was accepted")
			}
		})
	}
	ctx := metadata.InitHashID(context.Background())
	reader := func(ctx context.Context, _ *structured.QueryTs) (int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error) {
		metadata.SetStatus(ctx, metadata.QueryRawPartial, "shard failed")
		return 1, nil, nil, []metadata.RouteInfo{goodRoute}, nil
	}
	if _, err := executeESList(ctx, request, scope, target, reader, traceprofile.NewESSummaryLimits(traceprofile.ESSummaryMaxRows, traceprofile.ESSummaryMaxOffset)); traceprofile.ErrorCode(err) != traceprofile.CodePartialFailure {
		t.Fatalf("partial ES list escaped as complete: %v", err)
	}
}

func TestESListExecutionRepeatsTrustedScopeAcrossMixedTypeBranches(t *testing.T) {
	request, scope, target := esListHTTPFixture(t)
	request.Filters = traceprofile.Filters{
		FieldList: []traceprofile.FilterField{
			{FieldName: "entry_type", Value: []string{"request", "background"}, Operator: "eq"},
			{FieldName: "entry_subtype", Value: []string{"scheduled"}, Operator: "eq"},
			{FieldName: "entry_parent_span_id", Value: []string{""}, Operator: "eq"},
		},
		ConditionList: []string{"and", "and"},
	}
	reader := func(_ context.Context, query *structured.QueryTs) (int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error) {
		groups, err := query.QueryList[0].Conditions.AnalysisConditions()
		if err != nil || len(groups) != 2 {
			t.Fatalf("mixed entry predicate must compile to two trusted branches: groups=%+v err=%v", groups, err)
		}
		for _, group := range groups {
			identity := map[string]string{
				"biz_id": strconv.FormatInt(scope.BkBizID, 10),
				"app_id": strconv.FormatInt(target.AppID, 10),
			}
			timeBounds := map[string]bool{"gte": false, "lte": false}
			wanted := map[string]bool{"biz_id": false, "app_id": false, "min_start_time": false,
				"entry_type": false, "entry_parent_span_id": false}
			for _, field := range group {
				if expected, ok := identity[field.DimensionName]; ok &&
					(field.Operator != "eq" || len(field.Value) != 1 || field.Value[0] != expected) {
					t.Fatalf("OR branch widened trusted identity: %+v", field)
				}
				if field.DimensionName == "min_start_time" {
					if len(field.Value) != 1 ||
						field.Operator == "gte" && field.Value[0] != strconv.FormatInt(request.StartUS, 10) ||
						field.Operator == "lte" && field.Value[0] != strconv.FormatInt(request.EndUS-1, 10) ||
						field.Operator != "gte" && field.Operator != "lte" {
						t.Fatalf("OR branch widened trusted time: %+v", field)
					}
					timeBounds[field.Operator] = true
				}
				if field.DimensionName == "entry_parent_span_id" && (field.Operator != "eq" || len(field.Value) != 1 || field.Value[0] != "") {
					t.Fatalf("Root condition drifted in OR branch: %+v", field)
				}
				if _, ok := wanted[field.DimensionName]; ok {
					wanted[field.DimensionName] = true
				}
			}
			for field, found := range wanted {
				if !found {
					t.Fatalf("OR branch omitted trusted %s: %+v", field, group)
				}
			}
			if !timeBounds["gte"] || !timeBounds["lte"] {
				t.Fatalf("OR branch omitted one time bound: %+v", group)
			}
		}
		return 0, nil, nil, []metadata.RouteInfo{{
			TableID: target.ResultTableID, StorageType: metadata.ElasticsearchStorageType,
			StorageID: strconv.FormatInt(target.StorageClusterID, 10),
		}}, nil
	}
	if _, err := executeESList(context.Background(), request, scope, target, reader, traceprofile.NewESSummaryLimits(traceprofile.ESSummaryMaxRows, traceprofile.ESSummaryMaxOffset)); err != nil {
		t.Fatalf("bounded mixed entry predicate failed: %v", err)
	}
}

func TestESListExecutionPushesAuthorizedParentIntersectionBeforePagination(t *testing.T) {
	for _, relation := range []string{"observed", "missing"} {
		t.Run(relation, func(t *testing.T) {
			request, scope, target := esListHTTPFixture(t)
			request.ESSummaryList.ParentAllowedPairs = []traceprofile.MemberPair{
				{AppID: 7, BkBizID: 42}, {AppID: 8, BkBizID: 42},
			}
			target.RetentionDays = 30
			target.EntryIndexVersion = 2
			request.Filters = traceprofile.Filters{FieldList: []traceprofile.FilterField{
				{FieldName: "entry_type", Value: []string{"request", "background"}, Operator: "eq"},
				{FieldName: "entry_subtype", Value: []string{"scheduled"}, Operator: "eq"},
				{FieldName: "entry_parent_state", Value: []string{relation}, Operator: "eq"},
			}, ConditionList: []string{"and", "and"}}
			reader := func(_ context.Context, query *structured.QueryTs) (int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error) {
				groups, err := query.QueryList[0].Conditions.AnalysisConditions()
				if err != nil || len(groups) != 2 {
					t.Fatalf("parent relation lost mixed-type OR branches: groups=%+v err=%v", groups, err)
				}
				for _, group := range groups {
					foundParent, foundOwners, foundVersion, foundCount := false, false, false, false
					for _, field := range group {
						if field.DimensionName == "entry_index_version" && field.Operator == "eq" &&
							len(field.Value) == 1 && field.Value[0] == "2" {
							foundVersion = true
						}
						if field.DimensionName == "entry_parent_owner_count" && field.Operator == "gte" &&
							len(field.Value) == 1 && field.Value[0] == "0" {
							foundCount = true
						}
						if field.DimensionName == "entry_has_parent" && field.Operator == "eq" &&
							len(field.Value) == 1 && field.Value[0] == "true" {
							foundParent = true
						}
						wantOp := "eq"
						if relation == "missing" {
							wantOp = "ne"
						}
						if field.DimensionName == "entry_parent_owner_keys" && field.Operator == wantOp &&
							len(field.Value) == 2 && field.Value[0] == "42:7" && field.Value[1] == "42:8" {
							foundOwners = true
						}
					}
					if !foundParent || !foundOwners || !foundVersion || !foundCount {
						t.Fatalf("parent relation was not pushed to every ES branch: %+v", group)
					}
				}
				return 0, nil, nil, []metadata.RouteInfo{{
					TableID: target.ResultTableID, StorageType: metadata.ElasticsearchStorageType,
					StorageID: strconv.FormatInt(target.StorageClusterID, 10),
				}}, nil
			}
			if _, err := executeESList(context.Background(), request, scope, target, reader, traceprofile.NewESSummaryLimits(traceprofile.ESSummaryMaxRows, traceprofile.ESSummaryMaxOffset)); err != nil {
				t.Fatalf("authorized cross-app relation query failed: %v", err)
			}
		})
	}
}

func TestESListExecutionPushesEntryNameDurationAndErrorBeforeTotal(t *testing.T) {
	request, scope, target := esListHTTPFixture(t)
	request.Filters = traceprofile.Filters{FieldList: []traceprofile.FilterField{
		{FieldName: "entry_service_name", Value: []string{"checkout"}, Operator: "eq"},
		{FieldName: "entry_span_name", Value: []string{"GET /pay"}, Operator: "eq"},
		{FieldName: "entry_kind", Value: []string{"2"}, Operator: "eq"},
		{FieldName: "trace_duration_gte", Value: []string{"100"}, Operator: "gte"},
		{FieldName: "trace_duration_lte", Value: []string{"1000"}, Operator: "lte"},
		{FieldName: "error", Value: []string{"false"}, Operator: "eq"},
	}, ConditionList: []string{"and", "and", "and", "and", "and"}}
	reader := func(_ context.Context, query *structured.QueryTs) (int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error) {
		groups, err := query.QueryList[0].Conditions.AnalysisConditions()
		if err != nil || len(groups) != 1 || !query.QueryList[0].ExactTotal {
			t.Fatalf("indexed entry filters lost before exact total: %+v err=%v", groups, err)
		}
		seen := make(map[string]string)
		for _, field := range groups[0] {
			seen[field.DimensionName] = field.Operator
		}
		for field, operator := range map[string]string{
			"entry_service_name": "eq", "entry_span_name": "eq", "entry_kind": "eq",
			"trace_duration": "lte", "error": "eq",
		} {
			if seen[field] != operator {
				t.Fatalf("missing indexed filter %s: %+v", field, groups[0])
			}
		}
		return 0, nil, nil, []metadata.RouteInfo{{
			TableID: target.ResultTableID, StorageType: metadata.ElasticsearchStorageType,
			StorageID: strconv.FormatInt(target.StorageClusterID, 10),
		}}, nil
	}
	if _, err := executeESList(context.Background(), request, scope, target, reader, traceprofile.NewESSummaryLimits(traceprofile.ESSummaryMaxRows, traceprofile.ESSummaryMaxOffset)); err != nil {
		t.Fatalf("indexed entry query failed: %v", err)
	}
}

func TestESListExecutionUsesV2ParentFlagForRoot(t *testing.T) {
	request, scope, target := esListHTTPFixture(t)
	target.RetentionDays = 30
	target.EntryIndexVersion = 2
	request.Filters = traceprofile.Filters{FieldList: []traceprofile.FilterField{
		{FieldName: "entry_parent_state", Value: []string{"root"}, Operator: "eq"},
	}}
	reader := func(_ context.Context, query *structured.QueryTs) (int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error) {
		groups, err := query.QueryList[0].Conditions.AnalysisConditions()
		if err != nil || len(groups) != 1 {
			t.Fatalf("V2 root query shape invalid: %+v err=%v", groups, err)
		}
		found := false
		for _, field := range groups[0] {
			if field.DimensionName == "entry_has_parent" && field.Operator == "eq" &&
				len(field.Value) == 1 && field.Value[0] == "false" {
				found = true
			}
		}
		if !found {
			t.Fatalf("V2 root did not use exact boolean parent flag: %+v", groups[0])
		}
		return 0, nil, nil, []metadata.RouteInfo{{
			TableID: target.ResultTableID, StorageType: metadata.ElasticsearchStorageType,
			StorageID: strconv.FormatInt(target.StorageClusterID, 10),
		}}, nil
	}
	if _, err := executeESList(context.Background(), request, scope, target, reader, traceprofile.NewESSummaryLimits(traceprofile.ESSummaryMaxRows, traceprofile.ESSummaryMaxOffset)); err != nil {
		t.Fatalf("V2 root query failed: %v", err)
	}
}
