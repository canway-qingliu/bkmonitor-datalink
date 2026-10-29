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
	"errors"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/dorisclient"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

type fakeQuerier struct {
	total         int64
	snapshotTotal *int64
	rows          []map[string]any
	err           error
	statements    []string
	boundArgs     [][]any
}

func (f *fakeQuerier) Query(_ context.Context, statement string, args ...any) (dorisclient.Result, error) {
	f.statements = append(f.statements, statement)
	f.boundArgs = append(f.boundArgs, args)
	if f.err != nil {
		return dorisclient.Result{}, f.err
	}
	if strings.HasPrefix(statement, "SELECT COUNT(*)") {
		return dorisclient.Result{Rows: []map[string]any{{"total": f.total}}}, nil
	}
	if strings.HasPrefix(statement, "WITH ") {
		total := f.total
		if f.snapshotTotal != nil {
			total = *f.snapshotTotal
		}
		if len(f.rows) == 0 {
			profile := metadata.TraceProfileSpan
			if strings.Contains(statement, "otel_trace_summary") {
				profile = metadata.TraceProfileSummary
			}
			empty := map[string]any{"total": total}
			for _, column := range projectionForProfile(profile) {
				empty[column] = nil
			}
			return dorisclient.Result{Rows: []map[string]any{empty}}, nil
		}
		joined := make([]map[string]any, 0, len(f.rows))
		for _, row := range f.rows {
			item := make(map[string]any, len(row)+1)
			for key, value := range row {
				item[key] = value
			}
			item["total"] = total
			joined = append(joined, item)
		}
		return dorisclient.Result{Rows: joined}, nil
	}
	return dorisclient.Result{Rows: f.rows}, nil
}

func targetPtr(target DorisTarget) *DorisTarget {
	return &target
}

func testSnapshot(t *testing.T, profiles ...metadata.TraceProfile) *RouteSnapshot {
	t.Helper()
	routes := make([]ReadRoute, 0, len(profiles))
	for _, profile := range profiles {
		routes = append(routes, ReadRoute{
			BkTenantID:          testTenant,
			BkBizID:             testBizID,
			Profile:             profile,
			ObservationDomainID: testDomain,
			PublishedVersion:    3,
			Doris:               targetPtr(testTarget(profile)),
		})
	}
	snapshot, err := NewRouteSnapshot(3, routes)
	if err != nil {
		t.Fatalf("NewRouteSnapshot returned error: %v", err)
	}
	return snapshot
}

func testExecutor(t *testing.T, querier Querier, profiles ...metadata.TraceProfile) *Executor {
	t.Helper()
	if len(profiles) == 0 {
		profiles = []metadata.TraceProfile{metadata.TraceProfileSpan, metadata.TraceProfileSummary}
	}
	executor, err := NewExecutor(testSnapshot(t, profiles...), map[string]Querier{"18": querier}, Limits{})
	if err != nil {
		t.Fatalf("NewExecutor returned error: %v", err)
	}
	return executor
}

func TestExecutorSelectsExactApplicationSpanRouteFromConjunctiveAppFilter(t *testing.T) {
	const database = "tenant_bbc5e661e106c6dc_kapm"
	route := ReadRoute{
		BkTenantID: "system", BkBizID: 132, AppID: 7, Profile: metadata.TraceProfileSpan,
		ObservationDomainID: database,
		Doris:               &DorisTarget{ClusterID: "18", Database: database, Table: "otel_trace_span", SchemaVersion: "sha256:e6e9af09d86b396e"},
	}
	snapshot, err := NewRouteSnapshot(7, []ReadRoute{route})
	if err != nil {
		t.Fatal(err)
	}
	querier := &fakeQuerier{}
	executor, err := NewExecutor(snapshot, map[string]Querier{"18": querier}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{BkTenantID: "system", BkBizID: 132, SpaceUID: "bkcc__132", ObservationDomainID: database}
	request := Request{
		Profile: metadata.TraceProfileSpan, StartUS: 1721024820000000, EndUS: 1721024880000000, Limit: 1,
		Filters: Filters{FieldList: []FilterField{{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"}}},
	}
	if _, err := executor.Execute(context.Background(), request, scope); err != nil {
		t.Fatal(err)
	}
	if len(querier.statements) == 0 || !strings.Contains(querier.statements[0], "`"+database+"`.`otel_trace_span`") {
		t.Fatalf("application Span query used another table: %v", querier.statements)
	}
	querier.statements = nil
	request.Filters = Filters{
		FieldList: []FilterField{
			{FieldName: "data_id", Value: []string{"901"}, Operator: "eq"},
			{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"},
		},
		ConditionList: []string{"and"},
	}
	if _, err := executor.Execute(context.Background(), request, scope); err != nil {
		t.Fatalf("conjunctive app_id after another filter was rejected: %v", err)
	}
	querier.statements = nil
	request.Filters = Filters{}
	if _, err := executor.Execute(context.Background(), request, scope); ErrorCode(err) != CodeRouteNotFound {
		t.Fatalf("missing app_id reached shared table: %v", err)
	}
	if len(querier.statements) != 0 {
		t.Fatal("unscoped Span query reached Doris")
	}
}

func validTraceRow(profile metadata.TraceProfile) map[string]any {
	row := make(map[string]any, len(projectionForProfile(profile)))
	for _, field := range projectionForProfile(profile) {
		row[field] = traceFixtureValue(field)
	}
	return row
}

// traceFixtureValue 用驱动真实返回的形态造值：JSON/VARIANT 与 BOOLEAN 保持
// Doris 侧的原样（文本、整型），由执行器的解码桥接成行合同要求的类型。
func traceFixtureValue(field string) any {
	switch field {
	case "bk_tenant_id":
		return testTenant
	case "observation_domain_id":
		return testDomain
	case "space_uid":
		return testSpaceUID
	case "bk_biz_id":
		return testBizID
	case "data_id":
		return int64(42)
	case "app_id":
		return "app-1"
	case "trace_id":
		return "0123456789abcdef0123456789abcdef"
	case "span_id":
		return "0123456789abcdef"
	case "span_date", "trace_date":
		return "2026-09-23"
	case "partial":
		return int64(1)
	case "resource", "attributes", "category_statistics", "kind_statistics":
		return `{"k": 1}`
	case "events", "links":
		return `[{"k": 1}]`
	case "root_service_status_code":
		return `200`
	}
	if _, integer := traceIntegerFields[field]; integer {
		return int64(1)
	}
	return "text-" + field
}

func TestExecutorReturnsShapedPageWithRealTotal(t *testing.T) {
	querier := &fakeQuerier{total: 7, rows: []map[string]any{validTraceRow(metadata.TraceProfileSpan)}}
	executor := testExecutor(t, querier)
	request := testRequest(metadata.TraceProfileSpan)
	request.Limit = 1

	page, err := executor.Execute(context.Background(), request, testScope(t))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if page.Total != 7 || len(page.Items) != 1 {
		t.Fatalf("unexpected page: total=%d items=%d", page.Total, len(page.Items))
	}
	item := page.Items[0]
	member, ok := item["member"].(map[string]any)
	if !ok {
		t.Fatalf("span item missing member contract: %#v", item)
	}
	if member["data_id"] != int64(42) || member["app_id"] != "app-1" || member["bk_tenant_id"] != testTenant {
		t.Fatalf("unexpected member: %#v", member)
	}
	values, ok := item["values"].(map[string]any)
	if !ok {
		t.Fatalf("span item missing values: %#v", item)
	}
	attributes, ok := values["attributes"].(map[string]any)
	if !ok {
		t.Fatalf("VARIANT column was not decoded into the row contract: %#v", values["attributes"])
	}
	if number, isNumber := attributes["k"].(json.Number); !isNumber || number.String() != "1" {
		t.Fatalf("VARIANT numbers must stay exact: %#v", attributes["k"])
	}
	if executor.RouteVersion() != 3 {
		t.Fatalf("unexpected route version %d", executor.RouteVersion())
	}

	if len(querier.statements) != 1 {
		t.Fatalf("ordinary list must read one snapshot, got %d queries", len(querier.statements))
	}
	if !strings.HasPrefix(querier.statements[0], "WITH ") ||
		!strings.Contains(querier.statements[0], "LEFT JOIN") {
		t.Fatalf("ordinary list did not use one count/page snapshot: %#v", querier.statements)
	}
	for index, args := range querier.boundArgs {
		if len(args) == 0 {
			t.Fatalf("query %d sent no bound scope args", index)
		}
	}
}

func TestExecutorRejectsIncompleteOrExcessListPage(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile metadata.TraceProfile
		total   int64
		rows    int
		limit   int64
	}{
		{name: "span short page", profile: metadata.TraceProfileSpan, total: 2, rows: 1, limit: 2},
		{name: "summary missing page", profile: metadata.TraceProfileSummary, total: 1, rows: 0, limit: 2},
		{name: "summary excess page", profile: metadata.TraceProfileSummary, total: 1, rows: 2, limit: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]map[string]any, tc.rows)
			for i := range rows {
				rows[i] = validTraceRow(tc.profile)
			}
			querier := &fakeQuerier{total: tc.total, rows: rows}
			request := testRequest(tc.profile)
			request.Limit = tc.limit

			_, err := testExecutor(t, querier, tc.profile).Execute(context.Background(), request, testScope(t))
			if err == nil || ErrorCode(err) != CodePartialFailure {
				t.Fatalf("COUNT/Page mismatch returned %s (%v), want %s", ErrorCode(err), err, CodePartialFailure)
			}
		})
	}
}

func TestExecutorKeepsTotalForPageBeyondEnd(t *testing.T) {
	querier := &fakeQuerier{total: 2}
	request := testRequest(metadata.TraceProfileSummary)
	request.Limit = 1
	offset := int64(2)
	request.Offset = &offset

	page, err := testExecutor(t, querier, metadata.TraceProfileSummary).Execute(context.Background(), request, testScope(t))
	if err != nil || page.Total != 2 || len(page.Items) != 0 {
		t.Fatalf("empty page lost real total: page=%#v err=%v", page, err)
	}
	if len(querier.statements) != 1 {
		t.Fatalf("empty page must retain total in one read, queries=%#v", querier.statements)
	}
}

func TestExecutorDecodesSummaryPartialIntoBoolean(t *testing.T) {
	querier := &fakeQuerier{total: 1, rows: []map[string]any{validTraceRow(metadata.TraceProfileSummary)}}
	executor := testExecutor(t, querier)

	page, err := executor.Execute(context.Background(), testRequest(metadata.TraceProfileSummary), testScope(t))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	values := page.Items[0]["values"].(map[string]any)
	if values["partial"] != true {
		t.Fatalf("partial was not decoded into bool: %#v", values["partial"])
	}
	if _, isObject := values["kind_statistics"].(map[string]any); !isObject {
		t.Fatalf("JSON column was not decoded into object: %#v", values["kind_statistics"])
	}
}

func TestExecutorMemberLookupRejectsOverflowBeforeReadingRows(t *testing.T) {
	querier := &fakeQuerier{total: 2, rows: []map[string]any{validTraceRow(metadata.TraceProfileSummary)}}
	executor := testExecutor(t, querier, metadata.TraceProfileSummary)
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "global"
	request.Limit = 1
	request.MemberLookup = &MemberLookup{TraceID: "0123456789abcdef0123456789abcdef", ExpectedRouteVersion: 3,
		AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 7}}}

	_, err := executor.Execute(context.Background(), request, testScope(t))
	if ErrorCode(err) != CodeLimitExceeded || len(querier.statements) != 1 {
		t.Fatalf("member overflow must stop after count: code=%s statements=%#v", ErrorCode(err), querier.statements)
	}
}

func TestExecutorMemberLookupRejectsStalePublishedRouteBeforeQuery(t *testing.T) {
	querier := &fakeQuerier{total: 0}
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "global"
	request.MemberLookup = &MemberLookup{TraceID: "0123456789abcdef0123456789abcdef",
		ExpectedRouteVersion: 2, AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 7}}}

	_, err := testExecutor(t, querier, metadata.TraceProfileSummary).Execute(
		context.Background(), request, testScope(t))
	if ErrorCode(err) != CodeScopePartial || len(querier.statements) != 0 {
		t.Fatalf("stale route was queried or accepted: code=%s statements=%#v", ErrorCode(err), querier.statements)
	}
}

func TestExecutorDoesNotRouteESMemberLookupIntoDoris(t *testing.T) {
	querier := &fakeQuerier{total: 0}
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "global"
	request.MemberLookup = &MemberLookup{Backend: MemberBackendElasticsearch,
		TraceID: "0123456789abcdef0123456789abcdef", ExpectedRouteVersion: 3,
		AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 7}}}
	_, err := testExecutor(t, querier, metadata.TraceProfileSummary).Execute(context.Background(), request, testScope(t))
	if ErrorCode(err) != CodeUnsupportedSchemaCapability || len(querier.statements) != 0 {
		t.Fatalf("ES member lookup entered Doris executor: code=%s queries=%#v", ErrorCode(err), querier.statements)
	}
}

func TestExecutorMemberLookupUsesTenantPublicationVersionInsteadOfAggregateSnapshot(t *testing.T) {
	document, err := ParsePublishedDocument([]byte(business132Document), "system")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := NewRouteSnapshot(999, document.Routes)
	if err != nil {
		t.Fatal(err)
	}
	querier := &fakeQuerier{total: 0}
	executor, err := NewExecutor(snapshot, map[string]Querier{"18": querier}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := ResolveScope("system", "bkcc__132", "system_kapm")
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "global"
	request.ObservationDomainID = "system_kapm"
	request.MemberLookup = &MemberLookup{TraceID: "0123456789abcdef0123456789abcdef",
		ExpectedRouteVersion: 7, AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 132}}}

	if _, err := executor.Execute(context.Background(), request, scope); err != nil || len(querier.statements) != 2 {
		t.Fatalf("tenant-published route was rejected by aggregate version: err=%v statements=%#v", err, querier.statements)
	}
}

func TestExecutorMemberLookupRejectsStaticRouteWithoutPublishedVersion(t *testing.T) {
	querier := &fakeQuerier{total: 0}
	snapshot := testSnapshot(t, metadata.TraceProfileSummary)
	key := routeKey(testTenant, testBizID, metadata.TraceProfileSummary)
	route := snapshot.routes[key]
	route.PublishedVersion = 0
	snapshot.routes[key] = route
	executor, err := NewExecutor(snapshot, map[string]Querier{"18": querier}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "global"
	request.MemberLookup = &MemberLookup{TraceID: "0123456789abcdef0123456789abcdef",
		ExpectedRouteVersion: 3, AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 7}}}

	_, err = executor.Execute(context.Background(), request, testScope(t))
	if ErrorCode(err) != CodeScopePartial || len(querier.statements) != 0 {
		t.Fatalf("static route cannot attest a tenant publication: code=%s statements=%#v", ErrorCode(err), querier.statements)
	}
}

func TestExecutorMemberLookupReturnsAllExactAuthorizedSummaryRows(t *testing.T) {
	row := validTraceRow(metadata.TraceProfileSummary)
	row["app_id"] = "7"
	querier := &fakeQuerier{total: 1, rows: []map[string]any{row}}
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "global"
	request.Limit = 2
	request.MemberLookup = &MemberLookup{TraceID: "0123456789abcdef0123456789abcdef", ExpectedRouteVersion: 3,
		AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 7}}}

	page, err := testExecutor(t, querier, metadata.TraceProfileSummary).Execute(
		context.Background(), request, testScope(t))
	if err != nil || page.Total != 1 || len(page.Items) != 1 || page.Items[0]["app_id"] != "7" {
		t.Fatalf("exact authorized member was not returned: page=%#v err=%v", page, err)
	}
	if len(querier.statements) != 2 {
		t.Fatalf("member lookup must count and fetch the whole bounded result: %#v", querier.statements)
	}
	if !strings.HasPrefix(querier.statements[0], "SELECT COUNT(*)") ||
		!strings.HasPrefix(querier.statements[1], "WITH ") {
		t.Fatalf("member rows must come from their own total/page snapshot: %#v", querier.statements)
	}
}

func TestExecutorMemberLookupRejectsGrowthAfterBudgetPrecheck(t *testing.T) {
	row := validTraceRow(metadata.TraceProfileSummary)
	row["app_id"] = "7"
	newTotal := int64(2)
	querier := &fakeQuerier{total: 1, snapshotTotal: &newTotal, rows: []map[string]any{row}}
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "global"
	request.Limit = 1
	request.MemberLookup = &MemberLookup{TraceID: "0123456789abcdef0123456789abcdef", ExpectedRouteVersion: 3,
		AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 7}}}

	_, err := testExecutor(t, querier, metadata.TraceProfileSummary).Execute(
		context.Background(), request, testScope(t))
	if err == nil || ErrorCode(err) != CodeLimitExceeded {
		t.Fatalf("member growth after precheck was returned as complete: code=%s err=%v", ErrorCode(err), err)
	}
}

func TestExecutorMemberLookupRejectsRowsOutsidePairedScopeAndTruncation(t *testing.T) {
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "global"
	request.Limit = 2
	request.MemberLookup = &MemberLookup{TraceID: "0123456789abcdef0123456789abcdef", ExpectedRouteVersion: 3,
		AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 7}}}
	for name, mutate := range map[string]func(map[string]any){
		"wrong app": func(row map[string]any) { row["app_id"] = "8" },
		"unknown app with zero business": func(row map[string]any) {
			row["app_id"] = "9"
			row["bk_biz_id"] = int64(0)
		},
		"wrong trace":  func(row map[string]any) { row["trace_id"] = "11111111111111111111111111111111" },
		"wrong tenant": func(row map[string]any) { row["bk_tenant_id"] = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			row := validTraceRow(metadata.TraceProfileSummary)
			row["app_id"] = "7"
			mutate(row)
			querier := &fakeQuerier{total: 1, rows: []map[string]any{row}}
			_, err := testExecutor(t, querier, metadata.TraceProfileSummary).Execute(
				context.Background(), request, testScope(t))
			if ErrorCode(err) != CodeUnauthorizedMember {
				t.Fatalf("out-of-scope member row was accepted: %v", err)
			}
		})
	}
	querier := &fakeQuerier{total: 1}
	_, err := testExecutor(t, querier, metadata.TraceProfileSummary).Execute(
		context.Background(), request, testScope(t))
	if ErrorCode(err) != CodePartialFailure {
		t.Fatalf("missing row was reported as a complete member result: %v", err)
	}
}

func TestExecutorFailsClosedOnRouting(t *testing.T) {
	t.Run("route missing for profile", func(t *testing.T) {
		executor := testExecutor(t, &fakeQuerier{}, metadata.TraceProfileSummary)
		_, err := executor.Execute(context.Background(), testRequest(metadata.TraceProfileSpan), testScope(t))
		if ErrorCode(err) != CodeRouteNotFound {
			t.Fatalf("got %v, want %s", err, CodeRouteNotFound)
		}
	})

	t.Run("cluster has no readonly connection", func(t *testing.T) {
		executor, err := NewExecutor(
			testSnapshot(t, metadata.TraceProfileSpan), map[string]Querier{"99": &fakeQuerier{}}, Limits{},
		)
		if err != nil {
			t.Fatalf("NewExecutor returned error: %v", err)
		}
		_, err = executor.Execute(context.Background(), testRequest(metadata.TraceProfileSpan), testScope(t))
		if ErrorCode(err) != CodeRouteNotFound {
			t.Fatalf("got %v, want %s", err, CodeRouteNotFound)
		}
	})

	t.Run("observation domain outside route", func(t *testing.T) {
		target := testTarget(metadata.TraceProfileSpan)
		route := ReadRoute{
			BkTenantID: testTenant, BkBizID: testBizID, Profile: metadata.TraceProfileSpan,
			ObservationDomainID: testDomain, Doris: targetPtr(target),
		}
		snapshot, err := NewRouteSnapshot(1, []ReadRoute{route})
		if err != nil {
			t.Fatalf("NewRouteSnapshot returned error: %v", err)
		}
		executor, err := NewExecutor(snapshot, map[string]Querier{"18": &fakeQuerier{}}, Limits{})
		if err != nil {
			t.Fatalf("NewExecutor returned error: %v", err)
		}
		scope := testScope(t)
		scope.ObservationDomainID = "other_kapm"
		_, err = executor.Execute(context.Background(), testRequest(metadata.TraceProfileSpan), scope)
		if ErrorCode(err) != CodeUnauthorizedDomain {
			t.Fatalf("got %v, want %s", err, CodeUnauthorizedDomain)
		}
	})
}

func TestNewExecutorRejectsIncompleteConfiguration(t *testing.T) {
	snapshot := testSnapshot(t, metadata.TraceProfileSpan)

	if _, err := NewExecutor(nil, map[string]Querier{"18": &fakeQuerier{}}, Limits{}); err == nil {
		t.Fatal("expected missing snapshot rejected")
	}
	if _, err := NewExecutor(snapshot, nil, Limits{}); err == nil {
		t.Fatal("expected missing connections rejected")
	}
	if _, err := NewExecutor(snapshot, map[string]Querier{"": &fakeQuerier{}}, Limits{}); err == nil {
		t.Fatal("expected empty cluster id rejected")
	}
	if _, err := NewExecutor(snapshot, map[string]Querier{"18": nil}, Limits{}); err == nil {
		t.Fatal("expected nil connection rejected")
	}
}

func TestExecutorFailsClosedOnStorageAndContract(t *testing.T) {
	cases := []struct {
		name    string
		querier *fakeQuerier
		mutate  func(row map[string]any)
	}{
		{
			name:    "storage failure",
			querier: &fakeQuerier{err: errors.New("dial timeout")},
		},
		{
			name:    "missing projected column",
			querier: &fakeQuerier{total: 1},
			mutate: func(row map[string]any) {
				delete(row, "attributes")
			},
		},
		{
			name:    "partial not boolean",
			querier: &fakeQuerier{total: 1},
			mutate: func(row map[string]any) {
				row["partial"] = "maybe"
			},
		},
		{
			name:    "json column with trailing content",
			querier: &fakeQuerier{total: 1},
			mutate: func(row map[string]any) {
				row["attributes"] = `{"k": 1} trailing`
			},
		},
		{
			name:    "json column is not an object",
			querier: &fakeQuerier{total: 1},
			mutate: func(row map[string]any) {
				row["attributes"] = `plain text`
			},
		},
		{
			name:    "trace id outside contract",
			querier: &fakeQuerier{total: 1},
			mutate: func(row map[string]any) {
				row["trace_id"] = "not-a-trace-id"
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := validTraceRow(metadata.TraceProfileSpan)
			if tc.mutate != nil {
				tc.mutate(row)
			}
			tc.querier.rows = []map[string]any{row}
			executor := testExecutor(t, tc.querier)
			_, err := executor.Execute(context.Background(), testRequest(metadata.TraceProfileSpan), testScope(t))
			if err == nil {
				t.Fatal("expected execution rejected")
			}
			if code := ErrorCode(err); code != CodePartialFailure {
				t.Fatalf("got %s (%v), want %s", code, err, CodePartialFailure)
			}
		})
	}
}

func TestExecutorRejectsUnusableSnapshotResult(t *testing.T) {
	first := validTraceRow(metadata.TraceProfileSpan)
	first["total"] = int64(2)
	second := validTraceRow(metadata.TraceProfileSpan)
	second["total"] = int64(3)
	empty := map[string]any{"total": int64(1)}
	for _, column := range projectionForProfile(metadata.TraceProfileSpan) {
		empty[column] = nil
	}
	cases := []struct {
		name string
		rows []map[string]any
	}{
		{name: "missing total column", rows: []map[string]any{{"counted": int64(1)}}},
		{name: "negative total", rows: []map[string]any{{"total": int64(-1)}}},
		{name: "non canonical total", rows: []map[string]any{{"total": "many"}}},
		{name: "inconsistent totals", rows: []map[string]any{first, second}},
		{name: "sentinel mixed with rows", rows: []map[string]any{empty, second}},
		{name: "no rows", rows: []map[string]any{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor := testExecutor(t, &snapshotResultQuerier{rows: tc.rows})
			_, err := executor.Execute(context.Background(), testRequest(metadata.TraceProfileSpan), testScope(t))
			if err == nil {
				t.Fatal("expected execution rejected")
			}
			if code := ErrorCode(err); code != CodePartialFailure {
				t.Fatalf("got %s (%v), want %s", code, err, CodePartialFailure)
			}
		})
	}
}

type snapshotResultQuerier struct {
	rows []map[string]any
}

func (q *snapshotResultQuerier) Query(_ context.Context, _ string, _ ...any) (dorisclient.Result, error) {
	return dorisclient.Result{Rows: q.rows}, nil
}
