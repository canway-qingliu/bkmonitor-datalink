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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

const (
	testTenant   = "t1"
	testDomain   = "t1_kapm"
	testSpaceUID = "bkcc__7"
	testBizID    = int64(7)
)

func testScope(t *testing.T) Scope {
	t.Helper()
	scope, err := ResolveScope(testTenant, testSpaceUID)
	if err != nil {
		t.Fatalf("ResolveScope returned error: %v", err)
	}
	return scope
}

func testTarget(profile metadata.TraceProfile) DorisTarget {
	table := "otel_trace_span_7"
	schemaVersion := "2026-09-24"
	if profile == metadata.TraceProfileSummary {
		table = "otel_trace_summary"
		schemaVersion = tenantSummarySchemaVersion
	}
	return DorisTarget{ClusterID: "18", Database: testDomain, Table: table, SchemaVersion: schemaVersion}
}

func testWindow() (int64, int64) {
	start := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	return start.UnixMicro(), end.UnixMicro()
}

func testRequest(profile metadata.TraceProfile) Request {
	startUS, endUS := testWindow()
	return Request{
		Profile:          profile,
		ScopeFingerprint: strings.Repeat("a", 64),
		StartUS:          startUS,
		EndUS:            endUS,
		Limit:            100,
	}
}

func mustCompile(t *testing.T, request Request, limits Limits) CompiledList {
	t.Helper()
	compiled, err := CompileList(request.Profile, testTarget(request.Profile), testScope(t), request, limits)
	if err != nil {
		t.Fatalf("CompileList returned error: %v", err)
	}
	return compiled
}

func compileErrorCode(t *testing.T, request Request, limits Limits) string {
	t.Helper()
	_, err := CompileList(request.Profile, testTarget(request.Profile), testScope(t), request, limits)
	if err == nil {
		t.Fatal("expected CompileList rejected")
	}
	return ErrorCode(err)
}

func TestCompileListBindsSpanScopeTimeAndCanonicalOrder(t *testing.T) {
	request := testRequest(metadata.TraceProfileSpan)
	compiled := mustCompile(t, request, Limits{})

	for _, column := range projectionForProfile(metadata.TraceProfileSpan) {
		if !strings.Contains(compiled.SnapshotStatement, "`"+column+"`") {
			t.Fatalf("projection missing column %s: %s", column, compiled.SnapshotStatement)
		}
	}
	for _, forbidden := range []string{"dtEventTime", "thedate", "bkdata", "BKQLIMIT"} {
		if strings.Contains(compiled.SnapshotStatement, forbidden) {
			t.Fatalf("statement leaked BKSQL macro %s: %s", forbidden, compiled.SnapshotStatement)
		}
	}

	wantWhere := "WHERE `span_date` >= ? AND `span_date` <= ? AND `start_time_us` >= ? AND `start_time_us` < ?" +
		" AND `space_uid` = ? AND `bk_biz_id` = ?"
	if !strings.Contains(compiled.SnapshotStatement, wantWhere) {
		t.Fatalf("unexpected where clause: %s", compiled.SnapshotStatement)
	}
	wantOrder := "ORDER BY `start_time_us` DESC, `trace_id` ASC, `span_id` ASC LIMIT 100 OFFSET 0"
	if !strings.Contains(compiled.SnapshotStatement, wantOrder) {
		t.Fatalf("unexpected order/limit clause: %s", compiled.SnapshotStatement)
	}
	if !strings.HasPrefix(compiled.SnapshotStatement, "WITH filtered AS (SELECT `app_id`") ||
		!strings.Contains(compiled.SnapshotStatement, "FROM `t1_kapm`.`otel_trace_span_7`") {
		t.Fatalf("unexpected statement head: %s", compiled.SnapshotStatement)
	}
	if !strings.Contains(compiled.CountStatement, "SELECT COUNT(*) AS `total` FROM `t1_kapm`.`otel_trace_span_7`") {
		t.Fatalf("unexpected count statement: %s", compiled.CountStatement)
	}

	wantArgs := []any{"2026-09-23", "2026-09-23", request.StartUS, request.EndUS,
		testSpaceUID, testBizID}
	if !reflect.DeepEqual(compiled.SnapshotArgs, wantArgs) {
		t.Fatalf("unexpected bound args: %#v", compiled.SnapshotArgs)
	}
	if !reflect.DeepEqual(compiled.CountArgs, wantArgs) {
		t.Fatalf("count args diverged from snapshot args: %#v", compiled.CountArgs)
	}
	if strings.Count(compiled.SnapshotStatement, "?") != len(compiled.SnapshotArgs) {
		t.Fatalf("placeholder count mismatch: %s", compiled.SnapshotStatement)
	}
}

func TestCompileListReadsBusiness132InTheRequestedUTCWindow(t *testing.T) {
	scope, err := ResolveScope("system", "bkcc__132")
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(0)
	request := Request{
		Profile: metadata.TraceProfileSpan, StartUS: 1790749515960000, EndUS: 1790749816960000,
		Limit: 50, Offset: &offset,
		Filters: Filters{FieldList: []FilterField{{FieldName: "app_id", Value: []string{"42"}, Operator: "eq"}}},
	}
	target := DorisTarget{
		ClusterID: "18", Database: "system_kapm", Table: "otel_trace_span_132",
		SchemaVersion: "sha256:3fee31e7c6376abe",
	}
	compiled, err := CompileList(metadata.TraceProfileSpan, target, scope, request, Limits{})
	if err != nil {
		t.Fatalf("business 132 query rejected: %v", err)
	}
	if !strings.Contains(compiled.SnapshotStatement, "FROM `system_kapm`.`otel_trace_span_132`") ||
		!strings.Contains(compiled.SnapshotStatement, "`app_id` = ?") ||
		!strings.Contains(compiled.SnapshotStatement, "LIMIT 50 OFFSET 0") {
		t.Fatalf("query did not target the business table and requested page: %s", compiled.SnapshotStatement)
	}
	if len(compiled.SnapshotArgs) != 7 || compiled.SnapshotArgs[0] != "2026-09-30" ||
		compiled.SnapshotArgs[1] != "2026-09-30" || compiled.SnapshotArgs[2] != request.StartUS ||
		compiled.SnapshotArgs[3] != request.EndUS || compiled.SnapshotArgs[5] != int64(132) ||
		compiled.SnapshotArgs[6] != "42" {
		t.Fatalf("query lost UTC window or business scope: %#v", compiled.SnapshotArgs)
	}
}

func TestCompileListSummaryUsesSummaryIsolationAndOffset(t *testing.T) {
	request := testRequest(metadata.TraceProfileSummary)
	offset := int64(20)
	request.Offset = &offset
	request.Limit = 50
	compiled := mustCompile(t, request, Limits{})

	if strings.Contains(compiled.SnapshotStatement, "space_uid") || strings.Contains(compiled.SnapshotStatement, "data_id") {
		t.Fatalf("summary statement must not reference span-only columns: %s", compiled.SnapshotStatement)
	}
	wantWhere := "WHERE `trace_date` >= ? AND `trace_date` <= ? AND `trace_start_time_us` >= ? AND `trace_start_time_us` < ?" +
		" AND `bk_biz_id` = ?"
	if !strings.Contains(compiled.SnapshotStatement, wantWhere) {
		t.Fatalf("unexpected summary where clause: %s", compiled.SnapshotStatement)
	}
	wantOrder := "ORDER BY `trace_start_time_us` DESC, `trace_id` ASC, `app_id` ASC LIMIT 50 OFFSET 20"
	if !strings.Contains(compiled.SnapshotStatement, wantOrder) {
		t.Fatalf("unexpected summary order/limit clause: %s", compiled.SnapshotStatement)
	}
}

func TestCompileListMemberLookupBindsExactTraceAndPairedAllowSet(t *testing.T) {
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "global"
	request.Limit = 3
	request.MemberLookup = &MemberLookup{
		TraceID:              "0123456789abcdef0123456789abcdef",
		ExpectedRouteVersion: 3,
		AllowedPairs:         []MemberPair{{AppID: 7, BkBizID: 7}, {AppID: 8, BkBizID: 7}},
	}
	compiled := mustCompile(t, request, Limits{})
	paired := "`trace_id` = ? AND ((`app_id` = ? AND `bk_biz_id` = ?) OR " +
		"(`app_id` = ? AND `bk_biz_id` = ?))"
	if !strings.Contains(compiled.SnapshotStatement, paired) ||
		!strings.Contains(compiled.CountStatement, paired) ||
		strings.Contains(compiled.SnapshotStatement, "`app_id` IN") {
		t.Fatalf("member lookup lost paired exact predicate: %#v", compiled)
	}
	wantTail := []any{"0123456789abcdef0123456789abcdef", "7", int64(7), "8", int64(7)}
	if !reflect.DeepEqual(compiled.SnapshotArgs[len(compiled.SnapshotArgs)-5:], wantTail) ||
		!reflect.DeepEqual(compiled.CountArgs[len(compiled.CountArgs)-5:], wantTail) {
		t.Fatalf("member lookup bound wrong pair values: %#v", compiled)
	}
}

func TestCompileListMemberLookupRejectsPairOutsideTrustedBusiness(t *testing.T) {
	request := testRequest(metadata.TraceProfileSummary)
	request.Scene = "global"
	request.MemberLookup = &MemberLookup{TraceID: "0123456789abcdef0123456789abcdef", ExpectedRouteVersion: 3,
		AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 8}}}
	if code := compileErrorCode(t, request, Limits{}); code != CodeUnauthorizedMember {
		t.Fatalf("cross-business pair was accepted: %s", code)
	}
}

func TestCompileListKeepsConditionAlignmentWhenScopePredicateRepeats(t *testing.T) {
	request := testRequest(metadata.TraceProfileSpan)
	request.Filters = Filters{
		FieldList: []FilterField{
			{FieldName: "app_id", Value: []string{"app-1"}, Operator: "eq"},
			{FieldName: "space_uid", Value: []string{testSpaceUID}, Operator: "eq"},
		},
		ConditionList: []string{"or"},
	}
	statement := mustCompile(t, request, Limits{}).SnapshotStatement

	if !strings.Contains(statement, "(`app_id` = ? OR `space_uid` = ?)") {
		t.Fatalf("client condition list must be preserved verbatim: %s", statement)
	}
}

func TestCompileListRejectsTrustedIdentityRewrite(t *testing.T) {
	cases := []struct {
		name     string
		field    FilterField
		wantCode string
	}{
		{
			name:     "tenant conflict",
			field:    FilterField{FieldName: "bk_tenant_id", Value: []string{"other"}, Operator: "eq"},
			wantCode: CodeUnsupportedSchemaCapability,
		},
		{
			name:     "space conflict",
			field:    FilterField{FieldName: "space_uid", Value: []string{"bkcc__9"}, Operator: "eq"},
			wantCode: CodePredicateConflict,
		},
		{
			name:     "biz conflict",
			field:    FilterField{FieldName: "bk_biz_id", Value: []string{"9"}, Operator: "eq"},
			wantCode: CodePredicateConflict,
		},
		{
			name:     "retired domain field",
			field:    FilterField{FieldName: "observation_domain_id", Value: []string{"other_kapm"}, Operator: "eq"},
			wantCode: CodeUnsupportedSchemaCapability,
		},
		{
			name:     "negation not allowed",
			field:    FilterField{FieldName: "space_uid", Value: []string{testSpaceUID}, Operator: "ne"},
			wantCode: CodePredicateConflict,
		},
		{
			name:     "multi value not allowed",
			field:    FilterField{FieldName: "space_uid", Value: []string{testSpaceUID, "other"}, Operator: "eq"},
			wantCode: CodePredicateConflict,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := testRequest(metadata.TraceProfileSpan)
			request.Filters = Filters{FieldList: []FilterField{tc.field}, ConditionList: []string{}}
			if code := compileErrorCode(t, request, Limits{}); code != tc.wantCode {
				t.Fatalf("got error code %s, want %s", code, tc.wantCode)
			}
		})
	}

	t.Run("equal repeat is accepted once per isolation column", func(t *testing.T) {
		request := testRequest(metadata.TraceProfileSpan)
		request.Filters = Filters{
			FieldList:     []FilterField{{FieldName: "space_uid", Value: []string{testSpaceUID}, Operator: "eq"}},
			ConditionList: []string{},
		}
		statement := mustCompile(t, request, Limits{}).SnapshotStatement
		if strings.Count(statement, "`space_uid` = ?") != 2 {
			t.Fatalf("expected trusted isolation plus echoed equal condition: %s", statement)
		}
	})
}

func TestCompileListOnlyAllowsEqOnMemberColumns(t *testing.T) {
	cases := []struct {
		name     string
		field    FilterField
		wantCode string
	}{
		{
			name:  "app eq allowed",
			field: FilterField{FieldName: "app_id", Value: []string{"app-1"}, Operator: "eq"},
		},
		{
			name:  "data id eq allowed",
			field: FilterField{FieldName: "data_id", Value: []string{"42"}, Operator: "eq"},
		},
		{
			name:     "app contains rejected",
			field:    FilterField{FieldName: "app_id", Value: []string{"app"}, Operator: "contains"},
			wantCode: CodePredicateConflict,
		},
		{
			name:     "app ne rejected",
			field:    FilterField{FieldName: "app_id", Value: []string{"app-1"}, Operator: "ne"},
			wantCode: CodePredicateConflict,
		},
		{
			name:     "data id nexisted rejected",
			field:    FilterField{FieldName: "data_id", Value: []string{}, Operator: "nexisted"},
			wantCode: CodePredicateConflict,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := testRequest(metadata.TraceProfileSpan)
			request.Filters = Filters{FieldList: []FilterField{tc.field}, ConditionList: []string{}}
			_, err := CompileList(request.Profile, testTarget(request.Profile), testScope(t), request, Limits{})
			if tc.wantCode == "" {
				if err != nil {
					t.Fatalf("expected accepted, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected rejected")
			}
			if code := ErrorCode(err); code != tc.wantCode {
				t.Fatalf("got error code %s, want %s", code, tc.wantCode)
			}
		})
	}
}

func TestCompileListRejectsUnsupportedFiltersAndOrders(t *testing.T) {
	cases := []struct {
		name     string
		profile  metadata.TraceProfile
		filters  []FilterField
		orderBy  []string
		wantCode string
	}{
		{
			name:     "unknown column",
			profile:  metadata.TraceProfileSpan,
			filters:  []FilterField{{FieldName: "secret_column", Value: []string{"x"}, Operator: "eq"}},
			wantCode: CodeUnsupportedSchemaCapability,
		},
		{
			name:     "bksql macro column",
			profile:  metadata.TraceProfileSpan,
			filters:  []FilterField{{FieldName: "dtEventTimeStamp", Value: []string{"1"}, Operator: "eq"}},
			wantCode: CodeUnsupportedSchemaCapability,
		},
		{
			name:     "variant column filter",
			profile:  metadata.TraceProfileSpan,
			filters:  []FilterField{{FieldName: "attributes", Value: []string{"x"}, Operator: "eq"}},
			wantCode: CodeUnsupportedSchemaCapability,
		},
		{
			name:     "regexp operator not delivered",
			profile:  metadata.TraceProfileSpan,
			filters:  []FilterField{{FieldName: "span_name", Value: []string{"^a"}, Operator: "req"}},
			wantCode: CodeUnsupportedSchemaCapability,
		},
		{
			name:     "unknown order column",
			profile:  metadata.TraceProfileSpan,
			orderBy:  []string{"-secret_column"},
			wantCode: CodeUnsupportedSchemaCapability,
		},
		{
			name:     "json order column",
			profile:  metadata.TraceProfileSummary,
			orderBy:  []string{"kind_statistics"},
			wantCode: CodeUnsupportedSchemaCapability,
		},
		{
			name:     "summary space uid column absent",
			profile:  metadata.TraceProfileSummary,
			filters:  []FilterField{{FieldName: "space_uid", Value: []string{testSpaceUID}, Operator: "eq"}},
			wantCode: CodeUnsupportedSchemaCapability,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := testRequest(tc.profile)
			request.Filters = Filters{FieldList: tc.filters, ConditionList: []string{}}
			request.OrderBy = tc.orderBy
			if code := compileErrorCode(t, request, Limits{}); code != tc.wantCode {
				t.Fatalf("got error code %s, want %s", code, tc.wantCode)
			}
		})
	}
}

func TestCompileListEnforcesBudgets(t *testing.T) {
	hour := int64(time.Hour / time.Microsecond)

	t.Run("range over budget", func(t *testing.T) {
		request := testRequest(metadata.TraceProfileSpan)
		if code := compileErrorCode(t, request, Limits{MaxRangeUS: hour}); code != CodeUnboundedRange {
			t.Fatalf("got %s, want %s", code, CodeUnboundedRange)
		}
	})

	t.Run("range within budget", func(t *testing.T) {
		request := testRequest(metadata.TraceProfileSpan)
		mustCompile(t, request, Limits{MaxRangeUS: 48 * hour})
	})

	t.Run("inverted range", func(t *testing.T) {
		request := testRequest(metadata.TraceProfileSpan)
		request.EndUS = request.StartUS
		if code := compileErrorCode(t, request, Limits{}); code != CodeUnboundedRange {
			t.Fatalf("got %s, want %s", code, CodeUnboundedRange)
		}
	})

	t.Run("offset over budget", func(t *testing.T) {
		request := testRequest(metadata.TraceProfileSummary)
		offset := int64(1000)
		request.Offset = &offset
		if code := compileErrorCode(t, request, Limits{MaxOffset: 500}); code != CodeLimitExceeded {
			t.Fatalf("got %s, want %s", code, CodeLimitExceeded)
		}
	})

	t.Run("non positive limit", func(t *testing.T) {
		request := testRequest(metadata.TraceProfileSpan)
		request.Limit = 0
		if code := compileErrorCode(t, request, Limits{}); code != CodeLimitExceeded {
			t.Fatalf("got %s, want %s", code, CodeLimitExceeded)
		}
	})

	t.Run("too many order terms", func(t *testing.T) {
		request := testRequest(metadata.TraceProfileSpan)
		request.OrderBy = []string{"duration_us", "-start_time_us", "end_time_us", "status_code", "span_kind"}
		if code := compileErrorCode(t, request, Limits{}); code != CodeLimitExceeded {
			t.Fatalf("got %s, want %s", code, CodeLimitExceeded)
		}
	})
}

func TestCompileListBindsClientValuesWithoutInlining(t *testing.T) {
	request := testRequest(metadata.TraceProfileSpan)
	request.Filters = Filters{
		FieldList: []FilterField{
			{FieldName: "span_name", Value: []string{"100%_x\\"}, Operator: "contains", IsPrefix: true},
			{FieldName: "status_code", Value: []string{"2"}, Operator: "gte"},
			{FieldName: "duration_us", Value: []string{"1000", "2000"}, Operator: "ne"},
			{FieldName: "service_name", Value: []string{}, Operator: "existed"},
			{FieldName: "app_id", Value: []string{"' OR 1=1 --"}, Operator: "eq"},
		},
		ConditionList: []string{"and", "and", "and", "and"},
	}
	compiled := mustCompile(t, request, Limits{})
	statement := compiled.SnapshotStatement

	if strings.Contains(statement, "1=1") || strings.Contains(statement, "1000") || strings.Contains(statement, "2000") {
		t.Fatalf("client value inlined into statement: %s", statement)
	}
	if !strings.Contains(statement, "`span_name` LIKE ?") || !strings.Contains(statement, "`status_code` >= ?") ||
		!strings.Contains(statement, "`duration_us` NOT IN (?, ?)") ||
		!strings.Contains(statement, "`service_name` IS NOT NULL") {
		t.Fatalf("unexpected predicate shape: %s", statement)
	}

	wantTailArgs := []any{`100\%\_x\\%`, int64(2), int64(1000), int64(2000), "' OR 1=1 --"}
	tail := compiled.SnapshotArgs[len(compiled.SnapshotArgs)-len(wantTailArgs):]
	if !reflect.DeepEqual(tail, wantTailArgs) {
		t.Fatalf("unexpected bound client args: %#v", tail)
	}
}

func TestCompileListBindsBoolAndContainsVariants(t *testing.T) {
	request := testRequest(metadata.TraceProfileSummary)
	request.Filters = Filters{
		FieldList: []FilterField{
			{FieldName: "partial", Value: []string{"true"}, Operator: "eq"},
			{FieldName: "app_name", Value: []string{"a", "b"}, Operator: "ncontains", IsWildcard: true},
		},
		ConditionList: []string{"and"},
	}
	compiled := mustCompile(t, request, Limits{})
	statement := compiled.SnapshotStatement

	if !strings.Contains(statement, "`partial` = ?") {
		t.Fatalf("unexpected partial predicate: %s", statement)
	}
	if !strings.Contains(statement, "NOT (`app_name` LIKE ? OR `app_name` LIKE ?)") {
		t.Fatalf("unexpected ncontains predicate: %s", statement)
	}
	tail := compiled.SnapshotArgs[len(compiled.SnapshotArgs)-3:]
	if !reflect.DeepEqual(tail, []any{true, "%a%", "%b%"}) {
		t.Fatalf("unexpected bound args: %#v", tail)
	}

	t.Run("invalid boolean rejected", func(t *testing.T) {
		invalid := testRequest(metadata.TraceProfileSummary)
		invalid.Filters = Filters{
			FieldList:     []FilterField{{FieldName: "partial", Value: []string{"maybe"}, Operator: "eq"}},
			ConditionList: []string{},
		}
		if code := compileErrorCode(t, invalid, Limits{}); code != CodePredicateConflict {
			t.Fatalf("got %s, want %s", code, CodePredicateConflict)
		}
	})
}

func TestCompileListAppendsCanonicalTailWithoutDuplicating(t *testing.T) {
	cases := []struct {
		name    string
		profile metadata.TraceProfile
		orderBy []string
		want    string
	}{
		{
			name:    "client duration first",
			profile: metadata.TraceProfileSpan,
			orderBy: []string{"-duration_us"},
			want:    "ORDER BY `duration_us` DESC, `start_time_us` DESC, `trace_id` ASC, `span_id` ASC",
		},
		{
			name:    "client repeats canonical columns",
			profile: metadata.TraceProfileSpan,
			orderBy: []string{"start_time_us", "-trace_id"},
			want:    "ORDER BY `start_time_us` ASC, `trace_id` DESC, `span_id` ASC",
		},
		{
			name:    "summary client tiebreak only",
			profile: metadata.TraceProfileSummary,
			orderBy: []string{"trace_id"},
			want:    "ORDER BY `trace_id` ASC, `trace_start_time_us` DESC, `app_id` ASC",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := testRequest(tc.profile)
			request.OrderBy = tc.orderBy
			statement := mustCompile(t, request, Limits{}).SnapshotStatement
			if !strings.Contains(statement, tc.want+" LIMIT 100") {
				t.Fatalf("unexpected order clause: %s", statement)
			}
		})
	}
}
