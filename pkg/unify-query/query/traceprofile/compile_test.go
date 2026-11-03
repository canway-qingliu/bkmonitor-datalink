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
	"fmt"
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
	return DorisTarget{
		ClusterID: "18", Database: testDomain, Table: "cw_trace_span_app_7", SchemaVersion: "2026-09-24",
	}
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
		Filters:          Filters{FieldList: []FilterField{{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"}}},
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
		!strings.Contains(compiled.SnapshotStatement, "FROM `t1_kapm`.`cw_trace_span_app_7`") {
		t.Fatalf("unexpected statement head: %s", compiled.SnapshotStatement)
	}
	if !strings.Contains(compiled.CountStatement, "SELECT COUNT(*) AS `total` FROM `t1_kapm`.`cw_trace_span_app_7`") {
		t.Fatalf("unexpected count statement: %s", compiled.CountStatement)
	}

	wantArgs := []any{"2026-09-23", "2026-09-23", request.StartUS, request.EndUS,
		testSpaceUID, testBizID, "7"}
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

func TestTypedCompilersRejectRetiredBusinessTargets(t *testing.T) {
	scope := Scope{BkTenantID: "system", BkBizID: 132, SpaceUID: "bkcc__132"}
	for _, appID := range []int64{0, 7} {
		for _, table := range []string{"cw_trace_span_132", "cw_trace_span_biz_132", "otel_trace_span_132", "trace_span_biz_132"} {
			t.Run(fmt.Sprintf("%s/app=%d", table, appID), func(t *testing.T) {
				request := namingRequest(appID)
				target := DorisTarget{ClusterID: "18", Database: "system_kapm", Table: table, SchemaVersion: "test"}
				if _, err := CompileList(request.Profile, target, scope, request, Limits{}); ErrorCode(err) != CodeRouteNotFound {
					t.Errorf("list compiler accepted retired business target: %v", err)
				}
				request.Aggregation = &CountAggregation{Method: "count", IntervalSeconds: 60}
				if _, err := CompileAggregate(request.Profile, target, scope, request, Limits{}); ErrorCode(err) != CodeRouteNotFound {
					t.Errorf("aggregate compiler accepted retired business target: %v", err)
				}
			})
		}
	}
}

func TestCompileListReadsApplication42InTheRequestedUTCWindow(t *testing.T) {
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
		ClusterID: "18", Database: "system_kapm", Table: "cw_trace_span_app_42",
		SchemaVersion: "sha256:0bf5aac6160a636e",
	}
	compiled, err := CompileList(metadata.TraceProfileSpan, target, scope, request, Limits{})
	if err != nil {
		t.Fatalf("application 42 query rejected: %v", err)
	}
	if !strings.Contains(compiled.SnapshotStatement, "FROM `system_kapm`.`cw_trace_span_app_42`") ||
		!strings.Contains(compiled.SnapshotStatement, "`app_id` = ?") ||
		!strings.Contains(compiled.SnapshotStatement, "LIMIT 50 OFFSET 0") {
		t.Fatalf("query did not target the application table and requested page: %s", compiled.SnapshotStatement)
	}
	if len(compiled.SnapshotArgs) != 7 || compiled.SnapshotArgs[0] != "2026-09-30" ||
		compiled.SnapshotArgs[1] != "2026-09-30" || compiled.SnapshotArgs[2] != request.StartUS ||
		compiled.SnapshotArgs[3] != request.EndUS || compiled.SnapshotArgs[5] != int64(132) ||
		compiled.SnapshotArgs[6] != "42" {
		t.Fatalf("query lost UTC window or business scope: %#v", compiled.SnapshotArgs)
	}
}

func TestCompileListKeepsConditionAlignmentWhenScopePredicateRepeats(t *testing.T) {
	request := testRequest(metadata.TraceProfileSpan)
	request.Filters = Filters{
		FieldList: []FilterField{
			{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"},
			{FieldName: "space_uid", Value: []string{testSpaceUID}, Operator: "eq"},
		},
		ConditionList: []string{"and"},
	}
	statement := mustCompile(t, request, Limits{}).SnapshotStatement

	if !strings.Contains(statement, "(`app_id` = ? AND `space_uid` = ?)") {
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
			request.Filters = Filters{FieldList: []FilterField{request.Filters.FieldList[0], tc.field}, ConditionList: []string{"and"}}
			if code := compileErrorCode(t, request, Limits{}); code != tc.wantCode {
				t.Fatalf("got error code %s, want %s", code, tc.wantCode)
			}
		})
	}

	t.Run("equal repeat is accepted once per isolation column", func(t *testing.T) {
		request := testRequest(metadata.TraceProfileSpan)
		request.Filters = Filters{
			FieldList: []FilterField{
				{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"},
				{FieldName: "space_uid", Value: []string{testSpaceUID}, Operator: "eq"},
			},
			ConditionList: []string{"and"},
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
			field: FilterField{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"},
		},
		{
			name:  "data id eq allowed",
			field: FilterField{FieldName: "data_id", Value: []string{"42"}, Operator: "eq"},
		},
		{
			name:     "app contains rejected",
			field:    FilterField{FieldName: "app_id", Value: []string{"app"}, Operator: "contains"},
			wantCode: CodeRouteNotFound,
		},
		{
			name:     "app ne rejected",
			field:    FilterField{FieldName: "app_id", Value: []string{"7"}, Operator: "ne"},
			wantCode: CodeRouteNotFound,
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
			if tc.field.FieldName == "app_id" {
				request.Filters = Filters{FieldList: []FilterField{tc.field}}
			} else {
				request.Filters.FieldList = append(request.Filters.FieldList, tc.field)
				request.Filters.ConditionList = []string{"and"}
			}
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
			profile:  metadata.TraceProfileSpan,
			orderBy:  []string{"attributes"},
			wantCode: CodeUnsupportedSchemaCapability,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := testRequest(tc.profile)
			request.Filters.FieldList = append(request.Filters.FieldList, tc.filters...)
			if len(tc.filters) > 0 {
				request.Filters.ConditionList = []string{"and"}
			}
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
		request := testRequest(metadata.TraceProfileSpan)
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
			{FieldName: "service_instance_id", Value: []string{"' OR 1=1 --"}, Operator: "eq"},
			{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"},
		},
		ConditionList: []string{"and", "and", "and", "and", "and"},
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

	wantTailArgs := []any{`100\%\_x\\%`, int64(2), int64(1000), int64(2000), "' OR 1=1 --", "7"}
	tail := compiled.SnapshotArgs[len(compiled.SnapshotArgs)-len(wantTailArgs):]
	if !reflect.DeepEqual(tail, wantTailArgs) {
		t.Fatalf("unexpected bound client args: %#v", tail)
	}
}

func TestCompileListBindsContainsVariants(t *testing.T) {
	request := testRequest(metadata.TraceProfileSpan)
	request.Filters = Filters{
		FieldList: []FilterField{
			{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"},
			{FieldName: "app_name", Value: []string{"a", "b"}, Operator: "ncontains", IsWildcard: true},
		},
		ConditionList: []string{"and"},
	}
	compiled := mustCompile(t, request, Limits{})
	statement := compiled.SnapshotStatement

	if !strings.Contains(statement, "NOT (`app_name` LIKE ? OR `app_name` LIKE ?)") {
		t.Fatalf("unexpected ncontains predicate: %s", statement)
	}
	tail := compiled.SnapshotArgs[len(compiled.SnapshotArgs)-2:]
	if !reflect.DeepEqual(tail, []any{"%a%", "%b%"}) {
		t.Fatalf("unexpected bound args: %#v", tail)
	}
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
