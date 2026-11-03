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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

func namingDocument(t *testing.T, tenant, database, table string, appID int64) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"route_snapshot_version": 7, "bk_tenant_id": tenant,
		"read_routes": []map[string]any{{
			"bk_tenant_id": tenant, "bk_biz_id": int64(132), "app_id": appID, "profile": "trace_span",
			"database": database, "table": table, "cluster_id": 18, "cluster_name": "doris-test",
			"schema_version": "sha256:0bf5aac6160a636e",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func namingRequest(appID int64) Request {
	request := testRequest(metadata.TraceProfileSpan)
	request.Filters = Filters{}
	if appID > 0 {
		request.Filters = Filters{FieldList: []FilterField{{FieldName: "app_id", Operator: "eq", Value: []string{strconv.FormatInt(appID, 10)}}}}
	}
	return request
}

func TestTraceTableNamingPublishedTargetIsUsedForSQL(t *testing.T) {
	cases := []struct {
		tenant, database, table string
		appID                   int64
	}{
		{"system", "system_kapm", "cw_trace_span", 7},
		{"system", "system_kapm", "cw_trace_span_app_7", 7},
		{"default", "default_kapm", "cw_trace_span", 7},
		{"default", "default_kapm", "cw_trace_span_app_7", 7},
		{"tenant-a", "tenant_a_kapm", "cw_trace_span", 7},
		{"tenant-a", "tenant_a_kapm", "cw_trace_span_app_7", 7},
		{"tenant-a", "tenant_80a707af7dc77ee1_kapm", "cw_trace_span_app_7", 7},
	}
	for _, tc := range cases {
		t.Run(tc.tenant+"/"+tc.database+"/"+tc.table, func(t *testing.T) {
			doc, err := ParsePublishedDocument(namingDocument(t, tc.tenant, tc.database, tc.table, tc.appID), tc.tenant)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := NewRouteSnapshot(doc.Version, doc.Routes)
			if err != nil {
				t.Fatal(err)
			}
			route, err := snapshot.ResolveForApplication(tc.tenant, 132, tc.appID, metadata.TraceProfileSpan)
			if err != nil {
				t.Fatal(err)
			}
			scope, err := ResolveScope(tc.tenant, "bkcc__132")
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := CompileList(metadata.TraceProfileSpan, *route.Doris, scope, namingRequest(tc.appID), Limits{})
			if err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf("FROM `%s`.`%s`", tc.database, tc.table)
			if !strings.Contains(compiled.SnapshotStatement, want) || !strings.Contains(compiled.CountStatement, want) {
				t.Fatalf("published target was replaced: %s", compiled.SnapshotStatement)
			}
			request := namingRequest(tc.appID)
			request.Aggregation = &CountAggregation{Method: "count", IntervalSeconds: 60}
			aggregate, err := CompileAggregate(metadata.TraceProfileSpan, *route.Doris, scope, request, Limits{})
			if err != nil || !strings.Contains(aggregate.Statement, want) ||
				!strings.Contains(aggregate.Statement, "`bk_biz_id` = ?") || !strings.Contains(aggregate.Statement, "`app_id` = ?") {
				t.Fatalf("aggregate lost published target or identity: sql=%s err=%v", aggregate.Statement, err)
			}
		})
	}
}

func TestTraceTableNamingKeepsClosedApplicationScope(t *testing.T) {
	for _, tc := range []struct {
		database, table string
		appID           int64
	}{
		{"system_kapm", "cw_trace_span_132", 0},
		{"system_kapm", "cw_trace_span_132", 7},
		{"system_kapm", "cw_trace_span", 0},
		{"system_kapm", "cw_trace_span_app_7", 0},
		{"system_kapm", "trace_span_biz_132", 0},
		{"system_kapm", "trace_span_biz_133", 0},
		{"system_kapm", "otel_trace_span", 0},
		{"system_kapm", "otel_trace_span_132", 0},
		{"system_kapm", "otel_trace_span_app_7", 7},
		{"system_kapm", "cw_trace_span_biz_133", 0},
		{"system_kapm", "cw_trace_span_app_8", 7},
		{"system_kapm", "cw_trace_span_backup", 7},
		{"system_kapm", "trace_span_biz_0", 0},
		{"system_kapm", "trace_span_app_8", 7},
		{"system_kapm", "trace_span_app_0", 7},
		{"system_kapm", "trace_span_copy", 7},
		{"system_kapm", "trace_summary", 7},
		{"foreign_kapm", "trace_span", 7},
		{"foreign_kapm", "cw_trace_span", 7},
		{"tenant_80a707af7dc77ee1_kapm", "trace_span", 7},
	} {
		if _, err := ParsePublishedDocument(namingDocument(t, "system", tc.database, tc.table, tc.appID), "system"); err == nil {
			t.Fatalf("foreign or unknown target accepted: %+v", tc)
		}
		scope, err := ResolveScope("system", "bkcc__132")
		if err != nil {
			t.Fatal(err)
		}
		target := DorisTarget{ClusterID: "18", Database: tc.database, Table: tc.table, SchemaVersion: "sha256:0bf5aac6160a636e"}
		if _, err := CompileList(metadata.TraceProfileSpan, target, scope, namingRequest(tc.appID), Limits{}); err == nil {
			t.Fatalf("compiler accepted foreign target: %+v", tc)
		}
	}
	target := DorisTarget{ClusterID: "18", Database: "system_kapm", SchemaVersion: "sha256:0bf5aac6160a636e"}
	if _, err := NewRouteSnapshot(7, []ReadRoute{{
		BkTenantID: "system", BkBizID: 132, AppID: 7, Profile: metadata.TraceProfileSpan, Doris: &target,
	}}); err == nil {
		t.Fatal("missing physical table was implicitly derived")
	}
}
