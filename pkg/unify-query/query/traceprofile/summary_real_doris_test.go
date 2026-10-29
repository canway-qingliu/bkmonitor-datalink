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
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/dorisclient"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

// DORIS_SUMMARY_TEST_ADDR 显式指定本机隔离 FE；用例只创建并删除随机临时租户库。
func TestSummaryProfileReadsRealDorisTotalOrderAndEmptyPage(t *testing.T) {
	fixture := realDorisFixtureFor(t, "DORIS_SUMMARY_TEST_ADDR", "codexsummary")
	ctx, admin := fixture.ctx, fixture.admin
	host, port := fixture.host, fixture.port
	tenant, database, minute := fixture.tenant, fixture.database, fixture.minute
	const table = "otel_trace_summary"
	date := minute.Format("2006-01-02")
	upperDate := minute.AddDate(0, 0, 1).Format("2006-01-02")
	createSummaryRealTable(t, ctx, admin, database, table, upperDate)
	olderTrace := strings.Repeat("a", 31) + "1"
	newerTrace := strings.Repeat("a", 31) + "2"
	for _, row := range []struct {
		tenant, domain, appID, traceID string
		startUS                        int64
	}{
		{tenant, database, "7", olderTrace, minute.Add(10 * time.Second).UnixMicro()},
		{tenant, database, "7", newerTrace, minute.Add(20 * time.Second).UnixMicro()},
		{tenant, database, "8", strings.Repeat("b", 31) + "1", minute.Add(30 * time.Second).UnixMicro()},
		{"other", "other_kapm", "7", strings.Repeat("c", 31) + "1", minute.Add(40 * time.Second).UnixMicro()},
	} {
		insertSummaryRealRow(t, ctx, admin, database, table, date, row.tenant,
			row.domain, row.appID, row.traceID, row.startUS)
	}

	connection, err := dorisclient.New(dorisclient.Options{
		ClusterID: "local-summary", Host: host, Port: port, Username: "root", MaxRows: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	target := DorisTarget{ClusterID: "local-summary", Database: database, Table: table,
		SchemaVersion: tenantSummarySchemaVersion}
	snapshot, err := NewRouteSnapshot(7, []ReadRoute{{
		BkTenantID: tenant, BkBizID: 7, Profile: metadata.TraceProfileSummary,
		ObservationDomainID: database, PublishedVersion: 7, Doris: &target,
	}})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutor(snapshot, map[string]Querier{"local-summary": connection}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := ResolveScope(tenant, "bkcc__7", database)
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(0)
	request := Request{
		Profile: metadata.TraceProfileSummary, ObservationDomainID: database,
		StartUS: minute.UnixMicro(), EndUS: minute.Add(time.Minute).UnixMicro(),
		Limit: 1, Offset: &offset,
		Filters: Filters{FieldList: []FilterField{{
			FieldName: "app_id", Value: []string{"7"}, Operator: "eq",
		}}},
	}
	first, err := executor.Execute(ctx, request, scope)
	if err != nil || first.Total != 2 || len(first.Items) != 1 || first.Items[0]["trace_id"] != newerTrace {
		t.Fatalf("real Doris Summary first page lost count, filter or order: %+v err=%v", first, err)
	}
	values, ok := first.Items[0]["values"].(map[string]any)
	if !ok || values["span_count"] != int64(2) || values["partial"] != false ||
		values["trace_date"] != date {
		t.Fatalf("real Doris Summary types were not decoded: %+v", first.Items[0])
	}
	statistics, ok := values["category_statistics"].(map[string]any)
	if !ok || statistics["http"] != json.Number("2") {
		t.Fatalf("real Doris Summary JSON was not decoded: %+v", values["category_statistics"])
	}
	offset = 1
	second, err := executor.Execute(ctx, request, scope)
	if err != nil || second.Total != 2 || len(second.Items) != 1 || second.Items[0]["trace_id"] != olderTrace {
		t.Fatalf("real Doris Summary second page lost stable order: %+v err=%v", second, err)
	}
	offset = 2
	empty, err := executor.Execute(ctx, request, scope)
	if err != nil || empty.Total != 2 || len(empty.Items) != 0 {
		t.Fatalf("real Doris Summary empty page lost exact total: %+v err=%v", empty, err)
	}
	offset = 0
	request.Filters.FieldList[0].Value = []string{"999"}
	zero, err := executor.Execute(ctx, request, scope)
	if err != nil || zero.Total != 0 || len(zero.Items) != 0 {
		t.Fatalf("real Doris Summary zero-match page lost count sentinel: %+v err=%v", zero, err)
	}
	memberRequest := Request{
		Profile: metadata.TraceProfileSummary, ObservationDomainID: database, Scene: "global",
		StartUS: minute.UnixMicro(), EndUS: minute.Add(time.Minute).UnixMicro(), Limit: 2,
		MemberLookup: &MemberLookup{TraceID: newerTrace, ExpectedRouteVersion: 7,
			AllowedPairs: []MemberPair{{AppID: 7, BkBizID: 7}}},
	}
	member, err := executor.Execute(ctx, memberRequest, scope)
	if err != nil || member.Total != 1 || len(member.Items) != 1 || member.Items[0]["trace_id"] != newerTrace {
		t.Fatalf("real Doris Summary exact member lost trusted row or snapshot total: %+v err=%v", member, err)
	}
}

func createSummaryRealTable(t *testing.T, ctx context.Context, db *sql.DB,
	database, table, upperDate string) {
	t.Helper()
	statement := fmt.Sprintf(`CREATE TABLE %s.%s (
  trace_date DATE NOT NULL, app_id VARCHAR(128) NOT NULL, trace_id VARCHAR(32) NOT NULL,
  bk_tenant_id VARCHAR(64) NOT NULL, observation_domain_id VARCHAR(128) NOT NULL,
  bk_biz_id BIGINT NOT NULL, app_name VARCHAR(255) NOT NULL,
  trace_start_time_us BIGINT NOT NULL, trace_end_time_us BIGINT NOT NULL,
  duration_us BIGINT NOT NULL, span_count BIGINT NOT NULL, error_count BIGINT NOT NULL,
  service_count BIGINT NOT NULL, hierarchy_count BIGINT NOT NULL,
  span_min_duration_us BIGINT NOT NULL, span_max_duration_us BIGINT NOT NULL,
  event_time_ms BIGINT NOT NULL, root_service_name VARCHAR(512) NULL,
  root_service_span_id VARCHAR(32) NOT NULL, root_service_span_name STRING NOT NULL,
  root_service_kind INT NOT NULL, root_service_category VARCHAR(32) NOT NULL,
  root_service_status_code JSON NULL, root_span_id VARCHAR(32) NOT NULL,
  root_span_name STRING NOT NULL, root_span_service_name VARCHAR(512) NULL,
  root_span_kind INT NOT NULL, category_statistics JSON NOT NULL,
  kind_statistics JSON NOT NULL, partial BOOLEAN NOT NULL, computed_at_ms BIGINT NOT NULL
) UNIQUE KEY (trace_date, app_id, trace_id, bk_tenant_id, observation_domain_id)
PARTITION BY RANGE (trace_date) (PARTITION p_recent VALUES LESS THAN ('%s'))
DISTRIBUTED BY HASH (trace_id) BUCKETS 1
PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
  "function_column.sequence_col"="computed_at_ms")`, database, table, upperDate)
	if _, err := db.ExecContext(ctx, statement); err != nil {
		t.Fatalf("create isolated Summary table: %v", err)
	}
}

func insertSummaryRealRow(t *testing.T, ctx context.Context, db *sql.DB,
	database, table, date, tenant, domain, appID, traceID string, startUS int64) {
	t.Helper()
	statement := fmt.Sprintf(`INSERT INTO %s.%s VALUES
('%s','%s','%s','%s','%s',7,'checkout',%d,%d,250000,2,1,1,1,100000,250000,%d,
'checkout','1112131415161718','GET /checkout',2,'http','503',
'1112131415161718','GET /checkout','checkout',2,
'{"async_backend":0,"db":0,"http":2,"messaging":0,"other":0,"rpc":0}',
'{"async":0,"interval":0,"sync":2,"unspecified":0}',false,%d)`,
		database, table, date, appID, traceID, tenant, domain,
		startUS, startUS+250_000, startUS/1_000, startUS/1_000+1)
	if _, err := db.ExecContext(ctx, statement); err != nil {
		t.Fatalf("insert isolated Summary row: %v", err)
	}
}
