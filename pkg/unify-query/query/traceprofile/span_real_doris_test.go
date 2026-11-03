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
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/dorisclient"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	_ "github.com/go-sql-driver/mysql"
)

type realDorisFixture struct {
	ctx      context.Context
	admin    *sql.DB
	host     string
	port     int
	tenant   string
	database string
	minute   time.Time
}

// realDorisFixtureFor 只连接显式指定的本机隔离 FE，并创建随机临时租户库。
func realDorisFixtureFor(t *testing.T, addressEnv, tenantPrefix string) realDorisFixture {
	t.Helper()
	address := os.Getenv(addressEnv)
	if address == "" {
		t.Skipf("set %s=127.0.0.1:<isolated FE MySQL port> to run", addressEnv)
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		t.Fatalf("real Doris test requires an isolated IPv4 loopback address: %q", address)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		t.Fatalf("real Doris test port is invalid: %q", portText)
	}
	admin, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/", address))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("isolated Doris FE is unavailable: %v", err)
	}
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	tenant := tenantPrefix + hex.EncodeToString(suffix[:])
	database := tenant + "_kapm"
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+database+"`"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP DATABASE `"+database+"`"); err != nil {
			t.Errorf("remove isolated Doris database: %v", err)
		}
	})
	return realDorisFixture{
		ctx: ctx, admin: admin, host: host, port: port,
		tenant: tenant, database: database,
		minute: time.Now().UTC().Truncate(time.Minute).Add(-2 * time.Minute),
	}
}

// DORIS_SPAN_TEST_ADDR 显式指定本机隔离 FE；用例只创建并删除随机临时租户库。
func TestSpanProfileReadsRealDorisVariantArraysAndExactTotal(t *testing.T) {
	fixture := realDorisFixtureFor(t, "DORIS_SPAN_TEST_ADDR", "codexspan")
	ctx, admin := fixture.ctx, fixture.admin
	host, port := fixture.host, fixture.port
	tenant, database, minute := fixture.tenant, fixture.database, fixture.minute
	const table = "cw_trace_span_app_7"
	date := minute.Format("2006-01-02")
	upperDate := minute.AddDate(0, 0, 1).Format("2006-01-02")
	createSpanRealTable(t, ctx, admin, database, table, upperDate)
	traceID := strings.Repeat("a", 32)
	olderSpan := strings.Repeat("b", 15) + "1"
	newerSpan := strings.Repeat("b", 15) + "2"
	for _, row := range []struct {
		spaceUID, spanID string
		bizID, startUS   int64
	}{
		{"bkcc__7", olderSpan, 7, minute.Add(10 * time.Second).UnixMicro()},
		{"bkcc__7", newerSpan, 7, minute.Add(20 * time.Second).UnixMicro()},
		{"bkcc__8", strings.Repeat("c", 15) + "1", 7, minute.Add(30 * time.Second).UnixMicro()},
		{"bkcc__7", strings.Repeat("c", 15) + "2", 8, minute.Add(40 * time.Second).UnixMicro()},
	} {
		insertSpanRealRow(t, ctx, admin, database, table, date, row.spaceUID,
			row.bizID, traceID, row.spanID, row.startUS)
	}

	connection, err := dorisclient.New(dorisclient.Options{
		ClusterID: "local-span", Host: host, Port: port, Username: "root", MaxRows: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	target := DorisTarget{ClusterID: "local-span", Database: database, Table: table,
		SchemaVersion: "local-span-v1"}
	snapshot, err := NewRouteSnapshot(7, []ReadRoute{{
		BkTenantID: tenant, BkBizID: 7, AppID: 7, Profile: metadata.TraceProfileSpan,
		PublishedVersion: 7, Doris: &target,
	}})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutor(snapshot, map[string]Querier{"local-span": connection}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := ResolveScope(tenant, "bkcc__7")
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(0)
	request := Request{
		Profile: metadata.TraceProfileSpan, StartUS: minute.UnixMicro(), EndUS: minute.Add(time.Minute).UnixMicro(),
		Limit: 1, Offset: &offset,
		Filters: Filters{FieldList: []FilterField{
			{FieldName: "app_id", Value: []string{"7"}, Operator: "eq"},
			{FieldName: "trace_id", Value: []string{traceID}, Operator: "eq"},
		}, ConditionList: []string{"and"}},
	}
	first, err := executor.Execute(ctx, request, scope)
	if err != nil || first.Total != 2 || len(first.Items) != 1 || first.Items[0]["span_id"] != newerSpan {
		t.Fatalf("real Doris Span first page lost count, scope or order: %+v err=%v", first, err)
	}
	values, ok := first.Items[0]["values"].(map[string]any)
	if !ok || values["duration_us"] != int64(250_000) || values["ingest_time"] == nil {
		t.Fatalf("real Doris Span numeric fields were not decoded: %+v", first.Items[0])
	}
	resource, ok := values["resource"].(map[string]any)
	if !ok || resource["counter"] != json.Number("9007199254740993") {
		t.Fatalf("real Doris VARIANT resource lost large integer: %+v", values["resource"])
	}
	if events, ok := values["events"].([]any); !ok || len(events) != 1 {
		t.Fatalf("real Doris VARIANT events were not decoded as an array: %+v", values["events"])
	}
	if links, ok := values["links"].([]any); !ok || len(links) != 1 {
		t.Fatalf("real Doris VARIANT links were not decoded as an array: %+v", values["links"])
	}
	offset = 1
	second, err := executor.Execute(ctx, request, scope)
	if err != nil || second.Total != 2 || len(second.Items) != 1 || second.Items[0]["span_id"] != olderSpan {
		t.Fatalf("real Doris Span second page lost stable order: %+v err=%v", second, err)
	}
	offset = 2
	empty, err := executor.Execute(ctx, request, scope)
	if err != nil || empty.Total != 2 || len(empty.Items) != 0 {
		t.Fatalf("real Doris Span empty page lost exact total: %+v err=%v", empty, err)
	}
}

func createSpanRealTable(t *testing.T, ctx context.Context, db *sql.DB,
	database, table, upperDate string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, spanRealTableStatement(database, table, upperDate)); err != nil {
		t.Fatalf("create isolated Span table: %v", err)
	}
}

func spanRealTableStatement(database, table, upperDate string) string {
	return fmt.Sprintf(`CREATE TABLE %s.%s (
  span_date DATE NOT NULL, app_id VARCHAR(128) NOT NULL, trace_id VARCHAR(32) NOT NULL,
  span_id VARCHAR(32) NOT NULL, space_uid VARCHAR(128) NOT NULL,
  bk_biz_id BIGINT NOT NULL, data_id BIGINT NOT NULL, app_name VARCHAR(255) NOT NULL,
  parent_span_id VARCHAR(32) NOT NULL, trace_state STRING NOT NULL,
  span_name STRING NOT NULL, span_kind INT NOT NULL, status_code INT NOT NULL,
  status_message STRING NOT NULL, start_time_us BIGINT NOT NULL,
  end_time_us BIGINT NOT NULL, duration_us BIGINT NOT NULL, event_time_ms BIGINT NULL,
  service_namespace VARCHAR(512) NULL, service_name VARCHAR(512) NULL,
  service_instance_id VARCHAR(512) NULL, deployment_environment VARCHAR(512) NULL,
  resource VARIANT NOT NULL, attributes VARIANT NOT NULL,
  events VARIANT NOT NULL, links VARIANT NOT NULL, ingest_time BIGINT NOT NULL
) UNIQUE KEY (span_date, app_id, trace_id, span_id)
PARTITION BY RANGE (span_date) (PARTITION p_recent VALUES LESS THAN ('%s'))
DISTRIBUTED BY HASH (trace_id) BUCKETS 1
PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
  "function_column.sequence_col"="ingest_time")`, database, table, upperDate)
}

func insertSpanRealRow(t *testing.T, ctx context.Context, db *sql.DB,
	database, table, date, spaceUID string, bizID int64, traceID, spanID string, startUS int64) {
	t.Helper()
	statement := spanRealRowStatement(database, table, date, spaceUID, bizID, traceID, spanID, startUS)
	if _, err := db.ExecContext(ctx, statement); err != nil {
		t.Fatalf("insert isolated Span row: %v", err)
	}
}

func spanRealRowStatement(database, table, date, spaceUID string, bizID int64,
	traceID, spanID string, startUS int64) string {
	return fmt.Sprintf(`INSERT INTO %s.%s (
  span_date, app_id, trace_id, span_id, space_uid, bk_biz_id, data_id, app_name,
  parent_span_id, trace_state, span_name, span_kind, status_code, status_message,
  start_time_us, end_time_us, duration_us, event_time_ms,
  service_namespace, service_name, service_instance_id, deployment_environment,
  resource, attributes, events, links, ingest_time) VALUES
('%s','7','%s','%s','%s',%d,42,'checkout','','',
'GET /checkout',2,2,'boom',%d,%d,250000,%d,
'payments','checkout','checkout-1','test',
'{"service.name":"checkout","counter":9007199254740993}',
'{"http.route":"/checkout","nested":{"counter":9007199254740993}}',
'[{"name":"exception","timestamp":%d,"attributes":{"exception.message":"boom"}}]',
'[{"trace_id":"%s","span_id":"1112131415161718","trace_state":"","attributes":{}}]',%d)`,
		database, table, date, traceID, spanID, spaceUID, bizID,
		startUS, startUS+250_000, startUS/1_000, startUS+200_000,
		strings.Repeat("d", 32), startUS/1_000+1)
}
