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
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSpanRealFixtureInsertMatchesTableColumns(t *testing.T) {
	const database, table, date = "fixture_kapm", "cw_trace_span_app_7", "2026-09-23"
	startUS := time.Date(2026, 9, 23, 0, 0, 10, 0, time.UTC).UnixMicro()
	traceID, spanID := strings.Repeat("a", 32), strings.Repeat("b", 16)
	create := spanRealTableStatement(database, table, "2026-09-24")
	_, definitions, ok := strings.Cut(create, "(\n")
	if !ok {
		t.Fatalf("CREATE TABLE has no column definitions: %s", create)
	}
	definitions, _, ok = strings.Cut(definitions, "\n) UNIQUE KEY")
	if !ok {
		t.Fatalf("CREATE TABLE has no column boundary: %s", create)
	}
	var columns []string
	for _, definition := range strings.Split(definitions, ",") {
		columns = append(columns, strings.Fields(definition)[0])
	}

	for _, tc := range []struct {
		name, spaceUID string
		bizID          int64
	}{
		{"authorized scope", "bkcc__7", 7},
		{"different space", "bkcc__8", 7},
		{"different business", "bkcc__7", 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement := spanRealRowStatement(database, table, date, tc.spaceUID, tc.bizID, traceID, spanID, startUS)
			head, row, ok := strings.Cut(statement, " VALUES")
			if !ok {
				t.Fatalf("INSERT has no VALUES clause: %s", statement)
			}
			row = strings.TrimSpace(row)
			if !strings.HasPrefix(row, "(") || !strings.HasSuffix(row, ")") {
				t.Fatalf("INSERT has no value tuple: %s", statement)
			}
			values := regexp.MustCompile(`'(?:[^']|'')*'|[^,\s]+`).FindAllString(row[1:len(row)-1], -1)
			if len(values) != len(columns) {
				t.Fatalf("INSERT has %d values for %d CREATE TABLE columns", len(values), len(columns))
			}
			_, insertColumns, ok := strings.Cut(head, "(")
			if !ok {
				t.Fatalf("INSERT must name its columns: %s", statement)
			}
			insertColumns = strings.TrimSuffix(strings.TrimSpace(insertColumns), ")")
			listed := strings.Split(insertColumns, ",")
			if len(listed) != len(columns) {
				t.Fatalf("INSERT lists %d columns for %d CREATE TABLE columns", len(listed), len(columns))
			}
			want := []struct{ column, value string }{
				{"span_date", "'2026-09-23'"},
				{"app_id", "'7'"},
				{"trace_id", "'" + traceID + "'"},
				{"span_id", "'" + spanID + "'"},
				{"space_uid", "'" + tc.spaceUID + "'"},
				{"bk_biz_id", strconv.FormatInt(tc.bizID, 10)},
				{"data_id", "42"},
				{"app_name", "'checkout'"},
				{"parent_span_id", "''"},
				{"trace_state", "''"},
				{"span_name", "'GET /checkout'"},
				{"span_kind", "2"},
				{"status_code", "2"},
				{"status_message", "'boom'"},
				{"start_time_us", strconv.FormatInt(startUS, 10)},
				{"end_time_us", strconv.FormatInt(startUS+250_000, 10)},
				{"duration_us", "250000"},
				{"event_time_ms", strconv.FormatInt(startUS/1_000, 10)},
				{"service_namespace", "'payments'"},
				{"service_name", "'checkout'"},
				{"service_instance_id", "'checkout-1'"},
				{"deployment_environment", "'test'"},
				{"resource", `'{"service.name":"checkout","counter":9007199254740993}'`},
				{"attributes", `'{"http.route":"/checkout","nested":{"counter":9007199254740993}}'`},
				{"events", fmt.Sprintf(`'[{"name":"exception","timestamp":%d,"attributes":{"exception.message":"boom"}}]'`, startUS+200_000)},
				{"links", `'[{"trace_id":"` + strings.Repeat("d", 32) + `","span_id":"1112131415161718","trace_state":"","attributes":{}}]'`},
				{"ingest_time", strconv.FormatInt(startUS/1_000+1, 10)},
			}
			if len(columns) != len(want) {
				t.Fatalf("CREATE TABLE has %d columns, want %d", len(columns), len(want))
			}
			for i, pair := range want {
				if columns[i] != pair.column || strings.TrimSpace(listed[i]) != pair.column || values[i] != pair.value {
					t.Errorf("column %d: CREATE %q, INSERT %q = %s; want %s = %s",
						i, columns[i], strings.TrimSpace(listed[i]), values[i], pair.column, pair.value)
				}
			}
		})
	}
}
