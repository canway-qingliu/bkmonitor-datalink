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
	"strconv"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

const application7Document = `{"route_snapshot_version":7,"bk_tenant_id":"system","read_routes":[` +
	`{"bk_tenant_id":"system","bk_biz_id":132,"app_id":7,"profile":"trace_span",` +
	`"database":"system_kapm","table":"cw_trace_span_app_7","cluster_id":18,"cluster_name":"doris4_test",` +
	`"schema_version":"sha256:0bf5aac6160a636e"}]}`

func TestPublishedDocumentRejectsBusinessRoutes(t *testing.T) {
	legacy := strings.Replace(application7Document, `"app_id":7,`, "", 1)
	legacy = strings.Replace(legacy, "cw_trace_span_app_7", "cw_trace_span_132", 1)
	for name, body := range map[string]string{
		"missing app":        legacy,
		"zero app":           strings.Replace(legacy, `"profile":`, `"app_id":0,"profile":`, 1),
		"null app":           strings.Replace(legacy, `"profile":`, `"app_id":null,"profile":`, 1),
		"old table with app": strings.Replace(legacy, `"profile":`, `"app_id":7,"profile":`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParsePublishedDocument([]byte(body), "system"); err == nil {
				t.Fatal("retired business route was accepted")
			}
		})
	}
}

func TestPublishedApplicationRoutesKeepStrictDecoding(t *testing.T) {
	cases := map[string]string{
		"missing app":         strings.Replace(application7Document, `"app_id":7,`, "", 1),
		"unknown route field": strings.Replace(application7Document, `"app_id":7`, `"app_id":7,"physical_alias":"cw_trace_span"`, 1),
		"duplicate app key":   strings.Replace(application7Document, `"app_id":7`, `"app_id":0,"app_id":7`, 1),
		"trailing document":   application7Document + `{}`,
	}
	for _, raw := range []string{"0", "-1", "null", "7.5", `"7"`, "true", "9223372036854775808"} {
		cases["invalid app "+raw] = strings.Replace(application7Document, `"app_id":7`, `"app_id":`+raw, 1)
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParsePublishedDocument([]byte(body), "system"); err == nil {
				t.Fatal("invalid application publication passed strict decoding")
			}
		})
	}
}

func TestPublishedApplicationRouteAcceptsPairedSourceIdentity(t *testing.T) {
	for _, dataID := range []int64{901, 9007199254740993, 9223372036854775807} {
		t.Run(strconv.FormatInt(dataID, 10), func(t *testing.T) {
			body := strings.Replace(application7Document, `"profile":`,
				`"result_table_id":"system.apm_trace_span","data_id":`+strconv.FormatInt(dataID, 10)+`,"profile":`, 1)
			document, err := ParsePublishedDocument([]byte(body), "system")
			if err != nil || len(document.Routes) != 1 {
				t.Fatalf("paired source identity was rejected: routes=%+v err=%v", document.Routes, err)
			}
			route := document.Routes[0]
			if route.ResultTableID != "system.apm_trace_span" || route.DataID != dataID {
				t.Fatalf("source identity was lost or rounded: %+v", route)
			}
			if route.Doris.Table != "cw_trace_span_app_7" || route.AppID != 7 {
				t.Fatalf("source identity changed the trusted application target: %+v", route)
			}
			compiled, err := CompileList(route.Profile, *route.Doris,
				Scope{BkTenantID: "system", BkBizID: 132, SpaceUID: "bkcc__132"}, namingRequest(7), Limits{})
			if err != nil || !strings.Contains(compiled.SnapshotStatement, "FROM `system_kapm`.`cw_trace_span_app_7`") ||
				strings.Contains(compiled.SnapshotStatement, route.ResultTableID) {
				t.Fatalf("source metadata changed physical execution: sql=%s err=%v", compiled.SnapshotStatement, err)
			}
		})
	}
}

func TestPublishedApplicationRouteRejectsIncompleteSourceIdentity(t *testing.T) {
	for name, fields := range map[string]string{
		"RT without DataID": `"result_table_id":"system.apm_trace_span",`,
		"DataID without RT": `"data_id":901,`,
		"empty pair":        `"result_table_id":"","data_id":0,`,
		"blank RT":          `"result_table_id":" ","data_id":901,`,
		"null RT":           `"result_table_id":null,"data_id":901,`,
		"numeric RT":        `"result_table_id":901,"data_id":901,`,
		"zero DataID":       `"result_table_id":"system.apm_trace_span","data_id":0,`,
		"negative DataID":   `"result_table_id":"system.apm_trace_span","data_id":-1,`,
		"null DataID":       `"result_table_id":"system.apm_trace_span","data_id":null,`,
		"fractional DataID": `"result_table_id":"system.apm_trace_span","data_id":901.5,`,
		"string DataID":     `"result_table_id":"system.apm_trace_span","data_id":"901",`,
		"boolean DataID":    `"result_table_id":"system.apm_trace_span","data_id":true,`,
		"overflow DataID":   `"result_table_id":"system.apm_trace_span","data_id":9223372036854775808,`,
		"null pair":         `"result_table_id":null,"data_id":null,`,
	} {
		t.Run(name, func(t *testing.T) {
			body := strings.Replace(application7Document, `"profile":`, fields+`"profile":`, 1)
			if _, err := ParsePublishedDocument([]byte(body), "system"); err == nil {
				t.Fatal("incomplete or invalid source identity was accepted")
			}
		})
	}
}

func TestRetiredPublishedSegmentsAreToleratedAndIgnored(t *testing.T) {
	body := strings.Replace(application7Document, `]}`,
		`],"a3_targets":[{"label":"a3_edge","database":"system_kapm",`+
			`"table":"kapm_application_edge_observed_1m","cluster_id":18,"cluster_name":"doris4_test",`+
			`"schema_version":"sha256:8e56c7a2af5bd73b","retention_days":30}],`+
			`"application_topology_targets":[],"es_summary_targets":[{"bk_tenant_id":"system",`+
			`"bk_biz_id":132,"app_id":7,"app_name":"checkout",`+
			`"read_result_table_id":"system.trace_summary_es_v2","storage_cluster_id":9,"retention_days":30}]}`, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || len(document.Routes) != 1 || document.Routes[0].Profile != metadata.TraceProfileSpan {
		t.Fatalf("retired publication segments blocked the Span route: routes=%+v err=%v", document.Routes, err)
	}
}

func TestRetiredSummaryProfileRouteIsRejected(t *testing.T) {
	body := strings.Replace(application7Document, `"profile":"trace_span"`, `"profile":"trace_summary"`, 1)
	if _, err := ParsePublishedDocument([]byte(body), "system"); err == nil {
		t.Fatal("retired Summary profile route was accepted")
	}
}

func TestPublishedApplicationSpanRoutesSelectExactTargets(t *testing.T) {
	extra := `,{"bk_tenant_id":"system","bk_biz_id":132,"app_id":8,"profile":"trace_span",` +
		`"database":"tenant_bbc5e661e106c6dc_kapm",` +
		`"table":"cw_trace_span","cluster_id":18,"cluster_name":"doris4_test",` +
		`"schema_version":"sha256:0bf5aac6160a636e"}`
	body := strings.Replace(application7Document, `]}`, extra+`]}`, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || len(document.Routes) != 2 {
		t.Fatalf("application routes rejected: %+v %v", document.Routes, err)
	}
	snapshot, err := NewRouteSnapshot(7, document.Routes)
	if err != nil {
		t.Fatal(err)
	}
	for appID, table := range map[int64]string{7: "cw_trace_span_app_7", 8: "cw_trace_span"} {
		route, resolveErr := snapshot.ResolveForApplication("system", 132, appID, metadata.TraceProfileSpan)
		if resolveErr != nil || route.Doris.Table != table {
			t.Fatalf("app %d route=%+v err=%v", appID, route, resolveErr)
		}
	}
	if _, err := snapshot.ResolveForApplication("system", 132, 9, metadata.TraceProfileSpan); ErrorCode(err) != CodeRouteNotFound {
		t.Fatalf("missing application reused another target: %v", err)
	}
	if _, err := ParsePublishedDocument([]byte(strings.Replace(body, `"cw_trace_span_app_7"`, `"cw_trace_span_app_9"`, 1)), "system"); err == nil {
		t.Fatal("application route accepted another application's table")
	}
}

func TestPublishedApplicationSpanRouteAcceptsHyphenTenantWithHashedDatabase(t *testing.T) {
	const document = `{"route_snapshot_version":7,"bk_tenant_id":"tenant-a","read_routes":[` +
		`{"bk_tenant_id":"tenant-a","bk_biz_id":132,"app_id":7,"profile":"trace_span",` +
		`` +
		`"database":"tenant_80a707af7dc77ee1_kapm","table":"cw_trace_span",` +
		`"cluster_id":18,"cluster_name":"doris4_test","schema_version":"sha256:0bf5aac6160a636e"}]}`
	parsed, err := ParsePublishedDocument([]byte(document), "tenant-a")
	if err != nil || len(parsed.Routes) != 1 || parsed.Routes[0].AppID != 7 {
		t.Fatalf("valid hyphen tenant route rejected: %+v %v", parsed.Routes, err)
	}
}

func TestParsePublishedDocumentResolvesApplication7Table(t *testing.T) {
	document, err := ParsePublishedDocument([]byte(application7Document), "system")
	if err != nil {
		t.Fatalf("published routes rejected: %v", err)
	}
	if document.Version != 7 || document.TenantID != "system" || len(document.Routes) != 1 {
		t.Fatalf("published document lost identity or routes: %+v", document)
	}
	snapshot, err := NewRouteSnapshot(document.Version, document.Routes)
	if err != nil {
		t.Fatalf("published routes did not build a snapshot: %v", err)
	}
	span, err := snapshot.ResolveForApplication("system", 132, 7, metadata.TraceProfileSpan)
	if err != nil || span.Doris.Table != "cw_trace_span_app_7" || span.Doris.ClusterID != "18" {
		t.Fatalf("application 7 span target was lost: %+v, %v", span, err)
	}
	if _, err := snapshot.ResolveForApplication("system", 133, 7, metadata.TraceProfileSpan); ErrorCode(err) != CodeRouteNotFound {
		t.Fatalf("business 133 crossed into business 132: %v", err)
	}
}

func TestParsePublishedDocumentRejectsTenantAndPhysicalDrift(t *testing.T) {
	for name, body := range map[string]string{
		"wrong key tenant": application7Document,
		"wrong table":      strings.Replace(application7Document, "cw_trace_span_app_7", "cw_trace_span_app_8", 1),
		"wrong schema":     strings.Replace(application7Document, "sha256:0bf5aac6160a636e", "sha256:0123456789abcdef", 1),
		"unknown profile":  strings.Replace(application7Document, `"profile":"trace_span"`, `"profile":"unknown"`, 1),
	} {
		tenant := "system"
		if name == "wrong key tenant" {
			tenant = "other"
		}
		if _, err := ParsePublishedDocument([]byte(body), tenant); err == nil {
			t.Fatalf("%s: invalid publisher document was accepted", name)
		}
	}
}
