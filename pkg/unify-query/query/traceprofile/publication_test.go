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
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

const business132Document = `{"route_snapshot_version":7,"bk_tenant_id":"system","read_routes":[` +
	`{"bk_tenant_id":"system","bk_biz_id":132,"profile":"trace_span",` +
	`"database":"system_kapm","table":"otel_trace_span_132","cluster_id":18,"cluster_name":"doris4_test",` +
	`"schema_version":"sha256:3fee31e7c6376abe"}]}`

func TestRetiredPublishedSegmentsAreToleratedAndIgnored(t *testing.T) {
	body := strings.Replace(business132Document, `]}`,
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
	body := strings.Replace(business132Document, `"profile":"trace_span"`, `"profile":"trace_summary"`, 1)
	if _, err := ParsePublishedDocument([]byte(body), "system"); err == nil {
		t.Fatal("retired Summary profile route was accepted")
	}
}

func TestPublishedApplicationSpanRoutesCoexistWithLegacyBusinessRoute(t *testing.T) {
	extra := `,{"bk_tenant_id":"system","bk_biz_id":132,"app_id":7,"profile":"trace_span",` +
		`"database":"tenant_bbc5e661e106c6dc_kapm",` +
		`"table":"otel_trace_span","cluster_id":18,"cluster_name":"doris4_test",` +
		`"schema_version":"sha256:3fee31e7c6376abe"},` +
		`{"bk_tenant_id":"system","bk_biz_id":132,"app_id":8,"profile":"trace_span",` +
		`"database":"tenant_bbc5e661e106c6dc_kapm",` +
		`"table":"otel_trace_span_app_8","cluster_id":18,"cluster_name":"doris4_test",` +
		`"schema_version":"sha256:3fee31e7c6376abe"}`
	body := strings.Replace(business132Document, `]}`, extra+`]}`, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || len(document.Routes) != 3 {
		t.Fatalf("mixed routes rejected: %+v %v", document.Routes, err)
	}
	snapshot, err := NewRouteSnapshot(7, document.Routes)
	if err != nil {
		t.Fatal(err)
	}
	for appID, table := range map[int64]string{7: "otel_trace_span", 8: "otel_trace_span_app_8"} {
		route, resolveErr := snapshot.ResolveForApplication("system", 132, appID, metadata.TraceProfileSpan)
		if resolveErr != nil || route.Doris.Table != table {
			t.Fatalf("app %d route=%+v err=%v", appID, route, resolveErr)
		}
	}
	if _, err := ParsePublishedDocument([]byte(strings.Replace(body, `"otel_trace_span_app_8"`, `"otel_trace_span_app_9"`, 1)), "system"); err == nil {
		t.Fatal("application route accepted another application's table")
	}
}

func TestPublishedApplicationSpanRouteAcceptsHyphenTenantWithHashedDatabase(t *testing.T) {
	const document = `{"route_snapshot_version":7,"bk_tenant_id":"tenant-a","read_routes":[` +
		`{"bk_tenant_id":"tenant-a","bk_biz_id":132,"app_id":7,"profile":"trace_span",` +
		`` +
		`"database":"tenant_80a707af7dc77ee1_kapm","table":"otel_trace_span",` +
		`"cluster_id":18,"cluster_name":"doris4_test","schema_version":"sha256:3fee31e7c6376abe"}]}`
	parsed, err := ParsePublishedDocument([]byte(document), "tenant-a")
	if err != nil || len(parsed.Routes) != 1 || parsed.Routes[0].AppID != 7 {
		t.Fatalf("valid hyphen tenant route rejected: %+v %v", parsed.Routes, err)
	}
}

func TestParsePublishedDocumentResolvesBusiness132Tables(t *testing.T) {
	document, err := ParsePublishedDocument([]byte(business132Document), "system")
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
	span, err := snapshot.Resolve("system", 132, metadata.TraceProfileSpan)
	if err != nil || span.Doris.Table != "otel_trace_span_132" || span.Doris.ClusterID != "18" {
		t.Fatalf("business 132 span target was lost: %+v, %v", span, err)
	}
	if _, err := snapshot.Resolve("system", 133, metadata.TraceProfileSpan); ErrorCode(err) != CodeRouteNotFound {
		t.Fatalf("business 133 crossed into business 132: %v", err)
	}
}

func TestParsePublishedDocumentRejectsTenantAndPhysicalDrift(t *testing.T) {
	for name, body := range map[string]string{
		"wrong key tenant": business132Document,
		"wrong table":      strings.Replace(business132Document, "otel_trace_span_132", "otel_trace_span_133", 1),
		"wrong schema":     strings.Replace(business132Document, "sha256:3fee31e7c6376abe", "sha256:0123456789abcdef", 1),
		"unknown profile":  strings.Replace(business132Document, `"profile":"trace_span"`, `"profile":"unknown"`, 1),
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
