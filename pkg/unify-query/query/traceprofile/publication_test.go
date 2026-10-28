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
	`{"bk_tenant_id":"system","bk_biz_id":132,"profile":"trace_span","observation_domain_id":"system_kapm",` +
	`"database":"system_kapm","table":"otel_trace_span_132","cluster_id":18,"cluster_name":"doris4_test",` +
	`"schema_version":"sha256:e6e9af09d86b396e"},` +
	`{"bk_tenant_id":"system","bk_biz_id":132,"profile":"trace_summary","observation_domain_id":"system_kapm",` +
	`"database":"system_kapm","table":"otel_trace_summary_132","cluster_id":18,"cluster_name":"doris4_test",` +
	`"schema_version":"sha256:281052267ec373fc"}],"application_topology_targets":[]}`

const publishedESSummaryTargets = `[{"bk_tenant_id":"system","bk_biz_id":132,"app_id":7,` +
	`"app_name":"checkout","observation_domain_id":"system_kapm",` +
	`"read_result_table_id":"apm_global.trace_summary_v1_7","storage_cluster_id":9,"retention_days":30}]`

func TestLegacyPublishedTopologyFieldKeepsSpanAndSummaryReadable(t *testing.T) {
	legacy := strings.Replace(business132Document, `"application_topology_targets":[]`,
		`"a3_targets":[{"label":"a3_edge","observation_domain_id":"system_kapm",`+
			`"database":"system_kapm","table":"kapm_application_edge_observed_1m",`+
			`"cluster_id":18,"cluster_name":"doris4_test",`+
			`"schema_version":"sha256:19617e34c53d393e","retention_days":30}]`, 1)
	parsed, err := ParsePublishedDocument([]byte(legacy), "system")
	if err != nil || len(parsed.Routes) != 2 || len(parsed.ApplicationTopologyTargets) != 0 {
		t.Fatalf("legacy publication blocked ordinary Trace routes: routes=%d topology=%d err=%v",
			len(parsed.Routes), len(parsed.ApplicationTopologyTargets), err)
	}
}

func TestPublishedApplicationSpanRoutesCoexistWithLegacyBusinessRoute(t *testing.T) {
	extra := `,{"bk_tenant_id":"system","bk_biz_id":132,"app_id":7,"profile":"trace_span",` +
		`"observation_domain_id":"tenant_bbc5e661e106c6dc_kapm","database":"tenant_bbc5e661e106c6dc_kapm",` +
		`"table":"otel_trace_span","cluster_id":18,"cluster_name":"doris4_test",` +
		`"schema_version":"sha256:e6e9af09d86b396e"},` +
		`{"bk_tenant_id":"system","bk_biz_id":132,"app_id":8,"profile":"trace_span",` +
		`"observation_domain_id":"tenant_bbc5e661e106c6dc_kapm","database":"tenant_bbc5e661e106c6dc_kapm",` +
		`"table":"otel_trace_span_app_8","cluster_id":18,"cluster_name":"doris4_test",` +
		`"schema_version":"sha256:e6e9af09d86b396e"}`
	body := strings.Replace(business132Document, `],"application_topology_targets":[]`, extra+`],"application_topology_targets":[]`, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || len(document.Routes) != 4 {
		t.Fatalf("mixed routes rejected: %+v %v", document.Routes, err)
	}
	snapshot, err := NewRouteSnapshot(7, document.Routes)
	if err != nil {
		t.Fatal(err)
	}
	for appID, table := range map[int64]string{7: "otel_trace_span", 8: "otel_trace_span_app_8"} {
		route, resolveErr := snapshot.ResolveForApplication("system", 132, appID, metadata.TraceProfileSpan, "tenant_bbc5e661e106c6dc_kapm")
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
		`"observation_domain_id":"tenant_80a707af7dc77ee1_kapm",` +
		`"database":"tenant_80a707af7dc77ee1_kapm","table":"otel_trace_span",` +
		`"cluster_id":18,"cluster_name":"doris4_test","schema_version":"sha256:e6e9af09d86b396e"}],` +
		`"application_topology_targets":[]}`
	parsed, err := ParsePublishedDocument([]byte(document), "tenant-a")
	if err != nil || len(parsed.Routes) != 1 || parsed.Routes[0].AppID != 7 {
		t.Fatalf("valid hyphen tenant route rejected: %+v %v", parsed.Routes, err)
	}
}

func TestPublishedTenantSummaryRouteKeepsBusinessScopeInSharedTable(t *testing.T) {
	body := strings.Replace(business132Document, `"table":"otel_trace_summary_132"`,
		`"table":"otel_trace_summary"`, 1)
	body = strings.Replace(body, `"schema_version":"sha256:281052267ec373fc"`,
		`"schema_version":"`+tenantSummarySchemaVersion+`"`, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || len(document.Routes) != 2 || document.Routes[1].Doris.Table != "otel_trace_summary" {
		t.Fatalf("tenant Summary route rejected: routes=%+v err=%v", document.Routes, err)
	}
	scope, err := ResolveScope("system", "bkcc__132", "system_kapm")
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Profile: metadata.TraceProfileSummary, StartUS: 1721024820000000,
		EndUS: 1721024880000000, Limit: 10}
	compiled, err := CompileList(metadata.TraceProfileSummary, *document.Routes[1].Doris, scope, request, Limits{})
	if err != nil || !strings.Contains(compiled.SnapshotStatement, "`system_kapm`.`otel_trace_summary`") ||
		!strings.Contains(compiled.SnapshotStatement, "`bk_biz_id` = ?") {
		t.Fatalf("tenant Summary SQL lost physical table or trusted biz: %+v err=%v", compiled, err)
	}
	for _, invalid := range []string{
		strings.Replace(body, `"table":"otel_trace_summary"`, `"table":"otel_trace_summary_132"`, 1),
		strings.Replace(body, tenantSummarySchemaVersion, "sha256:281052267ec373fc", 1),
	} {
		if _, err := ParsePublishedDocument([]byte(invalid), "system"); err == nil {
			t.Fatal("tenant Summary layout accepted wrong table or schema version")
		}
	}
}

func TestPublishedESSummaryTargetsAreVersionedAndScoped(t *testing.T) {
	body := strings.Replace(business132Document, `"application_topology_targets":[]`,
		`"application_topology_targets":[],"es_summary_targets":`+publishedESSummaryTargets, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || len(document.ESSummaryTargets) != 1 {
		t.Fatalf("trusted ES Summary target was lost: targets=%+v err=%v", document.ESSummaryTargets, err)
	}
	target := document.ESSummaryTargets[0]
	if target.AppID != 7 || target.ResultTableID != "apm_global.trace_summary_v1_7" || target.PublishedVersion != 7 {
		t.Fatalf("ES Summary identity or version drifted: %+v", target)
	}
	snapshot, err := NewRouteSnapshotWithESTargets(document.Version, document.Routes, document.ESSummaryTargets)
	if err != nil {
		t.Fatalf("trusted ES target did not enter snapshot: %v", err)
	}
	resolved, err := snapshot.ResolveESSummary("system", 132, 7, "system_kapm")
	if err != nil || resolved.ResultTableID != target.ResultTableID {
		t.Fatalf("ES target did not resolve by paired identity: %+v err=%v", resolved, err)
	}
	if _, err := snapshot.ResolveESSummary("system", 132, 8, "system_kapm"); ErrorCode(err) != CodeRouteNotFound {
		t.Fatalf("another application gained ES target access: %v", err)
	}
}

func TestPublishedESSummaryEntryV2RequiresThirtyDayTarget(t *testing.T) {
	v2 := strings.Replace(publishedESSummaryTargets, `"retention_days":30`,
		`"retention_days":30,"entry_index_version":2`, 1)
	body := strings.Replace(business132Document, `"application_topology_targets":[]`,
		`"application_topology_targets":[],"es_summary_targets":`+v2, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || document.ESSummaryTargets[0].EntryIndexVersion != 2 {
		t.Fatalf("published entry v2 capability missing: %+v err=%v", document.ESSummaryTargets, err)
	}
	for _, invalid := range []string{
		strings.Replace(body, `"retention_days":30`, `"retention_days":7`, 1),
		strings.Replace(body, `"entry_index_version":2`, `"entry_index_version":3`, 1),
	} {
		if _, err := ParsePublishedDocument([]byte(invalid), "system"); err == nil {
			t.Fatal("invalid entry index version or retention was published")
		}
	}
}

func TestPublishedESSummaryTargetsRejectIdentityAndMetadataDrift(t *testing.T) {
	for name, target := range map[string]string{
		"wrong tenant":    strings.Replace(publishedESSummaryTargets, `"bk_tenant_id":"system"`, `"bk_tenant_id":"other"`, 1),
		"wrong domain":    strings.Replace(publishedESSummaryTargets, `"system_kapm"`, `"other_kapm"`, 1),
		"bad rt":          strings.Replace(publishedESSummaryTargets, `"apm_global.trace_summary_v1_7"`, `"trace_summary_v1_7"`, 1),
		"bad cluster":     strings.Replace(publishedESSummaryTargets, `"storage_cluster_id":9`, `"storage_cluster_id":0`, 1),
		"short retention": strings.Replace(publishedESSummaryTargets, `"retention_days":30`, `"retention_days":0`, 1),
		"duplicate":       publishedESSummaryTargets[:len(publishedESSummaryTargets)-1] + `,` + publishedESSummaryTargets[1:],
	} {
		t.Run(name, func(t *testing.T) {
			body := strings.Replace(business132Document, `"application_topology_targets":[]`,
				`"application_topology_targets":[],"es_summary_targets":`+target, 1)
			if _, err := ParsePublishedDocument([]byte(body), "system"); err == nil {
				t.Fatal("ES Summary target drift was accepted")
			}
		})
	}
}

func TestParsePublishedDocumentResolvesBusiness132Tables(t *testing.T) {
	document, err := ParsePublishedDocument([]byte(business132Document), "system")
	if err != nil {
		t.Fatalf("published routes rejected: %v", err)
	}
	if document.Version != 7 || document.TenantID != "system" || len(document.Routes) != 2 {
		t.Fatalf("published document lost identity or routes: %+v", document)
	}
	snapshot, err := NewRouteSnapshot(document.Version, document.Routes)
	if err != nil {
		t.Fatalf("published routes did not build a snapshot: %v", err)
	}
	span, err := snapshot.Resolve("system", 132, metadata.TraceProfileSpan, "system_kapm")
	if err != nil || span.Doris.Table != "otel_trace_span_132" || span.Doris.ClusterID != "18" {
		t.Fatalf("business 132 span target was lost: %+v, %v", span, err)
	}
	if _, err := snapshot.Resolve("system", 133, metadata.TraceProfileSpan, "system_kapm"); ErrorCode(err) != CodeRouteNotFound {
		t.Fatalf("business 133 crossed into business 132: %v", err)
	}
}

func TestParsePublishedDocumentRejectsTenantAndPhysicalDrift(t *testing.T) {
	for name, body := range map[string]string{
		"wrong key tenant": business132Document,
		"wrong table":      strings.Replace(business132Document, "otel_trace_span_132", "otel_trace_span_133", 1),
		"wrong schema":     strings.Replace(business132Document, "sha256:e6e9af09d86b396e", "sha256:0123456789abcdef", 1),
		"missing summary":  strings.Replace(business132Document, `,"profile":"trace_summary"`, `,"profile":"unknown"`, 1),
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
