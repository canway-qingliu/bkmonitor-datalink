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
	`"schema_version":"sha256:281052267ec373fc"}],"a3_targets":[]}`

const publishedESSummaryTargets = `[{"bk_tenant_id":"system","bk_biz_id":132,"app_id":7,` +
	`"app_name":"checkout","observation_domain_id":"system_kapm",` +
	`"read_result_table_id":"apm_global.trace_summary_v1_7","storage_cluster_id":9,"retention_days":30}]`

func TestPublishedESSummaryTargetsAreVersionedAndScoped(t *testing.T) {
	body := strings.Replace(business132Document, `"a3_targets":[]`,
		`"a3_targets":[],"es_summary_targets":`+publishedESSummaryTargets, 1)
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
			body := strings.Replace(business132Document, `"a3_targets":[]`,
				`"a3_targets":[],"es_summary_targets":`+target, 1)
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
