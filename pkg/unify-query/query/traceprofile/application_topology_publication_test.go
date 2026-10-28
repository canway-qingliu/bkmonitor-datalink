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
)

const observedApplicationTopologyTargetJSON = `[{"label":"application_edge","observation_domain_id":"system_kapm",
"database":"system_kapm","table":"kapm_application_edge_observed_1m",
"cluster_id":18,"cluster_name":"doris4_test","schema_version":"sha256:19617e34c53d393e","retention_days":30}]`

func TestPublishedApplicationTopologyEdgeIsTenantBoundAndVersioned(t *testing.T) {
	body := strings.Replace(business132Document, `"application_topology_targets":[]`,
		`"application_topology_targets":`+observedApplicationTopologyTargetJSON, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || len(document.ApplicationTopologyTargets) != 1 {
		t.Fatalf("observed application edge target rejected: %+v, %v", document.ApplicationTopologyTargets, err)
	}
	snapshot, err := NewRouteSnapshotWithAllTargets(9, document.Routes, document.ESSummaryTargets, document.ApplicationTopologyTargets)
	if err != nil {
		t.Fatal(err)
	}
	if target, err := snapshot.ResolveApplicationTopologyTarget("system", "application_edge"); err != nil || target.PublishedVersion != document.Version {
		t.Fatalf("published edge route mismatch: %+v, %v", target, err)
	}
	if _, err := snapshot.ResolveApplicationTopologyTarget("other", "application_edge"); err == nil {
		t.Fatal("edge target leaked to another tenant")
	}
}

func TestPublishedApplicationTopologyRejectsAdditionalOrWrongTarget(t *testing.T) {
	for _, targets := range []string{
		strings.TrimSuffix(observedApplicationTopologyTargetJSON, "]") + `,{"label":"application_coverage"}]`,
		strings.Replace(observedApplicationTopologyTargetJSON, "kapm_application_edge_observed_1m", "otel_trace_summary", 1),
		strings.Replace(observedApplicationTopologyTargetJSON, `"cluster_id":18`, `"cluster_id":0`, 1),
	} {
		body := strings.Replace(business132Document, `"application_topology_targets":[]`,
			`"application_topology_targets":`+targets, 1)
		if _, err := ParsePublishedDocument([]byte(body), "system"); err == nil {
			t.Fatal("extra or physically drifted application topology target was accepted")
		}
	}
}

func TestApplicationTopologyRouteSnapshotAcceptsMixedTenantEdges(t *testing.T) {
	edge := ApplicationTopologyTarget{BkTenantID: "tenant_a", Label: "application_edge", Database: "tenant_a_kapm",
		Table: "kapm_application_edge_observed_1m", ClusterID: "7", ClusterName: "doris",
		SchemaVersion: "sha256:19617e34c53d393e", RetentionDays: 30, PublishedVersion: 4}
	other := edge
	other.BkTenantID = "tenant_b"
	other.Database = "tenant_b_kapm"
	other.PublishedVersion = 5
	if _, err := NewRouteSnapshotWithAllTargets(9, nil, nil, []ApplicationTopologyTarget{edge, other}); err != nil {
		t.Fatalf("mixed tenant edge routes rejected: %v", err)
	}
}

func TestPublishedApplicationTopologyAcceptsEmptyTargets(t *testing.T) {
	document, err := ParsePublishedDocument([]byte(business132Document), "system")
	if err != nil || len(document.ApplicationTopologyTargets) != 0 {
		t.Fatalf("empty application topology target rejected: %+v, %v", document.ApplicationTopologyTargets, err)
	}
}
