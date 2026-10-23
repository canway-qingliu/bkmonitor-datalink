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

const legacyA3TargetsJSON = `[{"label":"a3_edge","observation_domain_id":"system_kapm",
"database":"system_kapm","table":"kapm_application_edge_observed_1m",
"cluster_id":18,"cluster_name":"doris4_test","schema_version":"sha256:19617e34c53d393e","retention_days":30},
{"label":"a3_coverage","observation_domain_id":"system_kapm",
"database":"system_kapm","table":"kapm_application_edge_coverage_1m",
"cluster_id":18,"cluster_name":"doris4_test","schema_version":"sha256:06c86fab2f251999","retention_days":30}]`

var panoramaApplicationTopologyTargets = strings.NewReplacer(
	`"a3_edge"`, `"application_edge"`, `"a3_coverage"`, `"application_coverage"`,
).Replace(legacyA3TargetsJSON)

func TestPublishedLegacyA3TargetPairIsTenantBoundAndVersioned(t *testing.T) {
	body := strings.Replace(business132Document, `"a3_targets":[]`, `"a3_targets":`+legacyA3TargetsJSON, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || len(document.ApplicationTopologyTargets) != 2 {
		t.Fatalf("A3 targets lost: %v", err)
	}
	snapshot, err := NewRouteSnapshotWithAllTargets(9, document.Routes, document.ESSummaryTargets, document.ApplicationTopologyTargets)
	if err != nil {
		t.Fatal(err)
	}
	if target, err := snapshot.ResolveApplicationTopologyTarget("system", "a3_edge"); err != nil || target.PublishedVersion != document.Version {
		t.Fatalf("A3 published route mismatch: %+v %v", target, err)
	}
	if _, err := snapshot.ResolveApplicationTopologyTarget("other", "a3_edge"); err == nil {
		t.Fatal("A3 target leaked to another tenant")
	}
}

func TestPublishedLegacyA3RejectsPartialOrWrongPhysicalTarget(t *testing.T) {
	for _, targets := range []string{
		strings.Split(legacyA3TargetsJSON, ",\n{")[0] + "]",
		strings.Replace(legacyA3TargetsJSON, "kapm_application_edge_coverage_1m", "otel_trace_summary", 1),
		strings.Replace(legacyA3TargetsJSON, `"cluster_id":18`, `"cluster_id":19`, 1),
	} {
		body := strings.Replace(business132Document, `"a3_targets":[]`, `"a3_targets":`+targets, 1)
		if _, err := ParsePublishedDocument([]byte(body), "system"); err == nil {
			t.Fatal("partial or physically drifted A3 publication was accepted")
		}
	}
}

func TestPublishedApplicationTopologyTargetsAcceptNewOnlyAndMatchingDualPublication(t *testing.T) {
	for _, replacement := range []string{
		`"application_topology_targets":` + panoramaApplicationTopologyTargets,
		`"a3_targets":` + legacyA3TargetsJSON + `,"application_topology_targets":` + panoramaApplicationTopologyTargets,
	} {
		body := strings.Replace(business132Document, `"a3_targets":[]`, replacement, 1)
		document, err := ParsePublishedDocument([]byte(body), "system")
		if err != nil || len(document.ApplicationTopologyTargets) != 2 {
			t.Fatalf("application topology targets rejected: %v", err)
		}
		snapshot, err := NewRouteSnapshotWithAllTargets(9, document.Routes, document.ESSummaryTargets, document.ApplicationTopologyTargets)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := snapshot.ResolveApplicationTopologyTarget("system", "application_edge"); err != nil {
			t.Fatalf("descriptive edge target was not resolved: %v", err)
		}
	}
}

func TestPublishedApplicationTopologyAcceptsEdgeOnly(t *testing.T) {
	targets := strings.Split(panoramaApplicationTopologyTargets, ",\n{")[0] + "]"
	for _, replacement := range []string{`"application_topology_targets":` + targets,
		`"a3_targets":[],"application_topology_targets":` + targets} {
		body := strings.Replace(business132Document, `"a3_targets":[]`, replacement, 1)
		document, err := ParsePublishedDocument([]byte(body), "system")
		if err != nil || len(document.ApplicationTopologyTargets) != 1 {
			t.Fatalf("edge-only topology publication rejected: %+v, %v", document.ApplicationTopologyTargets, err)
		}
	}
}

func TestPublishedApplicationTopologyKeepsLegacyCoverageForOldRequests(t *testing.T) {
	edge := strings.Split(panoramaApplicationTopologyTargets, ",\n{")[0] + "]"
	body := strings.Replace(business132Document, `"a3_targets":[]`,
		`"a3_targets":`+legacyA3TargetsJSON+`,"application_topology_targets":`+edge, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || len(document.ApplicationTopologyTargets) != 2 {
		t.Fatalf("legacy coverage target lost: %+v, %v", document.ApplicationTopologyTargets, err)
	}
}

func TestApplicationTopologyRouteSnapshotAcceptsMixedTenantTargets(t *testing.T) {
	edge := ApplicationTopologyTarget{BkTenantID: "tenant_a", Label: "application_edge", Database: "tenant_a_kapm",
		Table: "kapm_application_edge_observed_1m", ClusterID: "7", ClusterName: "doris",
		SchemaVersion: "sha256:19617e34c53d393e", RetentionDays: 30, PublishedVersion: 4}
	other := edge
	other.BkTenantID = "tenant_b"
	other.Database = "tenant_b_kapm"
	other.PublishedVersion = 5
	legacyCoverage := ApplicationTopologyTarget{BkTenantID: "tenant_a", Label: "application_coverage", Database: "tenant_a_kapm",
		Table: "kapm_application_edge_coverage_1m", ClusterID: "7", ClusterName: "doris",
		SchemaVersion: "sha256:06c86fab2f251999", RetentionDays: 30, PublishedVersion: 4}
	for _, targets := range [][]ApplicationTopologyTarget{{edge, other}, {edge, legacyCoverage, other}} {
		if _, err := NewRouteSnapshotWithAllTargets(9, nil, nil, targets); err != nil {
			t.Fatalf("mixed tenant topology routes rejected: %v", err)
		}
	}
}

func TestPublishedApplicationTopologyTargetsRejectsDualPublicationDrift(t *testing.T) {
	changed := strings.Replace(panoramaApplicationTopologyTargets, `"cluster_id":18`, `"cluster_id":19`, 1)
	body := strings.Replace(business132Document, `"a3_targets":[]`,
		`"a3_targets":`+legacyA3TargetsJSON+`,"application_topology_targets":`+changed, 1)
	if _, err := ParsePublishedDocument([]byte(body), "system"); err == nil {
		t.Fatal("different old and new topology targets were accepted together")
	}
}

func TestPublishedApplicationTopologyTargetsAcceptsMatchingEmptySegments(t *testing.T) {
	body := strings.Replace(business132Document, `"a3_targets":[]`,
		`"a3_targets":[],"application_topology_targets":[]`, 1)
	document, err := ParsePublishedDocument([]byte(body), "system")
	if err != nil || len(document.ApplicationTopologyTargets) != 0 {
		t.Fatalf("empty compatibility segments rejected: %+v err=%v", document.ApplicationTopologyTargets, err)
	}
}
