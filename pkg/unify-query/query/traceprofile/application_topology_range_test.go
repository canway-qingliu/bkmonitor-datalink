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
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/dorisclient"
)

type applicationTopologyReader struct {
	queries         int
	badBiz          bool
	badWatermark    bool
	mixedResolution bool
	// pairs, when set, is how many distinct pairs the edge table holds; the
	// reader returns no more of them than the statement's LIMIT asks for.
	pairs int
}

var statementLimit = regexp.MustCompile(`LIMIT (\d+)$`)

// storedApplicationPairs are the distinct directed pairs between the two
// applications of applicationTopologyInput.
var storedApplicationPairs = [][4]int64{{11, 2, 12, 3}, {12, 3, 11, 2}}

func (reader *applicationTopologyReader) Query(_ context.Context, statement string, args ...any) (dorisclient.Result, error) {
	reader.queries++
	if err := dorisclient.ValidateStatement(statement); err != nil {
		return dorisclient.Result{}, err
	}
	if len(args) < 6 || args[0] != "tenant_a" || !strings.Contains(statement, "LIMIT") {
		return dorisclient.Result{}, nil
	}
	if strings.HasPrefix(statement, "SELECT source_namespace_id") {
		biz := int64(2)
		if reader.badBiz {
			biz = 3
		}
		return dorisclient.Result{Rows: []map[string]any{{
			"source_namespace_id": int64(11), "source_bk_biz_id": biz,
			"observed_minutes": int64(15), "complete_minutes": int64(15),
			"partial_minutes": int64(0), "pending_minutes": int64(0),
			"unresolved_peers": int64(0), "as_of_ms": int64(1760573700000),
		}}}, nil
	}
	beyond := int64(0)
	if reader.badWatermark {
		beyond = 1
	}
	maximum := "stable"
	if reader.mixedResolution {
		maximum = "unique_service_name"
	}
	if reader.pairs > 0 {
		match := statementLimit.FindStringSubmatch(statement)
		if match == nil {
			return dorisclient.Result{}, nil
		}
		limit, _ := strconv.Atoi(match[1])
		rows := []map[string]any{}
		for _, pair := range storedApplicationPairs[:reader.pairs] {
			if len(rows) == limit {
				break
			}
			rows = append(rows, map[string]any{
				"source_namespace_id": pair[0], "source_bk_biz_id": pair[1],
				"target_namespace_id": pair[2], "target_bk_biz_id": pair[3],
				"min_resolution": "stable", "max_resolution": "stable",
				"observed_minutes": int64(1), "as_of_ms": int64(1760573700000),
				"unobserved_count": int64(0), "beyond_watermark": int64(0),
			})
		}
		return dorisclient.Result{Rows: rows}, nil
	}
	return dorisclient.Result{Rows: []map[string]any{{
		"source_namespace_id": int64(11), "source_bk_biz_id": int64(2),
		"target_namespace_id": int64(12), "target_bk_biz_id": int64(3),
		"min_resolution": "stable", "max_resolution": maximum,
		"observed_minutes": int64(1), "as_of_ms": int64(1760573700000),
		"unobserved_count": int64(0), "beyond_watermark": beyond,
	}}}, nil
}

func applicationTopologyExecutor(t *testing.T, reader *applicationTopologyReader) *Executor {
	t.Helper()
	targets := []ApplicationTopologyTarget{
		{BkTenantID: "tenant_a", Label: "application_edge", Database: "tenant_a_kapm",
			Table: "kapm_application_edge_observed_1m", ClusterID: "7", ClusterName: "doris",
			SchemaVersion: "sha256:19617e34c53d393e", RetentionDays: 30, PublishedVersion: 4},
		{BkTenantID: "tenant_a", Label: "application_coverage", Database: "tenant_a_kapm",
			Table: "kapm_application_edge_coverage_1m", ClusterID: "7", ClusterName: "doris",
			SchemaVersion: "sha256:06c86fab2f251999", RetentionDays: 30, PublishedVersion: 4},
	}
	snapshot, err := NewRouteSnapshotWithAllTargets(7, nil, nil, targets)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutor(snapshot, map[string]Querier{"7": reader}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func applicationTopologyInput() ApplicationTopologyRangeRequest {
	return ApplicationTopologyRangeRequest{TenantID: "tenant_a", ExpectedVersion: 4,
		StartMS: 1760572800000, EndMS: 1760573700000, MaxPairs: 100,
		Applications: []ApplicationTopologyApplication{{NamespaceID: 11, BkBizID: 2}, {NamespaceID: 12, BkBizID: 3}}}
}

func TestApplicationTopologyReadsPublishedPairAndChecksRows(t *testing.T) {
	reader := &applicationTopologyReader{}
	result, err := applicationTopologyExecutor(t, reader).QueryApplicationTopologyRange(context.Background(), applicationTopologyInput())
	if err != nil {
		t.Fatal(err)
	}
	if reader.queries != 2 || result.ExpectedMinutes != 15 || result.Coverage[0].CompleteMinutes != 15 ||
		len(result.Edges) != 1 || result.Edges[0].TargetNamespaceID != 12 {
		t.Fatalf("unexpected bounded application topology read: %+v, queries=%d", result, reader.queries)
	}
}

func TestApplicationTopologyV2ReadsOnlyObservedEdges(t *testing.T) {
	reader := &applicationTopologyReader{}
	request := applicationTopologyInput()
	request.ContractVersion = "application_topology_range_v2"
	result, err := applicationTopologyExecutor(t, reader).QueryApplicationTopologyRange(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if reader.queries != 1 || len(result.Edges) != 1 || len(result.Coverage) != 0 {
		t.Fatalf("V2 must read edges only: %+v, queries=%d", result, reader.queries)
	}
}

func TestApplicationTopologyV2ReadsEdgeOnlyPublication(t *testing.T) {
	reader := &applicationTopologyReader{}
	target := ApplicationTopologyTarget{BkTenantID: "tenant_a", Label: "application_edge", Database: "tenant_a_kapm",
		Table: "kapm_application_edge_observed_1m", ClusterID: "7", ClusterName: "doris",
		SchemaVersion: "sha256:19617e34c53d393e", RetentionDays: 30, PublishedVersion: 4}
	snapshot, err := NewRouteSnapshotWithAllTargets(7, nil, nil, []ApplicationTopologyTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutor(snapshot, map[string]Querier{"7": reader}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	request := applicationTopologyInput()
	request.ContractVersion = "application_topology_range_v2"
	if _, err := executor.QueryApplicationTopologyRange(context.Background(), request); err != nil || reader.queries != 1 {
		t.Fatalf("V2 edge-only read failed: %v, queries=%d", err, reader.queries)
	}
	request.ContractVersion = "application_topology_range_v1"
	if _, err := executor.QueryApplicationTopologyRange(context.Background(), request); err == nil || reader.queries != 1 {
		t.Fatalf("V1 must fail closed without coverage: %v, queries=%d", err, reader.queries)
	}
}

// max_pairs is the most pairs a window may hold: a window with exactly that
// many is answered, one with more is refused.
func TestApplicationTopologyPairBudgetAdmitsExactlyMaxPairs(t *testing.T) {
	for _, version := range []string{"application_topology_range_v1", "application_topology_range_v2"} {
		for _, test := range []struct {
			name           string
			stored         int
			maxPairs       int64
			wantEdges      int
			wantOverBudget bool
		}{
			{name: "exactly max pairs is answered", stored: 2, maxPairs: 2, wantEdges: 2},
			{name: "fewer than max pairs is answered", stored: 1, maxPairs: 2, wantEdges: 1},
			{name: "one pair at a budget of one is answered", stored: 1, maxPairs: 1, wantEdges: 1},
			{name: "more than max pairs is refused", stored: 2, maxPairs: 1, wantOverBudget: true},
		} {
			t.Run(version+"/"+test.name, func(t *testing.T) {
				request := applicationTopologyInput()
				request.ContractVersion = version
				request.MaxPairs = test.maxPairs
				result, err := applicationTopologyExecutor(t, &applicationTopologyReader{pairs: test.stored}).
					QueryApplicationTopologyRange(context.Background(), request)
				if test.wantOverBudget {
					if err == nil || !strings.Contains(err.Error(), "应用对超过行预算") {
						t.Fatalf("%d pairs at max_pairs %d must be refused as over budget, got %+v, %v", test.stored, test.maxPairs, result, err)
					}
					return
				}
				if err != nil || len(result.Edges) != test.wantEdges {
					t.Fatalf("%d pairs at max_pairs %d must be answered with %d edges, got %+v, %v", test.stored, test.maxPairs, test.wantEdges, result, err)
				}
			})
		}
	}
}

func TestApplicationTopologyRejectsStaleVersionAndForeignRowsBeforePublishing(t *testing.T) {
	reader := &applicationTopologyReader{}
	executor := applicationTopologyExecutor(t, reader)
	request := applicationTopologyInput()
	request.ExpectedVersion = 3
	if _, err := executor.QueryApplicationTopologyRange(context.Background(), request); err == nil || reader.queries != 0 {
		t.Fatal("stale publication must fail before Doris I/O")
	}
	reader.badBiz = true
	if _, err := executor.QueryApplicationTopologyRange(context.Background(), applicationTopologyInput()); err == nil {
		t.Fatal("coverage business identity must be checked")
	}
	reader.badBiz = false
	reader.badWatermark = true
	if _, err := executor.QueryApplicationTopologyRange(context.Background(), applicationTopologyInput()); err == nil {
		t.Fatal("edge newer than complete coverage watermark must be rejected")
	}
}

func TestApplicationTopologyMixedResolutionUsesConservativePairLabel(t *testing.T) {
	reader := &applicationTopologyReader{mixedResolution: true}
	result, err := applicationTopologyExecutor(t, reader).QueryApplicationTopologyRange(context.Background(), applicationTopologyInput())
	if err != nil || result.Edges[0].Resolution != "unique_service_name" {
		t.Fatalf("mixed valid evidence must keep weaker resolution: %+v %v", result, err)
	}
}

func TestApplicationTopologyRejectsUnknownNamespaceWithZeroBiz(t *testing.T) {
	allowed := map[int64]int64{11: 2, 12: 3}
	coverage := map[string]any{
		"source_namespace_id": int64(99), "source_bk_biz_id": int64(0),
		"observed_minutes": int64(15), "complete_minutes": int64(15),
		"partial_minutes": int64(0), "pending_minutes": int64(0),
		"unresolved_peers": int64(0), "as_of_ms": int64(1760573700000),
	}
	if _, err := decodeApplicationTopologyCoverage(coverage, allowed, 15); err == nil {
		t.Fatal("zero business ID cannot make a foreign source look authorized")
	}
	edge := map[string]any{
		"source_namespace_id": int64(99), "source_bk_biz_id": int64(0),
		"target_namespace_id": int64(12), "target_bk_biz_id": int64(3),
		"observed_minutes": int64(1), "as_of_ms": int64(1760573700000),
		"unobserved_count": int64(0), "beyond_watermark": int64(0),
		"min_resolution": "stable", "max_resolution": "stable",
	}
	if _, err := decodeApplicationTopologyEdge(edge, allowed, 15, true); err == nil {
		t.Fatal("zero business ID cannot make a foreign edge look authorized")
	}
}

func TestApplicationTopologyRejectsDuplicateAndUnboundedRequest(t *testing.T) {
	request := []byte(`{"a3_contract_version":"a3_panorama_range_v1","a3_request":{"start_time_ms":1760572800000,"start_time_ms":1760572800000}}`)
	if _, err := ParseApplicationTopologyRangeRequest(request, 65536); err == nil {
		t.Fatal("duplicate JSON key must be rejected")
	}
	input := applicationTopologyInput()
	input.EndMS = input.StartMS + 31*24*60*60000
	if _, err := applicationTopologyExecutor(t, &applicationTopologyReader{}).QueryApplicationTopologyRange(context.Background(), input); err == nil {
		t.Fatal("more than 30 days must be rejected")
	}
}
