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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

func TestRouteSnapshotAcceptsPublishedBusinessTable(t *testing.T) {
	route := ReadRoute{
		BkTenantID:          "system",
		BkBizID:             132,
		Profile:             metadata.TraceProfileSpan,
		ObservationDomainID: "system_kapm",
		Doris: &DorisTarget{
			ClusterID:     "18",
			Database:      "system_kapm",
			Table:         "otel_trace_span_132",
			SchemaVersion: "sha256:0123456789abcdef",
		},
	}
	if _, err := NewRouteSnapshot(7, []ReadRoute{route}); err != nil {
		t.Fatalf("published business table was rejected: %v", err)
	}
}

func TestRouteSnapshotKeepsBusinessesOnSeparateTables(t *testing.T) {
	first := ReadRoute{
		BkTenantID: "system", BkBizID: 132, Profile: metadata.TraceProfileSpan,
		ObservationDomainID: "system_kapm",
		Doris:               &DorisTarget{ClusterID: "18", Database: "system_kapm", Table: "otel_trace_span_132", SchemaVersion: "sha256:e6e9af09d86b396e"},
	}
	second := first
	second.BkBizID = 133
	second.Doris = &DorisTarget{ClusterID: "18", Database: "system_kapm", Table: "otel_trace_span_133", SchemaVersion: "sha256:e6e9af09d86b396e"}
	snapshot, err := NewRouteSnapshot(7, []ReadRoute{first, second})
	if err != nil {
		t.Fatal(err)
	}
	for bizID, want := range map[int64]string{132: "otel_trace_span_132", 133: "otel_trace_span_133"} {
		route, err := snapshot.Resolve("system", bizID, metadata.TraceProfileSpan, "system_kapm")
		if err != nil || route.Doris.Table != want {
			t.Fatalf("business %d was misrouted: %+v, %v", bizID, route, err)
		}
	}
}
