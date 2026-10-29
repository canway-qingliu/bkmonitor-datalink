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

func TestResolveScopeRequiresOnlyTenantAndSpace(t *testing.T) {
	scope, err := ResolveScope("system", "bkcc__42")
	if err != nil || scope.BkTenantID != "system" || scope.BkBizID != 42 {
		t.Fatalf("tenant and space did not resolve: scope=%+v err=%v", scope, err)
	}
}

func TestRouteSnapshotAcceptsPublishedBusinessTable(t *testing.T) {
	route := ReadRoute{
		BkTenantID: "system",
		BkBizID:    132,
		Profile:    metadata.TraceProfileSpan,
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
		Doris: &DorisTarget{ClusterID: "18", Database: "system_kapm", Table: "otel_trace_span_132", SchemaVersion: "sha256:3fee31e7c6376abe"},
	}
	second := first
	second.BkBizID = 133
	second.Doris = &DorisTarget{ClusterID: "18", Database: "system_kapm", Table: "otel_trace_span_133", SchemaVersion: "sha256:3fee31e7c6376abe"}
	snapshot, err := NewRouteSnapshot(7, []ReadRoute{first, second})
	if err != nil {
		t.Fatal(err)
	}
	for bizID, want := range map[int64]string{132: "otel_trace_span_132", 133: "otel_trace_span_133"} {
		route, err := snapshot.Resolve("system", bizID, metadata.TraceProfileSpan)
		if err != nil || route.Doris.Table != want {
			t.Fatalf("business %d was misrouted: %+v, %v", bizID, route, err)
		}
	}
}

func TestRouteSnapshotSelectsPublishedSpanTableByApplication(t *testing.T) {
	legacy := ReadRoute{
		BkTenantID: "system", BkBizID: 132, Profile: metadata.TraceProfileSpan,
		Doris: &DorisTarget{ClusterID: "18", Database: "system_kapm", Table: "otel_trace_span_132", SchemaVersion: "sha256:3fee31e7c6376abe"},
	}
	shared := ReadRoute{
		BkTenantID: "system", BkBizID: 132, AppID: 7, Profile: metadata.TraceProfileSpan,
		Doris: &DorisTarget{ClusterID: "18", Database: "tenant_bbc5e661e106c6dc_kapm", Table: "otel_trace_span", SchemaVersion: "sha256:3fee31e7c6376abe"},
	}
	exclusive := ReadRoute{
		BkTenantID: "system", BkBizID: 132, AppID: 8, Profile: metadata.TraceProfileSpan,
		Doris: &DorisTarget{ClusterID: "18", Database: "tenant_bbc5e661e106c6dc_kapm", Table: "otel_trace_span_app_8", SchemaVersion: "sha256:3fee31e7c6376abe"},
	}
	snapshot, err := NewRouteSnapshot(7, []ReadRoute{legacy, shared, exclusive})
	if err != nil {
		t.Fatal(err)
	}
	for appID, want := range map[int64]string{7: "otel_trace_span", 8: "otel_trace_span_app_8"} {
		route, resolveErr := snapshot.ResolveForApplication("system", 132, appID, metadata.TraceProfileSpan)
		if resolveErr != nil || route.Doris.Table != want {
			t.Fatalf("app %d route=%+v err=%v", appID, route, resolveErr)
		}
	}
	if route, err := snapshot.ResolveForApplication("system", 132, 9, metadata.TraceProfileSpan); err != nil || route.Doris.Table != "otel_trace_span_132" {
		t.Fatalf("business Span route was not selected: %+v %v", route, err)
	}
	if _, err := NewRouteSnapshot(7, []ReadRoute{shared, shared}); err == nil {
		t.Fatal("duplicate app route accepted")
	}
}

func TestResolveScopeAcceptsHyphenTenant(t *testing.T) {
	scope, err := ResolveScope("tenant-a", "bkcc__132")
	if err != nil || scope.BkTenantID != "tenant-a" || scope.BkBizID != 132 {
		t.Fatalf("valid tenant scope rejected: %+v %v", scope, err)
	}
}
