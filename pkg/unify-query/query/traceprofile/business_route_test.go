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

func TestRouteSnapshotRejectsPublishedBusinessTable(t *testing.T) {
	route := ReadRoute{
		BkTenantID: "system",
		BkBizID:    132,
		Profile:    metadata.TraceProfileSpan,
		Doris: &DorisTarget{
			ClusterID:     "18",
			Database:      "system_kapm",
			Table:         "cw_trace_span_132",
			SchemaVersion: "sha256:0123456789abcdef",
		},
	}
	for _, appID := range []int64{0, 7} {
		route.AppID = appID
		if _, err := NewRouteSnapshot(7, []ReadRoute{route}); err == nil {
			t.Errorf("retired business table accepted for app_id=%d", appID)
		}
	}
	for _, appID := range []int64{0, -1} {
		for _, table := range []string{"cw_trace_span", "cw_trace_span_app_7"} {
			route.AppID = appID
			route.Doris.Table = table
			if _, err := NewRouteSnapshot(7, []ReadRoute{route}); err == nil {
				t.Errorf("application table %s accepted non-positive app_id=%d", table, appID)
			}
		}
	}
}

func TestRouteSnapshotKeepsApplicationsOnSeparateTables(t *testing.T) {
	first := ReadRoute{
		BkTenantID: "system", BkBizID: 132, AppID: 7, Profile: metadata.TraceProfileSpan,
		Doris: &DorisTarget{ClusterID: "18", Database: "system_kapm", Table: "cw_trace_span_app_7", SchemaVersion: "sha256:0bf5aac6160a636e"},
	}
	second := first
	second.BkBizID = 133
	second.AppID = 8
	second.Doris = &DorisTarget{ClusterID: "18", Database: "system_kapm", Table: "cw_trace_span_app_8", SchemaVersion: "sha256:0bf5aac6160a636e"}
	snapshot, err := NewRouteSnapshot(7, []ReadRoute{first, second})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []ReadRoute{first, second} {
		route, err := snapshot.ResolveForApplication("system", expected.BkBizID, expected.AppID, metadata.TraceProfileSpan)
		if err != nil || route.Doris.Table != expected.Doris.Table {
			t.Fatalf("application %d was misrouted: %+v, %v", expected.AppID, route, err)
		}
	}
	if _, err := snapshot.ResolveForApplication("system", 132, 8, metadata.TraceProfileSpan); ErrorCode(err) != CodeRouteNotFound {
		t.Fatalf("application crossed the trusted business: %v", err)
	}
}

func TestRouteSnapshotSelectsPublishedSpanTableByApplication(t *testing.T) {
	shared := ReadRoute{
		BkTenantID: "system", BkBizID: 132, AppID: 7, Profile: metadata.TraceProfileSpan,
		Doris: &DorisTarget{ClusterID: "18", Database: "tenant_bbc5e661e106c6dc_kapm", Table: "cw_trace_span", SchemaVersion: "sha256:0bf5aac6160a636e"},
	}
	exclusive := ReadRoute{
		BkTenantID: "system", BkBizID: 132, AppID: 8, Profile: metadata.TraceProfileSpan,
		Doris: &DorisTarget{ClusterID: "18", Database: "tenant_bbc5e661e106c6dc_kapm", Table: "cw_trace_span_app_8", SchemaVersion: "sha256:0bf5aac6160a636e"},
	}
	snapshot, err := NewRouteSnapshot(7, []ReadRoute{shared, exclusive})
	if err != nil {
		t.Fatal(err)
	}
	for appID, want := range map[int64]string{7: "cw_trace_span", 8: "cw_trace_span_app_8"} {
		route, resolveErr := snapshot.ResolveForApplication("system", 132, appID, metadata.TraceProfileSpan)
		if resolveErr != nil || route.Doris.Table != want {
			t.Fatalf("app %d route=%+v err=%v", appID, route, resolveErr)
		}
	}
	for _, appID := range []int64{0, -1, 9} {
		if _, err := snapshot.ResolveForApplication("system", 132, appID, metadata.TraceProfileSpan); ErrorCode(err) != CodeRouteNotFound {
			t.Fatalf("missing application %d did not fail closed: %v", appID, err)
		}
	}
	if _, err := NewRouteSnapshot(7, []ReadRoute{shared, shared}); err == nil {
		t.Fatal("duplicate app route accepted")
	}
}

func TestRouteSnapshotValidatesOptionalSourceIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, resultTableID string
		dataID              int64
		valid               bool
	}{
		{name: "absent", valid: true},
		{name: "paired", resultTableID: "system.apm_trace_span", dataID: 901, valid: true},
		{name: "missing RT", dataID: 901},
		{name: "missing DataID", resultTableID: "system.apm_trace_span"},
		{name: "blank RT", resultTableID: " ", dataID: 901},
		{name: "negative DataID", resultTableID: "system.apm_trace_span", dataID: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := ReadRoute{
				BkTenantID: "system", BkBizID: 132, AppID: 7, Profile: metadata.TraceProfileSpan,
				ResultTableID: tc.resultTableID, DataID: tc.dataID,
				Doris: &DorisTarget{ClusterID: "18", Database: "system_kapm", Table: "cw_trace_span", SchemaVersion: "test"},
			}
			snapshot, err := NewRouteSnapshot(7, []ReadRoute{route})
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
				resolved, err := snapshot.ResolveForApplication("system", 132, 7, metadata.TraceProfileSpan)
				if err != nil || resolved.ResultTableID != tc.resultTableID || resolved.DataID != tc.dataID {
					t.Fatalf("snapshot lost source identity: %+v %v", resolved, err)
				}
			} else if err == nil {
				t.Fatal("snapshot accepted incomplete source identity")
			}
		})
	}
}

func TestResolveScopeAcceptsHyphenTenant(t *testing.T) {
	scope, err := ResolveScope("tenant-a", "bkcc__132")
	if err != nil || scope.BkTenantID != "tenant-a" || scope.BkBizID != 132 {
		t.Fatalf("valid tenant scope rejected: %+v %v", scope, err)
	}
}
