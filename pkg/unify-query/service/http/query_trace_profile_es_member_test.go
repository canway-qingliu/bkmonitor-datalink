// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb"
	ir "github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/router/influxdb"
)

func esMemberHTTPFixture(t *testing.T) (traceprofile.Request, traceprofile.Scope, traceprofile.ESSummaryTarget) {
	t.Helper()
	metadata.InitMetadata()
	scope, err := traceprofile.ResolveScope("system", "bkcc__42", "system_kapm")
	if err != nil {
		t.Fatal(err)
	}
	request := traceprofile.Request{
		Profile: metadata.TraceProfileSummary, Scene: "global",
		StartUS: 1721024820000000, EndUS: 1721024880000000, Limit: 2,
		MemberLookup: &traceprofile.MemberLookup{
			Backend: traceprofile.MemberBackendElasticsearch,
			TraceID: strings.Repeat("a", 32), ExpectedRouteVersion: 7,
			AllowedPairs: []traceprofile.MemberPair{{AppID: 7, BkBizID: 42}},
		},
	}
	target := traceprofile.ESSummaryTarget{
		BkTenantID: "system", BkBizID: 42, AppID: 7, AppName: "checkout",
		ObservationDomainID: "system_kapm", ResultTableID: "apm_global.trace_summary_v1_7",
		StorageClusterID: 9, RetentionDays: 30, PublishedVersion: 7,
	}
	return request, scope, target
}

func TestESMemberExecutionBuildsClosedExactQueryAndChecksResolvedRoute(t *testing.T) {
	request, scope, target := esMemberHTTPFixture(t)
	called := false
	reader := func(_ context.Context, query *structured.QueryTs) (int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error) {
		called = true
		if query.SpaceUid != "bkcc__42" || query.Start != "1721024820" || query.End != "1721024880" ||
			query.From != 0 || query.Limit != 2 || len(query.QueryList) != 1 ||
			!query.QueryList[0].ExactTotal || string(query.QueryList[0].TableID) != target.ResultTableID {
			t.Fatalf("ES exact request lost trusted bounds: %+v", query)
		}
		conditions := query.QueryList[0].Conditions.FieldList
		wanted := map[string]string{
			"trace_id": request.MemberLookup.TraceID, "bk_tenant_id": "system",
			"observation_domain_id": "system_kapm", "biz_id": "42", "app_id": "7",
		}
		for _, field := range conditions {
			if expected, found := wanted[field.DimensionName]; found {
				if field.Operator != "eq" || len(field.Value) != 1 || field.Value[0] != expected {
					t.Fatalf("ES scope condition drifted: %+v", field)
				}
				delete(wanted, field.DimensionName)
			}
		}
		if len(wanted) != 0 {
			t.Fatalf("ES query omitted paired scope conditions: %+v", wanted)
		}
		return 0, nil, nil, []metadata.RouteInfo{{
			TableID: target.ResultTableID, StorageType: metadata.ElasticsearchStorageType,
			StorageID: strconv.FormatInt(target.StorageClusterID, 10),
		}}, nil
	}
	page, err := executeESMemberLookup(context.Background(), request, scope, target, reader, traceprofile.DefaultESSummaryLimits())
	if err != nil || !called || page.Total != 0 || len(page.Items) != 0 {
		t.Fatalf("valid exact ES member query failed: called=%v page=%+v err=%v", called, page, err)
	}
}

func TestESMemberExecutionRejectsDriftedOrPartialRawRoute(t *testing.T) {
	request, scope, target := esMemberHTTPFixture(t)
	for name, route := range map[string]metadata.RouteInfo{
		"wrong table":   {TableID: "other.summary", StorageType: metadata.ElasticsearchStorageType, StorageID: "9"},
		"wrong cluster": {TableID: target.ResultTableID, StorageType: metadata.ElasticsearchStorageType, StorageID: "8"},
		"non ES":        {TableID: target.ResultTableID, StorageType: metadata.BkSqlStorageType, StorageID: "9"},
	} {
		t.Run(name, func(t *testing.T) {
			reader := func(_ context.Context, _ *structured.QueryTs) (int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error) {
				return 0, nil, nil, []metadata.RouteInfo{route}, nil
			}
			if _, err := executeESMemberLookup(context.Background(), request, scope, target, reader, traceprofile.DefaultESSummaryLimits()); err == nil {
				t.Fatal("drifted ES storage route was accepted")
			}
		})
	}
	ctx := metadata.InitHashID(context.Background())
	reader := func(ctx context.Context, _ *structured.QueryTs) (int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error) {
		metadata.SetStatus(ctx, metadata.QueryRawPartial, "one index failed")
		return 0, nil, nil, []metadata.RouteInfo{{
			TableID: target.ResultTableID, StorageType: metadata.ElasticsearchStorageType, StorageID: "9",
		}}, nil
	}
	if _, err := executeESMemberLookup(ctx, request, scope, target, reader, traceprofile.DefaultESSummaryLimits()); traceprofile.ErrorCode(err) != traceprofile.CodePartialFailure {
		t.Fatalf("partial ES index read escaped as complete: %v", err)
	}
}

func TestESMemberExecutionTraversesRealRawRouterAndMockElasticsearch(t *testing.T) {
	mock.Init()
	request, scope, target := esMemberHTTPFixture(t)
	ctx := metadata.InitHashID(context.Background())
	metadata.SetUser(ctx, &metadata.User{TenantID: "system", SpaceUID: scope.SpaceUID})
	influxdb.MockSpaceRouter(ctx)
	router, err := influxdb.GetSpaceTsDbRouter()
	require.NoError(t, err)
	detail := &ir.ResultTableDetail{
		StorageId: 9, TableId: target.ResultTableID, DB: "es_member_summary_index",
		StorageType: metadata.ElasticsearchStorageType,
	}
	detail.Options.TimeField = ir.TimeField{Name: "min_start_time", Type: "long", Unit: "microsecond"}
	require.NoError(t, router.Add(ctx, ir.ResultTableDetailKey, target.ResultTableID, detail))
	space := ir.Space{target.ResultTableID: &ir.SpaceResultTable{TableId: target.ResultTableID}}
	require.NoError(t, router.Add(ctx, ir.SpaceToResultTableKey, scope.SpaceUID, &space))
	tsdb.SetStorage("9", &tsdb.Storage{Type: metadata.ElasticsearchStorageType, Address: mock.EsUrl})
	properties := map[string]any{}
	for _, field := range esMemberColumns {
		properties[field] = map[string]any{"type": "keyword"}
	}
	for _, field := range []string{
		"app_id", "min_start_time", "max_end_time", "trace_duration", "span_count",
		"error_count", "service_count", "hierarchy_count", "span_min_duration", "span_max_duration",
		"event_time_ms", "root_service_kind", "root_span_kind", "time",
	} {
		properties[field] = map[string]any{"type": "long"}
	}
	properties["partial"] = map[string]any{"type": "boolean"}
	categoryProperties := map[string]any{}
	for _, key := range []string{"http", "rpc", "db", "messaging", "async_backend", "other"} {
		categoryProperties[key] = map[string]any{"type": "integer"}
	}
	kindProperties := map[string]any{}
	for _, key := range []string{"async", "sync", "interval", "unspecified"} {
		kindProperties[key] = map[string]any{"type": "integer"}
	}
	properties["category_statistics"] = map[string]any{"properties": categoryProperties}
	properties["kind_statistics"] = map[string]any{"properties": kindProperties}
	mappingBytes, err := json.Marshal(map[string]any{
		"es_member_summary_index": map[string]any{
			"settings": map[string]any{}, "mappings": map[string]any{"properties": properties},
		},
	})
	require.NoError(t, err)
	mapping := string(mappingBytes)
	httpmock.RegisterResponder(http.MethodGet, mock.EsUrl+"/es_member_summary_index",
		httpmock.NewStringResponder(http.StatusOK, mapping))
	httpmock.RegisterResponder(http.MethodGet, mock.EsUrl+"/es_member_summary_index/_mapping/",
		httpmock.NewStringResponder(http.StatusOK, mapping))
	row := map[string]any{
		"bk_tenant_id": "system", "observation_domain_id": "system_kapm",
		"biz_id": "42", "app_id": 7, "app_name": "checkout", "trace_id": request.MemberLookup.TraceID,
		"trace_date": "2024-07-15", "min_start_time": request.StartUS,
		"max_end_time": request.StartUS + 1000, "trace_duration": 1000,
		"span_count": 1, "error_count": 0, "service_count": 1, "hierarchy_count": 0,
		"span_min_duration": 1000, "span_max_duration": 1000, "event_time_ms": 0,
		"root_service": "checkout", "root_service_span_id": "1112131415161718",
		"root_service_span_name": "GET /", "root_service_kind": 2,
		"root_service_category": "http", "root_service_status_code": 200,
		"root_span_id": "1112131415161718", "root_span_name": "GET /",
		"root_span_service": "checkout", "root_span_kind": 2,
		"category_statistics": map[string]any{
			"http": 1, "rpc": 0, "db": 0, "messaging": 0, "async_backend": 0, "other": 0,
		},
		"kind_statistics": map[string]any{
			"async": 0, "sync": 1, "interval": 0, "unspecified": 0,
		},
		"partial": false, "time": request.StartUS + 1000000,
	}
	encoded, err := json.Marshal(row)
	require.NoError(t, err)
	searches := 0
	httpmock.RegisterResponder(http.MethodPost, mock.EsUrl+"/es_member_summary_index/_search",
		func(*http.Request) (*http.Response, error) {
			searches++
			return httpmock.NewStringResponse(http.StatusOK,
				`{"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"hits":[`+
					`{"_index":"es_member_summary_index","_id":"member-7","_source":`+string(encoded)+`}]}}`), nil
		})
	var executedQuery *structured.QueryTs
	reader := func(ctx context.Context, query *structured.QueryTs) (
		int64, []map[string]any, metadata.ResultTableOptions, []metadata.RouteInfo, error,
	) {
		executedQuery = query
		return queryESMemberRawWithTimeField(ctx, query)
	}
	page, err := executeESMemberLookup(ctx, request, scope, target, reader, traceprofile.DefaultESSummaryLimits())
	require.NoError(t, err)
	require.Equal(t, int64(1), page.Total)
	require.Len(t, page.Items, 1)
	require.Equal(t, "7", page.Items[0]["app_id"])
	require.Greater(t, searches, 0)
	// 已校验的查询引用必须用于实际 ES 执行；中途 RT 变更不能触发第二次路由解析。
	resolved, err := executedQuery.ToQueryReference(ctx)
	require.NoError(t, err)
	detail.Options.TimeField = ir.TimeField{}
	require.NoError(t, router.Add(ctx, ir.ResultTableDetailKey, target.ResultTableID, detail))
	total, rows, _, _, err := queryRawWithResolvedInstance(ctx, executedQuery, resolved)
	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Len(t, rows, 1)
	before := searches
	_, err = executeESMemberLookup(ctx, request, scope, target, queryESMemberRawWithTimeField, traceprofile.DefaultESSummaryLimits())
	require.Equal(t, traceprofile.CodeScopePartial, traceprofile.ErrorCode(err))
	require.Equal(t, before, searches, "wrong RT time field must fail before ES I/O")
}
