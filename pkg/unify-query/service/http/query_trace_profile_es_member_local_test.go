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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb"
	ir "github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/router/influxdb"
)

// Opt-in: ES_TEST_URL/ES_TEST_USERNAME/ES_TEST_PASSWORD must point to a local disposable ES.
func TestESSummaryMemberLookupAgainstLocalElasticsearch(t *testing.T) {
	baseURL, username, password := os.Getenv("ES_TEST_URL"), os.Getenv("ES_TEST_USERNAME"), os.Getenv("ES_TEST_PASSWORD")
	if baseURL == "" || username == "" || password == "" {
		t.Skip("local Elasticsearch credentials are not configured")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") ||
		(parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost") || parsed.Port() == "" {
		t.Fatal("ES_TEST_URL must point to a local HTTP Elasticsearch port")
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	client := &http.Client{Timeout: 5 * time.Second}
	requestES := func(method, path string, body []byte) (*http.Response, error) {
		request, requestErr := http.NewRequest(method, baseURL+path, bytes.NewReader(body))
		if requestErr != nil {
			return nil, requestErr
		}
		request.SetBasicAuth(username, password)
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		return client.Do(request)
	}
	index := fmt.Sprintf("codex-es-member-local-%d", time.Now().UnixNano())
	readAlias := index + "-read"
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
	category := map[string]any{}
	for _, key := range []string{"http", "rpc", "db", "messaging", "async_backend", "other"} {
		category[key] = map[string]any{"type": "integer"}
	}
	kind := map[string]any{}
	for _, key := range []string{"async", "sync", "interval", "unspecified"} {
		kind[key] = map[string]any{"type": "integer"}
	}
	properties["category_statistics"] = map[string]any{"properties": category}
	properties["kind_statistics"] = map[string]any{"properties": kind}
	definition, err := json.Marshal(map[string]any{
		"mappings": map[string]any{"properties": properties},
		"aliases":  map[string]any{readAlias: map[string]any{}},
	})
	require.NoError(t, err)
	created, err := requestES(http.MethodPut, "/"+index, definition)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, created.Body)
	_ = created.Body.Close()
	t.Cleanup(func() {
		deleted, deleteErr := requestES(http.MethodDelete, "/"+index, nil)
		if deleteErr != nil {
			t.Errorf("temporary ES index cleanup failed: %v", deleteErr)
			return
		}
		_, _ = io.Copy(io.Discard, deleted.Body)
		_ = deleted.Body.Close()
		if deleted.StatusCode != http.StatusOK {
			t.Errorf("temporary ES index cleanup returned HTTP %d", deleted.StatusCode)
		}
	})
	require.Equal(t, http.StatusOK, created.StatusCode)

	request, scope, target := esMemberHTTPFixture(t)
	row := map[string]any{
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
	writeRow := func(id string, value map[string]any) {
		encoded, marshalErr := json.Marshal(value)
		require.NoError(t, marshalErr)
		indexed, indexErr := requestES(http.MethodPut, "/"+index+"/_doc/"+id+"?refresh=wait_for", encoded)
		require.NoError(t, indexErr)
		_, _ = io.Copy(io.Discard, indexed.Body)
		_ = indexed.Body.Close()
		require.True(t, indexed.StatusCode == http.StatusCreated || indexed.StatusCode == http.StatusOK)
	}
	writeRow("member-7", row)
	foreign := make(map[string]any, len(row))
	for key, value := range row {
		foreign[key] = value
	}
	foreign["app_id"] = 8
	writeRow("member-8", foreign)

	metadata.InitMetadata()
	ctx := metadata.InitHashID(context.Background())
	metadata.SetUser(ctx, &metadata.User{TenantID: "system", SpaceUID: scope.SpaceUID})
	influxdb.MockSpaceRouter(ctx)
	router, err := influxdb.GetSpaceTsDbRouter()
	require.NoError(t, err)
	detail := &ir.ResultTableDetail{
		StorageId: 9, TableId: target.ResultTableID, DB: readAlias,
		StorageType: metadata.ElasticsearchStorageType,
	}
	detail.Options.TimeField = ir.TimeField{Name: "min_start_time", Type: "long", Unit: "microsecond"}
	require.NoError(t, router.Add(ctx, ir.ResultTableDetailKey, target.ResultTableID, detail))
	space := ir.Space{target.ResultTableID: &ir.SpaceResultTable{TableId: target.ResultTableID}}
	require.NoError(t, router.Add(ctx, ir.SpaceToResultTableKey, scope.SpaceUID, &space))
	tsdb.SetStorage("9", &tsdb.Storage{
		Type: metadata.ElasticsearchStorageType, Address: baseURL,
		Username: username, Password: password,
	})
	page, err := executeESMemberLookup(ctx, request, scope, target, queryESMemberRawWithTimeField, traceprofile.NewESSummaryLimits(traceprofile.ESSummaryMaxRows, traceprofile.ESSummaryMaxOffset))
	require.NoError(t, err)
	require.Equal(t, int64(1), page.Total)
	require.Len(t, page.Items, 1)
	require.Equal(t, "7", page.Items[0]["app_id"])
	values := page.Items[0]["values"].(map[string]any)
	require.Equal(t, request.StartUS, values["trace_start_time_us"])
	require.Equal(t, false, values["partial"])
	require.Len(t, values["category_statistics"].(map[string]any), 6)
	require.Len(t, values["kind_statistics"].(map[string]any), 4)
}
