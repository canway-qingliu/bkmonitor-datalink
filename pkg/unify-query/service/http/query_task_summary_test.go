// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	ir "github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/router/influxdb"
)

func TestValidateTaskSummaryQueryIsClosedAndVersioned(t *testing.T) {
	valid := `{"background_task_summary":{"version":1},"space_uid":"bkcc__2","start_time":"1793149612","end_time":"1793151414","query_list":[{"data_source":"bkapm","table_id":"2_bkapm.trace_demo","time_field":"start_time","query_string":"*"}]}`
	for name, body := range map[string]string{
		"valid":              valid,
		"missing contract":   `{"query_list":[{"data_source":"bkapm","table_id":"2_bkapm.trace_demo"}]}`,
		"wrong version":      `{"background_task_summary":{"version":2},"query_list":[{"data_source":"bkapm","table_id":"2_bkapm.trace_demo"}]}`,
		"extra contract key": `{"background_task_summary":{"version":1,"script":"x"},"query_list":[{"data_source":"bkapm","table_id":"2_bkapm.trace_demo"}]}`,
		"multiple queries":   `{"background_task_summary":{"version":1},"query_list":[{"data_source":"bkapm","table_id":"2_bkapm.trace_demo"},{"data_source":"bkapm","table_id":"2_bkapm.trace_other"}]}`,
		"other data source":  `{"background_task_summary":{"version":1},"query_list":[{"data_source":"bklog","table_id":"2_bkapm.trace_demo"}]}`,
		"caller function":    `{"background_task_summary":{"version":1},"query_list":[{"data_source":"bkapm","table_id":"2_bkapm.trace_demo","function":[{"method":"count"}]}]}`,
		"caller query":       `{"background_task_summary":{"version":1},"query_list":[{"data_source":"bkapm","table_id":"2_bkapm.trace_demo","query_string":"span_name:Quartz*"}]}`,
		"exact raw total":    `{"background_task_summary":{"version":1},"query_list":[{"data_source":"bkapm","table_id":"2_bkapm.trace_demo","exact_total":true}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			var query structured.QueryTs
			require.NoError(t, json.Unmarshal([]byte(body), &query))
			err := validateTaskSummaryQuery(&query)
			if name == "valid" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestHandlerQueryRawTaskSummaryRejectsMissingTrustedRoute(t *testing.T) {
	body := `{"background_task_summary":{"version":1},"space_uid":"bkcc__42","start_time":"1793149612","end_time":"1793151414","query_list":[{"data_source":"bkapm","table_id":"42_bkapm.trace_missing","query_string":"span_name:Quartz*"}]}`
	response := runTraceProfileRequest(t, body, "kingeye")
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.NotContains(t, response.Body.String(), `"background_task_summary_exact":true`)
}

func TestQueryTaskSummaryUsesOneResolvedESRoute(t *testing.T) {
	mock.Init()
	ctx := metadata.InitHashID(context.Background())
	const spaceUID = "bkcc__424242"
	metadata.SetUser(ctx, &metadata.User{TenantID: "system", SpaceUID: spaceUID})
	influxdb.MockSpaceRouter(ctx)
	router, err := influxdb.GetSpaceTsDbRouter()
	require.NoError(t, err)
	const table, index = "task_summary_rt", "task_summary_index"
	require.NoError(t, router.Add(ctx, ir.DataLabelToResultTableKey, table, &ir.ResultTableList{table}))
	require.NoError(t, router.Add(ctx, ir.ResultTableDetailKey, table, &ir.ResultTableDetail{
		StorageId: 3, TableId: table, DB: index, StorageType: "elasticsearch", DataLabel: table,
	}))
	space := ir.Space{table: &ir.SpaceResultTable{TableId: table}}
	require.NoError(t, router.Add(ctx, ir.SpaceToResultTableKey, spaceUID, &space))
	const esURL = "http://127.0.0.1:93002"
	mapping := `{"task_summary_index":{"mappings":{"properties":{"dtEventTimeStamp":{"type":"date"}}}}}`
	httpmock.RegisterResponder(http.MethodGet, esURL+"/"+index, httpmock.NewStringResponder(http.StatusOK, mapping))
	httpmock.RegisterResponder(http.MethodGet, esURL+"/"+index+"/_mapping/", httpmock.NewStringResponder(http.StatusOK, mapping))
	httpmock.RegisterResponder(http.MethodPost, esURL+"/"+index+"/_search", httpmock.NewStringResponder(http.StatusOK,
		`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]},"aggregations":{"tasks":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[]}}}`))
	query := &structured.QueryTs{
		// A valid 24-hour millisecond window rounds outward to 24 hours + 1 second.
		SpaceUid: spaceUID, Start: "1752141400000", End: "1752227801000",
		BackgroundTaskSummaryEnvelope: json.RawMessage(`{"version":1}`),
		QueryList:                     []*structured.Query{{DataSource: "bkapm", TableID: table, QueryString: "*"}},
	}
	rows, err := queryTaskSummary(ctx, query)
	require.NoError(t, err)
	require.Empty(t, rows)
	body, err := json.Marshal(query)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/query/ts/raw", bytes.NewReader(body)).WithContext(ctx)
	HandlerQueryRaw(ginContext)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"background_task_summary_exact":true`)
	overLimit := *query
	overLimit.End = "1752227803000"
	_, err = queryTaskSummary(ctx, &overLimit)
	require.ErrorContains(t, err, "exceeds 24 hours")
}
