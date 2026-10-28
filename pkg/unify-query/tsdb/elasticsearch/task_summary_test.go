// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package elasticsearch

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	elastic "github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
)

func TestParseTaskSummaryResultReturnsExactGroupedMetrics(t *testing.T) {
	var result elastic.SearchResult
	require.NoError(t, json.Unmarshal([]byte(`{
  "timed_out": false,
  "_shards": {"total": 1, "successful": 1, "failed": 0},
  "aggregations": {"tasks": {
    "doc_count_error_upper_bound": 0, "sum_other_doc_count": 0,
    "buckets": [{
      "key": "3:svc5:async4:task", "doc_count": 2,
      "duration_sum": {"value": 400000},
      "duration_max": {"value": 300000},
      "failed": {"doc_count": 1},
      "latest": {"hits": {"total": {"value": 1, "relation": "eq"}, "hits": [
        {"_source": {"trace_id": "trace-b", "start_time": 2000000}}
      ]}}
    }]
  }}
}`), &result))

	rows, err := parseTaskSummaryResult(&result)
	require.NoError(t, err)
	require.Equal(t, []TaskSummaryRow{{
		ServiceName: "svc", TaskType: "async", TaskName: "task",
		ExecutionCount: 2, FailureCount: 1, AvgDurationMs: 200,
		MaxDurationMs: 300, LastCalledMs: 2000, LatestTraceID: "trace-b",
	}}, rows)
}

func TestParseTaskSummaryResultRejectsPartialAggregations(t *testing.T) {
	for name, payload := range map[string]string{
		"timeout":           `{"timed_out":true,"_shards":{"total":1,"successful":1,"failed":0},"aggregations":{"tasks":{"buckets":[]}}}`,
		"terminated early":  `{"terminated_early":true,"_shards":{"total":1,"successful":1,"failed":0},"aggregations":{"tasks":{"buckets":[]}}}`,
		"failed shard":      `{"_shards":{"total":1,"successful":0,"failed":1},"aggregations":{"tasks":{"buckets":[]}}}`,
		"truncated buckets": `{"_shards":{"total":1,"successful":1,"failed":0},"aggregations":{"tasks":{"sum_other_doc_count":1,"buckets":[]}}}`,
		"approximate count": `{"_shards":{"total":1,"successful":1,"failed":0},"aggregations":{"tasks":{"doc_count_error_upper_bound":1,"buckets":[]}}}`,
		"missing buckets":   `{"_shards":{"total":1,"successful":1,"failed":0}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var result elastic.SearchResult
			require.NoError(t, json.Unmarshal([]byte(payload), &result))
			_, err := parseTaskSummaryResult(&result)
			require.Error(t, err)
		})
	}
}

func TestTaskSummaryAggregationUsesFixedScriptAndBoundedBuckets(t *testing.T) {
	agg := taskSummaryAggregation()
	source, err := agg.Source()
	require.NoError(t, err)
	encoded, err := json.Marshal(source)
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, json.Unmarshal(encoded, &body))
	terms := body["terms"].(map[string]any)
	require.Equal(t, float64(taskSummaryMaxBuckets+1), terms["size"])
	require.Equal(t, float64(taskSummaryMaxBuckets+1), terms["shard_size"])
	require.NotEmpty(t, terms["script"])
	aggs := body["aggregations"].(map[string]any)
	require.Contains(t, aggs, "duration_sum")
	require.Contains(t, aggs, "duration_max")
	require.Contains(t, aggs, "failed")
	require.Contains(t, aggs, "latest")
}

func TestParseTaskSummaryKeyUsesJavaStringLengthForUnicode(t *testing.T) {
	service, kind, name, err := parseTaskSummaryKey("2:服务5:async2:任务")
	require.NoError(t, err)
	require.Equal(t, "服务", service)
	require.Equal(t, "async", kind)
	require.Equal(t, "任务", name)
	_, _, _, err = parseTaskSummaryKey("2:服务5:async2:任务extra")
	require.Error(t, err)
}

func TestQueryTaskSummaryUsesNativeCandidateAndReturnsOnlyBuckets(t *testing.T) {
	mock.Init()
	ctx := metadata.InitHashID(context.Background())
	var searchBody map[string]any
	httpmock.RegisterResponder(http.MethodPost, mock.EsUrl+"/es_index/_search", func(request *http.Request) (*http.Response, error) {
		require.NoError(t, json.NewDecoder(request.Body).Decode(&searchBody))
		return httpmock.NewStringResponse(http.StatusOK, `{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]},"aggregations":{"tasks":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[]}}}`), nil
	})
	instance, err := NewInstance(ctx, &InstanceOption{
		Connect: Connect{Address: mock.EsUrl}, MaxSize: 5000, Timeout: 3 * time.Second,
	})
	require.NoError(t, err)
	query := &metadata.Query{
		DB: "es_index", TableID: "result_table.es", StorageType: metadata.ElasticsearchStorageType,
		QueryString: "*",
		TimeField:   metadata.TimeField{Name: "dtEventTimeStamp", Type: TimeFieldTypeTime, Unit: "millisecond"},
	}
	rows, err := instance.QueryTaskSummary(ctx, query, time.UnixMilli(1723593608000), time.UnixMilli(1723679962000))
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Equal(t, float64(0), searchBody["size"])
	require.Contains(t, searchBody, "aggregations")
	filter := searchBody["query"].(map[string]any)["bool"].(map[string]any)["filter"]
	require.Contains(t, stringMustJSON(t, filter), taskSummaryCandidates)
}

func stringMustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return string(encoded)
}
