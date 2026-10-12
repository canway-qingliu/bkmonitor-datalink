// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package bksql_test

import (
	"context"
	_ "embed"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb/bksql"
)

type replayQueryable struct{ result *prompb.QueryResult }

func (q replayQueryable) Querier(context.Context, int64, int64) (storage.Querier, error) {
	return replayQuerier{q.result}, nil
}

type replayQuerier struct{ result *prompb.QueryResult }

func (q replayQuerier) Close() error { return nil }
func (q replayQuerier) LabelNames(...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}
func (q replayQuerier) LabelValues(string, ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}
func (q replayQuerier) Select(_ bool, hints *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return remote.FromQueryResult(true, q.result)
}

//go:embed testdata/doris_event_time_5m.json
var dorisEventTimeFixture []byte

// Replays captured SQL result rows through the production formatter and PromQL engine.
// SQL generation is checked separately; this test does not execute or interpret SQL.
func TestDorisEventTimeFormatterPromQLReplay(t *testing.T) {
	var f struct {
		StartMS      int64            `json:"start_ms"`
		EndMS        int64            `json:"end_ms"`
		EvalStartMS  int64            `json:"evaluation_start_ms"`
		EvalEndMS    int64            `json:"evaluation_end_ms"`
		OldRows      []map[string]any `json:"old_rows"`
		NewRows      []map[string]any `json:"new_rows"`
		ExpectedRows []struct {
			Timestamp int64   `json:"timestamp"`
			Count     float64 `json:"count"`
		} `json:"expected_rows"`
	}
	e := json.Unmarshal(dorisEventTimeFixture, &f)
	if e != nil {
		t.Fatal(e)
	}
	ctx := metadata.InitHashID(context.Background())
	var q structured.Query
	if e = json.Unmarshal([]byte(`{"reference_name":"a","function":[{"method":"sum","dimensions":[]}],"time_aggregation":{"function":"count_over_time","window":"5m"},"step":"5m"}`), &q); e != nil {
		t.Fatal(e)
	}
	aggs, e := q.Aggregates()
	if e != nil || len(aggs) != 1 || !q.IsDomSampled {
		t.Fatalf("downsample aggs=%v err=%v", aggs, e)
	}
	q.AlignInfluxdbResult = true
	expr, e := q.ToPromExpr(ctx, &structured.PromExprOption{IgnoreTimeAggregationEnable: true})
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("production ToPromExpr=%s", expr.String())
	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 100000, Timeout: time.Minute, EnableNegativeOffset: true})
	expected := map[int64]float64{}
	for _, r := range f.ExpectedRows {
		expected[r.Timestamp] = r.Count
	}
	for name, rows := range map[string][]map[string]any{"old": f.OldRows, "new": f.NewRows} {
		t.Run(name, func(t *testing.T) {
			query := &metadata.Query{DataSource: "bkdata", StorageType: metadata.BkSqlStorageType, TableID: "actual_log.doris", DB: "actual_log", Measurement: "doris", Field: "dtEventTimeStamp", Aggregates: aggs}
			fact := bksql.NewQueryFactory(ctx, query).WithRangeTime(time.UnixMilli(f.StartMS), time.UnixMilli(f.EndMS))
			if _, e := fact.SQL(); e != nil {
				t.Fatal(e)
			}
			result, e := fact.FormatDataToQueryResult(ctx, rows)
			if e != nil {
				t.Fatal(e)
			}
			for _, series := range result.Timeseries {
				series.Labels = []prompb.Label{{Name: "__name__", Value: "a"}}
				t.Logf("formatter samples=%v", series.Samples)
			}
			rq, e := engine.NewRangeQuery(replayQueryable{result}, &promql.QueryOpts{}, expr.String(), time.UnixMilli(f.EvalStartMS), time.UnixMilli(f.EvalEndMS), 5*time.Minute)
			if e != nil {
				t.Fatal(e)
			}
			defer rq.Close()
			got := rq.Exec(ctx)
			if got.Err != nil {
				t.Fatal(got.Err)
			}
			matrix, ok := got.Value.(promql.Matrix)
			if !ok || len(matrix) != 1 {
				t.Fatalf("matrix=%v", got.Value)
			}
			if len(matrix[0].Points) != len(expected) {
				t.Fatalf("points=%d expected=%d", len(matrix[0].Points), len(expected))
			}
			for _, p := range matrix[0].Points {
				want, ok := expected[p.T]
				if !ok {
					t.Fatalf("unexpected time %d", p.T)
				}
				if name == "old" {
					want = 0
				}
				if p.V != want {
					t.Errorf("time=%d actual=%v expected=%v", p.T, p.V, want)
				}
			}
			t.Logf("evaluation points=%v", matrix[0].Points)
		})
	}
}
