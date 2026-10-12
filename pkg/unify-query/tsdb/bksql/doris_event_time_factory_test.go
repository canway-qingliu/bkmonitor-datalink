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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb/bksql"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb/bksql/sql_expr"
)

// Exercises QueryFactory with supplied metadata; does not invoke InitQueryFactory or metadata APIs.
func TestDorisEventTimeFactoryPhysicalTables(t *testing.T) {
	for _, hasShard := range []bool{false, true} {
		t.Run(map[bool]string{false: "without shard", true: "with shard"}[hasShard], func(t *testing.T) {
			fields := metadata.FieldsMap{"event_ms": {FieldType: sql_expr.DorisTypeBigInt}}
			if hasShard {
				fields[sql_expr.ShardKey] = metadata.FieldOption{FieldType: sql_expr.DorisTypeBigInt}
			}
			query := &metadata.Query{DB: "logs", DBs: []string{"logs", "logs_history"}, Measurement: sql_expr.Doris, Field: "event_ms", TimeField: metadata.TimeField{Name: "event_ms"}, Aggregates: metadata.Aggregates{{Name: "count", Window: 5 * time.Minute}}}
			factory := bksql.NewQueryFactory(metadata.InitHashID(context.Background()), query).WithFieldsMap(fields).WithRangeTime(time.UnixMilli(1791451200000), time.UnixMilli(1791454800000))
			sql, err := factory.SQL()
			require.NoError(t, err)
			require.Contains(t, sql, "(CAST(FLOOR((event_ms + 0) / 300000) AS BIGINT) * 300000 - 0)")
			require.NotContains(t, sql, "__shard_key__")
		})
	}
}

// Fixed bucket result rows span 09:55 and 10:00 UTC. Counts are independent
// inputs; this test checks formatter boundaries, not SQL arithmetic execution.
func TestDorisEventTimeFormatterCrossHour(t *testing.T) {
	for _, offset := range []int64{0, 12345, -12345} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			query := &metadata.Query{Measurement: sql_expr.Doris, Field: "event_ms", TimeField: metadata.TimeField{Name: "event_ms"}, Aggregates: metadata.Aggregates{{Name: "count", Window: 5 * time.Minute, TimeZoneOffset: offset}}}
			factory := bksql.NewQueryFactory(metadata.InitHashID(context.Background()), query).WithRangeTime(time.UnixMilli(1791453300000+offset), time.UnixMilli(1791453900000+offset))
			_, err := factory.SQL()
			require.NoError(t, err)
			result, err := factory.FormatDataToQueryResult(context.Background(), []map[string]any{{"_timestamp_": int64(1791453300000) + offset, "_value_": 7}, {"_timestamp_": int64(1791453600000) + offset, "_value_": 11}})
			require.NoError(t, err)
			require.Len(t, result.Timeseries, 1)
			require.Len(t, result.Timeseries[0].Samples, 2)
			require.Equal(t, int64(1791453300000)+offset, result.Timeseries[0].Samples[0].Timestamp)
			require.Equal(t, float64(7), result.Timeseries[0].Samples[0].Value)
			require.Equal(t, int64(1791453600000)+offset, result.Timeseries[0].Samples[1].Timestamp)
			require.Equal(t, float64(11), result.Timeseries[0].Samples[1].Value)
		})
	}
}
