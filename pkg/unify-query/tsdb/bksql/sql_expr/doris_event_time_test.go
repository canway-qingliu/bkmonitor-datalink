// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package sql_expr

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

func TestDorisEventTimeBuckets(t *testing.T) {
	cases := []struct {
		name, field, want string
		window            time.Duration
		offset            int64
	}{
		{"1m", "dtEventTimeStamp", "(CAST(FLOOR((dtEventTimeStamp + 0) / 60000) AS BIGINT) * 60000 - 0)", time.Minute, 0},
		{"4m", "dtEventTimeStamp", "(CAST(FLOOR((dtEventTimeStamp + 0) / 240000) AS BIGINT) * 240000 - 0)", 4 * time.Minute, 0},
		{"5m", "dtEventTimeStamp", "(CAST(FLOOR((dtEventTimeStamp + 0) / 300000) AS BIGINT) * 300000 - 0)", 5 * time.Minute, 0},
		{"90s", "dtEventTimeStamp", "(CAST(FLOOR((dtEventTimeStamp + 0) / 90000) AS BIGINT) * 90000 - 0)", 90 * time.Second, 0},
		{"positive offset", "event_ms", "(CAST(FLOOR((event_ms - 12345) / 300000) AS BIGINT) * 300000 + 12345)", 5 * time.Minute, 12345},
		{"negative offset", "event_ms", "(CAST(FLOOR((event_ms + 12345) / 300000) AS BIGINT) * 300000 - 12345)", 5 * time.Minute, -12345}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expr := NewSQLExpr(Doris).WithInternalFields(c.field, "log").WithFieldsMap(metadata.FieldsMap{c.field: {FieldType: DorisTypeBigInt}, ShardKey: {FieldType: DorisTypeBigInt}, "log": {FieldType: DorisTypeText}})
			fields, groups, _, _, agg, err := expr.ParserAggregatesAndOrders(nil, metadata.Aggregates{{Name: "count", Window: c.window, TimeZoneOffset: c.offset}}, nil)
			require.NoError(t, err)
			t.Logf("GENERATED_BUCKET=%s", fields[len(fields)-1])
			require.Contains(t, strings.Join(fields, ", "), c.want+" AS `_timestamp_`")
			require.NotContains(t, strings.Join(fields, ", "), ShardKey)
			require.Contains(t, groups, TimeStamp)
			require.Equal(t, c.offset, agg.OffsetMillis)
		})
	}
}
