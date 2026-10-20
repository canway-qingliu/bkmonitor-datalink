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
	"fmt"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

type CompiledAggregate struct {
	Statement string
	Args      []any
}

// CompileAggregate uses the same trusted route and isolation predicates as the list reader.
// LIMIT + 1 lets the executor reject an oversized result instead of returning a partial series.
func CompileAggregate(
	profile metadata.TraceProfile, target DorisTarget, scope Scope, request Request, limits Limits,
) (CompiledAggregate, error) {
	aggregation := request.Aggregation
	if aggregation == nil || aggregation.Method != "count" || aggregation.IntervalSeconds <= 0 ||
		aggregation.IntervalSeconds > 86400 || request.Offset != nil || request.MemberLookup != nil ||
		len(request.OrderBy) != 0 || request.Limit <= 0 || request.Limit >= 1<<63-1 ||
		aggregation.TimezoneOffsetSeconds < -14*3600 || aggregation.TimezoneOffsetSeconds > 14*3600 ||
		len(aggregation.GroupBy) > 5 {
		return CompiledAggregate{}, reject(CodePredicateConflict, "Trace 聚合参数不符合闭合合同")
	}
	expectedTable, err := physicalTableForSchema(profile, scope.BkBizID, target.SchemaVersion)
	if err != nil || target.Table != expectedTable {
		return CompiledAggregate{}, reject(CodeRouteNotFound, "Trace 聚合读目标与 profile 不匹配")
	}
	if request.EndUS <= request.StartUS || limits.MaxRangeUS > 0 && !WindowFits(request.StartUS, request.EndUS, limits.MaxRangeUS) {
		return CompiledAggregate{}, reject(CodeUnboundedRange, "Trace 聚合时间范围无效或超预算")
	}
	projection := projectionForProfile(profile)
	columns := make([]string, 0, len(aggregation.GroupBy))
	seen := make(map[string]struct{}, len(aggregation.GroupBy))
	for _, field := range aggregation.GroupBy {
		allowed := false
		for _, column := range projection {
			if column == field {
				allowed = true
				break
			}
		}
		if _, jsonColumn := jsonProjectionColumns[profile][field]; jsonColumn {
			allowed = false
		}
		if _, identity := identityScopeColumns[field]; identity {
			allowed = false
		}
		if _, duplicate := seen[field]; !allowed || duplicate {
			return CompiledAggregate{}, reject(CodeUnsupportedSchemaCapability, "Trace 聚合分组字段不受支持: %s", field)
		}
		seen[field] = struct{}{}
		columns = append(columns, quoteIdentifier(field))
	}
	where, whereArgs, err := buildWhere(profile, scope, request)
	if err != nil {
		return CompiledAggregate{}, err
	}
	offsetUS := aggregation.TimezoneOffsetSeconds * 1_000_000
	intervalUS := aggregation.IntervalSeconds * 1_000_000
	selectColumns := []string{
		fmt.Sprintf("CAST(FLOOR((%s + ?) / ?) * ? - ? AS BIGINT) AS `bucket_start_us`",
			quoteIdentifier(timeColumnForProfile(profile))),
	}
	selectColumns = append(selectColumns, columns...)
	selectColumns = append(selectColumns, "COUNT(*) AS `count`")
	ordinals := make([]string, 0, 1+len(columns))
	for index := 1; index <= 1+len(columns); index++ {
		ordinals = append(ordinals, fmt.Sprint(index))
	}
	statement := fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s GROUP BY %s ORDER BY %s LIMIT %d",
		strings.Join(selectColumns, ", "), quoteIdentifier(target.Database), quoteIdentifier(target.Table), where,
		strings.Join(ordinals, ", "), strings.Join(ordinals, ", "), request.Limit+1)
	args := []any{offsetUS, intervalUS, intervalUS, offsetUS}
	args = append(args, whereArgs...)
	return CompiledAggregate{Statement: statement, Args: args}, nil
}
