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
	"context"
	"math"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

type AggregateRow struct {
	BucketStartMS int64          `json:"bucket_start_ms"`
	Count         int64          `json:"count"`
	Dimensions    map[string]any `json:"dimensions"`
}

// ExecuteAggregate reads exact server-side buckets from the trusted Trace route.
func (e *Executor) ExecuteAggregate(ctx context.Context, request Request, scope Scope) ([]AggregateRow, error) {
	if e == nil || e.snapshot == nil || request.Aggregation == nil {
		return nil, reject(CodeRouteNotFound, "Trace 聚合读路由或请求未就绪")
	}
	route, err := e.snapshot.Resolve(scope.BkTenantID, scope.BkBizID, request.Profile, scope.ObservationDomainID)
	if err != nil {
		return nil, err
	}
	connection, ok := e.connections[route.Doris.ClusterID]
	if !ok {
		return nil, reject(CodeRouteNotFound, "Trace 目标集群缺少只读连接: %s", route.Doris.ClusterID)
	}
	compiled, err := CompileAggregate(request.Profile, *route.Doris, scope, request, e.limits)
	if err != nil {
		return nil, err
	}
	result, err := connection.Query(ctx, compiled.Statement, compiled.Args...)
	if err != nil {
		return nil, queryFailure(err)
	}
	if int64(len(result.Rows)) > request.Limit {
		return nil, reject(CodeLimitExceeded, "Trace 聚合结果超过点数预算")
	}
	rows := make([]AggregateRow, 0, len(result.Rows))
	for index, raw := range result.Rows {
		bucketUS, bucketErr := exactTraceInt64(raw["bucket_start_us"])
		count, countErr := exactTraceInt64(raw["count"])
		if bucketErr != nil || countErr != nil || bucketUS%1000 != 0 || count <= 0 ||
			bucketUS < request.StartUS-request.Aggregation.IntervalSeconds*1_000_000 || bucketUS >= request.EndUS {
			return nil, reject(CodePartialFailure, "Trace 聚合第 %d 行桶或计数非法", index)
		}
		expectedFields := 2 + len(request.Aggregation.GroupBy)
		if len(raw) != expectedFields {
			return nil, reject(CodePartialFailure, "Trace 聚合第 %d 行字段数量非法", index)
		}
		dimensions := make(map[string]any, len(request.Aggregation.GroupBy))
		for _, field := range request.Aggregation.GroupBy {
			value, present := raw[field]
			if !present {
				return nil, reject(CodePartialFailure, "Trace 聚合第 %d 行缺少维度 %s", index, field)
			}
			switch value.(type) {
			case nil, string, bool, int, int64, float64:
				if number, ok := value.(float64); ok && (math.IsNaN(number) || math.IsInf(number, 0)) {
					return nil, reject(CodePartialFailure, "Trace 聚合第 %d 行维度 %s 非有限数值", index, field)
				}
				dimensions[field] = value
			default:
				return nil, reject(CodePartialFailure, "Trace 聚合第 %d 行维度 %s 不是标量", index, field)
			}
		}
		rows = append(rows, AggregateRow{BucketStartMS: bucketUS / 1000, Count: count, Dimensions: dimensions})
	}
	return rows, nil
}

type AggregateResponse struct {
	TraceContractVersion string                `json:"trace_contract_version"`
	TraceProfile         metadata.TraceProfile `json:"trace_profile"`
	ScopeFingerprint     string                `json:"scope_fingerprint"`
	ProductState         any                   `json:"product_state"`
	Buckets              []AggregateRow        `json:"buckets"`
}

func NewAggregateResponse(request Request, rows []AggregateRow) AggregateResponse {
	if rows == nil {
		rows = []AggregateRow{}
	}
	return AggregateResponse{
		TraceContractVersion: aggregateContractVersion,
		TraceProfile:         request.Profile,
		ScopeFingerprint:     request.ScopeFingerprint,
		ProductState:         nil,
		Buckets:              rows,
	}
}
