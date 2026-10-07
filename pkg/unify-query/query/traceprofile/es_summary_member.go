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
	"strconv"
	"strings"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

// ES Summary 保留 BMW 兼容字段名；仅这些别名能转换成闭合 typed Summary 行。
var esSummaryAliases = map[string]string{
	"bk_biz_id": "biz_id", "trace_start_time_us": "min_start_time",
	"trace_end_time_us": "max_end_time", "duration_us": "trace_duration",
	"span_min_duration_us": "span_min_duration", "span_max_duration_us": "span_max_duration",
	"root_service_name": "root_service", "root_span_service_name": "root_span_service",
}

var esCategoryStatistics = []string{"http", "rpc", "db", "messaging", "async_backend", "other"}
var esKindStatistics = []string{"async", "sync", "interval", "unspecified"}

func restoreESStatistics(row map[string]any, field string, keys []string) (map[string]any, error) {
	nested, hasNested := row[field]
	var values map[string]any
	if hasNested {
		var valid bool
		values, valid = nested.(map[string]any)
		if !valid {
			return nil, reject(CodePartialFailure, "ES Summary %s 不是对象", field)
		}
	} else {
		values = make(map[string]any, len(keys))
		for _, key := range keys {
			value, present := row[field+"."+key]
			if !present {
				return nil, reject(CodePartialFailure, "ES Summary %s 缺少 %s", field, key)
			}
			values[key] = value
		}
	}
	if len(values) != len(keys) {
		return nil, reject(CodePartialFailure, "ES Summary %s 字段数不完整", field)
	}
	allowed := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		allowed[key] = struct{}{}
	}
	result := make(map[string]any, len(keys))
	for key, value := range values {
		if _, ok := allowed[key]; !ok {
			return nil, reject(CodePartialFailure, "ES Summary %s 含未知统计项", field)
		}
		count, err := exactTraceInt64(value)
		if err != nil || count < 0 {
			return nil, reject(CodePartialFailure, "ES Summary %s 计数非法", field)
		}
		result[key] = count
	}
	for key := range row {
		if strings.HasPrefix(key, field+".") {
			if _, ok := allowed[strings.TrimPrefix(key, field+".")]; !ok || hasNested {
				return nil, reject(CodePartialFailure, "ES Summary %s 统计字段冲突", field)
			}
		}
	}
	return result, nil
}

// ShapeESSummaryMemberPage 把同一受信 RT 的完整 ES 精确命中转换为 Summary typed 行。
// 路由/总数/微秒边界/版本或任一行不符时整份失败，不筛掉坏行后伪装完整结果。
func ShapeESSummaryMemberPage(raw []map[string]any, total int64, request Request, scope Scope, target ESSummaryTarget) (Page, error) {
	lookup := request.MemberLookup
	if request.Profile != metadata.TraceProfileSummary || request.Scene != "global" || lookup == nil ||
		lookup.Backend != MemberBackendElasticsearch || len(lookup.AllowedPairs) != 1 ||
		target.BkTenantID != scope.BkTenantID || target.ObservationDomainID != scope.ObservationDomainID ||
		target.BkBizID != scope.BkBizID || target.PublishedVersion == 0 ||
		target.PublishedVersion != lookup.ExpectedRouteVersion ||
		lookup.AllowedPairs[0] != (MemberPair{AppID: target.AppID, BkBizID: target.BkBizID}) {
		return Page{}, reject(CodeUnauthorizedMember, "ES Summary 成员作用域与受信目标不一致")
	}
	if total < 0 || total > request.Limit || int64(len(raw)) != total {
		return Page{}, reject(CodeLimitExceeded, "ES Summary 精确成员 total 或完整行预算非法")
	}
	physicalRows := make([]map[string]any, 0, len(raw))
	for _, row := range raw {
		if row == nil {
			return Page{}, reject(CodePartialFailure, "ES Summary 成员行为空")
		}
		appID, appErr := exactTraceInt64(row["app_id"])
		bizID, bizErr := exactTraceInt64(row["biz_id"])
		started, startErr := exactTraceInt64(row["min_start_time"])
		ended, endErr := exactTraceInt64(row["max_end_time"])
		versionUS, versionErr := exactTraceInt64(row["time"])
		if appErr != nil || bizErr != nil || startErr != nil || endErr != nil || versionErr != nil ||
			appID != target.AppID || bizID != target.BkBizID ||
			row["bk_tenant_id"] != scope.BkTenantID || row["observation_domain_id"] != scope.ObservationDomainID ||
			row["app_name"] != target.AppName || row["trace_id"] != lookup.TraceID ||
			started < request.StartUS || started >= request.EndUS || ended < started ||
			row["trace_date"] != time.UnixMicro(started).UTC().Format("2006-01-02") ||
			versionUS <= 0 || versionUS%1000 != 0 {
			return Page{}, reject(CodeUnauthorizedMember, "ES Summary 成员行越过受信身份、时间或版本")
		}
		physical := make(map[string]any, len(summaryProjection))
		for _, field := range summaryProjection {
			switch field {
			case "app_id":
				physical[field] = strconv.FormatInt(appID, 10)
			case "bk_biz_id":
				physical[field] = bizID
			case "computed_at_ms":
				physical[field] = versionUS / 1000
			case "category_statistics":
				statistics, err := restoreESStatistics(row, field, esCategoryStatistics)
				if err != nil {
					return Page{}, err
				}
				physical[field] = statistics
			case "kind_statistics":
				statistics, err := restoreESStatistics(row, field, esKindStatistics)
				if err != nil {
					return Page{}, err
				}
				physical[field] = statistics
			default:
				source := field
				if alias, hasAlias := esSummaryAliases[field]; hasAlias {
					source = alias
				}
				value, present := row[source]
				if !present {
					return Page{}, reject(CodePartialFailure, "ES Summary 成员行缺少字段 %s", source)
				}
				physical[field] = value
			}
		}
		physicalRows = append(physicalRows, physical)
	}
	items, err := ShapeRows(metadata.TraceProfileSummary, physicalRows)
	if err != nil {
		return Page{}, reject(CodePartialFailure, "ES Summary 成员行合同非法: %s", err)
	}
	return Page{Items: items, Total: total}, nil
}
