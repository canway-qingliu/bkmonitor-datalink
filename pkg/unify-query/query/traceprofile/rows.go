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
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

var (
	traceIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
	spanIDPattern  = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

var traceIntegerFields = map[string]struct{}{
	"bk_biz_id": {}, "data_id": {}, "span_kind": {}, "status_code": {},
	"start_time_us": {}, "end_time_us": {}, "duration_us": {}, "event_time_ms": {},
	"ingest_time": {}, "trace_start_time_us": {}, "trace_end_time_us": {},
	"span_count": {}, "error_count": {}, "service_count": {}, "hierarchy_count": {},
	"span_min_duration_us": {}, "span_max_duration_us": {}, "root_service_kind": {},
	"root_span_kind": {}, "computed_at_ms": {},
}

var traceNullableFields = map[string]struct{}{
	"service_namespace": {}, "service_name": {},
	"service_instance_id": {}, "deployment_environment": {},
	"root_service_name": {}, "root_span_service_name": {}, "root_service_status_code": {},
}

var traceInternalRowFields = map[string]struct{}{
	"_value_": {}, "_timestamp_": {},
	metadata.KeyIndex: {}, metadata.KeyTableID: {}, metadata.KeyDataLabel: {}, metadata.KeyTableUUID: {},
}

var spanIdentityFields = map[string]struct{}{
	"bk_tenant_id": {}, "observation_domain_id": {}, "bk_biz_id": {}, "data_id": {},
	"app_id": {}, "space_uid": {}, "trace_id": {}, "span_id": {},
}

// ShapeRows converts a complete physical Trace page into the closed KAPM row
// contract. Any invalid row rejects the entire page; no partial result escapes.
func ShapeRows(profile metadata.TraceProfile, input []map[string]any) ([]map[string]any, error) {
	if profile != metadata.TraceProfileSpan && profile != metadata.TraceProfileSummary {
		return nil, fmt.Errorf("Trace 行 profile 不受支持")
	}
	projection := projectionForProfile(profile)
	allowed := make(map[string]struct{}, len(projection)+len(traceInternalRowFields))
	for _, field := range projection {
		allowed[field] = struct{}{}
	}
	for field := range traceInternalRowFields {
		allowed[field] = struct{}{}
	}
	items := make([]map[string]any, 0, len(input))
	for index, row := range input {
		if row == nil {
			return nil, fmt.Errorf("Trace 第 %d 行为空", index)
		}
		for field := range row {
			if _, ok := allowed[field]; !ok {
				return nil, fmt.Errorf("Trace 第 %d 行含未声明字段 %s", index, field)
			}
		}
		physical := make(map[string]any, len(projection))
		for _, field := range projection {
			value, present := row[field]
			if !present {
				return nil, fmt.Errorf("Trace 第 %d 行缺少字段 %s", index, field)
			}
			normalized, err := normalizeTracePhysicalField(profile, field, value)
			if err != nil {
				return nil, fmt.Errorf("Trace 第 %d 行字段 %s 非法: %w", index, field, err)
			}
			physical[field] = normalized
		}
		traceID, ok := physical["trace_id"].(string)
		if !ok || !traceIDPattern.MatchString(traceID) {
			return nil, fmt.Errorf("Trace 第 %d 行 trace_id 非法", index)
		}
		appID, ok := physical["app_id"].(string)
		if !ok || appID == "" || strings.TrimSpace(appID) != appID {
			return nil, fmt.Errorf("Trace 第 %d 行 app_id 非法", index)
		}
		tenantID := physical["bk_tenant_id"].(string)
		if tenantID == "" || physical["observation_domain_id"] != tenantID+"_kapm" {
			return nil, fmt.Errorf("Trace 第 %d 行租户与观测域不一致", index)
		}
		bizID := physical["bk_biz_id"].(int64)
		if bizID < 0 {
			return nil, fmt.Errorf("Trace 第 %d 行业务 ID 非法", index)
		}
		values := make(map[string]any, len(physical))
		if profile == metadata.TraceProfileSpan {
			spanID, ok := physical["span_id"].(string)
			if !ok || !spanIDPattern.MatchString(spanID) || physical["data_id"].(int64) <= 0 ||
				physical["space_uid"] != "bkcc__"+strconv.FormatInt(bizID, 10) {
				return nil, fmt.Errorf("Trace 第 %d 行 Span 成员身份非法", index)
			}
			for field, value := range physical {
				if _, identity := spanIdentityFields[field]; !identity {
					values[field] = value
				}
			}
			items = append(items, map[string]any{
				"member": map[string]any{
					"bk_tenant_id": physical["bk_tenant_id"], "observation_domain_id": physical["observation_domain_id"],
					"data_id": physical["data_id"], "bk_biz_id": physical["bk_biz_id"], "app_id": appID,
				},
				"trace_id": traceID, "span_id": spanID, "values": values,
			})
			continue
		}
		for _, countField := range []string{"span_count", "error_count", "service_count", "hierarchy_count"} {
			if physical[countField].(int64) < 0 {
				return nil, fmt.Errorf("Trace 第 %d 行计数 %s 非法", index, countField)
			}
		}
		for field, value := range physical {
			if field != "trace_id" {
				values[field] = value
			}
		}
		items = append(items, map[string]any{"app_id": appID, "trace_id": traceID, "values": values})
	}
	return items, nil
}

func normalizeTracePhysicalField(profile metadata.TraceProfile, field string, value any) (any, error) {
	if value == nil {
		// Doris VARIANT stores absent Span events/links as JSON null. In the
		// response contract those collections are empty arrays.
		if profile == metadata.TraceProfileSpan && (field == "events" || field == "links") {
			return []any{}, nil
		}
		if profile == metadata.TraceProfileSpan && field == "event_time_ms" {
			return nil, nil
		}
		if _, nullable := traceNullableFields[field]; nullable {
			return nil, nil
		}
		return nil, fmt.Errorf("非空列为 null")
	}
	if _, integer := traceIntegerFields[field]; integer {
		return exactTraceInt64(value)
	}
	switch field {
	case "span_date", "trace_date":
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("日期必须是 UTC 字符串")
		}
		if _, err := time.Parse("2006-01-02", text); err != nil {
			return nil, err
		}
		return text, nil
	case "partial":
		boolean, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("partial 必须是布尔值")
		}
		return boolean, nil
	case "resource", "attributes", "category_statistics", "kind_statistics":
		if _, ok := value.(map[string]any); !ok {
			return nil, fmt.Errorf("JSON 列必须是对象")
		}
		return cloneTraceJSON(value, 0)
	case "events", "links":
		// Doris VARIANT serializes an absent array as an empty JSON object over
		// the MySQL protocol. Accept only that empty sentinel, not arbitrary
		// objects, as an empty collection.
		if empty, ok := value.(map[string]any); ok && len(empty) == 0 {
			return []any{}, nil
		}
		if _, ok := value.([]any); !ok {
			return nil, fmt.Errorf("JSON 列必须是数组")
		}
		return cloneTraceJSON(value, 0)
	case "root_service_status_code":
		switch value.(type) {
		case map[string]any, []any:
			return nil, fmt.Errorf("状态码 JSON 必须是标量")
		}
		return cloneTraceJSON(value, 0)
	default:
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("文本列必须是字符串")
		}
		return text, nil
	}
}

func exactTraceInt64(value any) (int64, error) {
	var text string
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int64:
		return typed, nil
	case json.Number:
		text = typed.String()
	case string:
		text = typed
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed ||
			math.Abs(typed) >= 1<<53 || typed < math.MinInt64 || typed >= -float64(math.MinInt64) {
			return 0, fmt.Errorf("浮点整数不精确或越界")
		}
		return int64(typed), nil
	default:
		return 0, fmt.Errorf("不是精确整数")
	}
	parsed, err := strconv.ParseInt(text, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != text {
		return 0, fmt.Errorf("不是规范 int64")
	}
	return parsed, nil
}

func cloneTraceJSON(value any, depth int) (any, error) {
	if depth > 64 {
		return nil, fmt.Errorf("JSON 嵌套过深")
	}
	switch typed := value.(type) {
	case nil, string, bool, int, int64:
		return typed, nil
	case json.Number:
		if _, err := json.Marshal(typed); err != nil {
			return nil, fmt.Errorf("JSON 数字非法: %w", err)
		}
		return typed, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Abs(typed) >= 1<<53 {
			return nil, fmt.Errorf("JSON 浮点数不精确或非有限")
		}
		return typed, nil
	case map[string]any:
		clone := make(map[string]any, len(typed))
		for key, item := range typed {
			if key == "" {
				return nil, fmt.Errorf("JSON 字段名为空")
			}
			value, err := cloneTraceJSON(item, depth+1)
			if err != nil {
				return nil, err
			}
			clone[key] = value
		}
		return clone, nil
	case []any:
		clone := make([]any, len(typed))
		for index, item := range typed {
			value, err := cloneTraceJSON(item, depth+1)
			if err != nil {
				return nil, err
			}
			clone[index] = value
		}
		return clone, nil
	default:
		return nil, fmt.Errorf("JSON 值类型不受支持")
	}
}
