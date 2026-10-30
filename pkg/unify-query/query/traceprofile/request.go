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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

const contractVersion = "trace_query_v2"
const aggregateContractVersion = "trace_query_v3"

// Trace 数据不会早于 2000 年；该下界使误传的秒/毫秒 epoch 明确失败。
const minTraceEpochUS int64 = 946684800000000

var (
	fingerprintPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	fieldPattern       = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]*$`)
	orderPattern       = regexp.MustCompile(`^-?[A-Za-z_][A-Za-z0-9_.]*$`)
)

type FilterField struct {
	FieldName  string   `json:"field_name"`
	Value      []string `json:"value"`
	Operator   string   `json:"op"`
	IsWildcard bool     `json:"is_wildcard,omitempty"`
	IsPrefix   bool     `json:"is_prefix,omitempty"`
	IsSuffix   bool     `json:"is_suffix,omitempty"`
}

type Filters struct {
	FieldList     []FilterField `json:"field_list"`
	ConditionList []string      `json:"condition_list"`
}

// Request contains only logical query fields. Physical coordinates and SQL are
// absent from the wire type and rejected during strict JSON decoding.
type Request struct {
	Profile          metadata.TraceProfile
	ScopeFingerprint string
	StartUS          int64
	EndUS            int64
	Filters          Filters
	OrderBy          []string
	Limit            int64
	Offset           *int64
	Aggregation      *CountAggregation
}

// CountAggregation is a bounded, server-side count query over one trusted Trace profile.
type CountAggregation struct {
	Method                string   `json:"method"`
	IntervalSeconds       int64    `json:"interval_seconds"`
	TimezoneOffsetSeconds int64    `json:"timezone_offset_seconds"`
	GroupBy               []string `json:"group_by"`
}

type wireRequest struct {
	ExpectedScopeFingerprint string          `json:"expected_scope_fingerprint"`
	Scene                    json.RawMessage `json:"scene,omitempty"`
	StartTime                string          `json:"start_time"`
	EndTime                  string          `json:"end_time"`
	Filters                  *Filters        `json:"filters"`
	OrderBy                  []string        `json:"order_by"`
	Limit                    int64           `json:"limit"`
	Offset                   json.RawMessage `json:"offset,omitempty"`
	PageToken                json.RawMessage `json:"page_token,omitempty"`
	Aggregation              json.RawMessage `json:"aggregation,omitempty"`
}

type wireEnvelope struct {
	TraceProfile         metadata.TraceProfile `json:"trace_profile"`
	TraceContractVersion string                `json:"trace_contract_version"`
	TraceRequest         *wireRequest          `json:"trace_request"`
}

func ParseRequest(body []byte, maxBodyBytes int, maxLimit int64) (Request, error) {
	if maxBodyBytes <= 0 || maxLimit <= 0 || len(body) == 0 || len(body) > maxBodyBytes {
		return Request{}, fmt.Errorf("Trace profile 请求超过预算或配置无效")
	}
	if err := rejectDuplicateKeys(body); err != nil {
		return Request{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var wire wireEnvelope
	if err := decoder.Decode(&wire); err != nil {
		return Request{}, fmt.Errorf("Trace profile 请求不是闭合 JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Request{}, fmt.Errorf("Trace profile 请求包含多余 JSON 内容")
	}
	if wire.TraceRequest == nil ||
		wire.TraceContractVersion != contractVersion && wire.TraceContractVersion != aggregateContractVersion {
		return Request{}, fmt.Errorf("Trace profile 合同版本或请求体不受支持")
	}
	if wire.TraceProfile != metadata.TraceProfileSpan {
		return Request{}, fmt.Errorf("Trace profile 类型不受支持")
	}
	query := wire.TraceRequest
	if wire.TraceContractVersion == contractVersion && len(query.Aggregation) != 0 ||
		wire.TraceContractVersion == aggregateContractVersion && len(query.Aggregation) == 0 {
		return Request{}, fmt.Errorf("Trace profile 聚合与合同版本不匹配")
	}
	if len(query.Scene) != 0 {
		return Request{}, fmt.Errorf("Span profile 不支持场景")
	}
	var offset *int64
	if len(query.Offset) != 0 {
		if bytes.Equal(query.Offset, []byte("null")) {
			return Request{}, fmt.Errorf("Trace profile offset 不得为 null")
		}
		var value int64
		if err := json.Unmarshal(query.Offset, &value); err != nil {
			return Request{}, fmt.Errorf("Trace profile offset 必须是整数: %w", err)
		}
		offset = &value
	}
	if !fingerprintPattern.MatchString(query.ExpectedScopeFingerprint) || query.Filters == nil || query.OrderBy == nil {
		return Request{}, fmt.Errorf("Trace profile 缺少受信范围或查询字段")
	}
	if len(query.PageToken) != 0 || query.Limit <= 0 || query.Limit > maxLimit ||
		offset != nil && (*offset < 0 || *offset > (1<<63-1)-query.Limit) {
		return Request{}, fmt.Errorf("Trace profile 分页参数非法或超预算")
	}
	startUS, err := parseMicroseconds(query.StartTime)
	if err != nil {
		return Request{}, fmt.Errorf("Trace profile start_time 非法: %w", err)
	}
	endUS, err := parseMicroseconds(query.EndTime)
	if err != nil || endUS <= startUS {
		return Request{}, fmt.Errorf("Trace profile 时间范围非法")
	}
	if err := validateFilters(*query.Filters); err != nil {
		return Request{}, err
	}
	var aggregation *CountAggregation
	if len(query.Aggregation) != 0 {
		if bytes.Equal(query.Aggregation, []byte("null")) || offset != nil || len(query.OrderBy) != 0 {
			return Request{}, fmt.Errorf("Trace 聚合不能混用分页或排序")
		}
		aggregationDecoder := json.NewDecoder(bytes.NewReader(query.Aggregation))
		aggregationDecoder.DisallowUnknownFields()
		var value CountAggregation
		if err := aggregationDecoder.Decode(&value); err != nil {
			return Request{}, fmt.Errorf("Trace 聚合形状非法: %w", err)
		}
		if err := aggregationDecoder.Decode(&extra); err != io.EOF {
			return Request{}, fmt.Errorf("Trace 聚合包含多余内容")
		}
		if value.Method == "task_summary" {
			closed := json.NewDecoder(bytes.NewReader(query.Aggregation))
			closed.DisallowUnknownFields()
			if err := closed.Decode(new(struct {
				Method string `json:"method"`
			})); err != nil {
				return Request{}, fmt.Errorf("后台任务聚合只接受 method 字段: %w", err)
			}
			if query.Limit > 5000 ||
				value.IntervalSeconds != 0 || value.TimezoneOffsetSeconds != 0 || value.GroupBy != nil ||
				!validTaskSummaryFilters(*query.Filters) {
				return Request{}, fmt.Errorf("后台任务聚合范围或字段超出闭合合同")
			}
		} else if value.Method != "count" || value.IntervalSeconds <= 0 || value.IntervalSeconds > 86400 ||
			value.TimezoneOffsetSeconds < -14*3600 || value.TimezoneOffsetSeconds > 14*3600 ||
			value.GroupBy == nil || len(value.GroupBy) > 5 {
			return Request{}, fmt.Errorf("Trace 聚合方法、周期、时区或分组超出合同")
		}
		if value.Method == "count" {
			seen := make(map[string]struct{}, len(value.GroupBy))
			for _, field := range value.GroupBy {
				if !fieldPattern.MatchString(field) {
					return Request{}, fmt.Errorf("Trace 聚合分组字段非法")
				}
				if _, duplicate := seen[field]; duplicate {
					return Request{}, fmt.Errorf("Trace 聚合分组字段重复")
				}
				seen[field] = struct{}{}
			}
		}
		aggregation = &value
	}
	for _, order := range query.OrderBy {
		if !orderPattern.MatchString(order) {
			return Request{}, fmt.Errorf("Trace profile 排序字段非法")
		}
	}
	return Request{
		Profile: wire.TraceProfile, ScopeFingerprint: query.ExpectedScopeFingerprint,
		StartUS: startUS, EndUS: endUS, Filters: *query.Filters,
		OrderBy: append([]string(nil), query.OrderBy...), Limit: query.Limit, Offset: offset,
		Aggregation: aggregation,
	}, nil
}

func parseMicroseconds(value string) (int64, error) {
	if value == "" || strings.Trim(value, "0123456789") != "" {
		return 0, fmt.Errorf("必须是微秒整数字符串")
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < minTraceEpochUS || strconv.FormatInt(parsed, 10) != value {
		return 0, fmt.Errorf("必须是正的 int64 微秒")
	}
	return parsed, nil
}

func validateFilters(filters Filters) error {
	if len(filters.ConditionList) != max(len(filters.FieldList)-1, 0) {
		return fmt.Errorf("Trace profile 过滤条件连接符数量不匹配")
	}
	for _, connector := range filters.ConditionList {
		if connector != "and" && connector != "or" {
			return fmt.Errorf("Trace profile 过滤条件连接符不受支持")
		}
	}
	for _, field := range filters.FieldList {
		if !fieldPattern.MatchString(field.FieldName) {
			return fmt.Errorf("Trace profile 过滤字段非法")
		}
		if field.Value == nil {
			return fmt.Errorf("Trace profile 过滤值必须是数组")
		}
		switch field.Operator {
		case "eq", "ne", "req", "nreq", "contains", "ncontains", "exact", "gt", "gte", "lt", "lte":
			if len(field.Value) == 0 {
				return fmt.Errorf("Trace profile 过滤值不能为空")
			}
		case "existed", "nexisted":
			if len(field.Value) != 0 {
				return fmt.Errorf("Trace profile 存在性过滤不得带值")
			}
		default:
			return fmt.Errorf("Trace profile 过滤操作符不受支持")
		}
		for _, value := range field.Value {
			if strings.ContainsRune(value, 0) {
				return fmt.Errorf("Trace profile 过滤值非法")
			}
		}
	}
	return nil
}

func rejectDuplicateKeys(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := scanJSONValue(decoder, 0); err != nil {
		return fmt.Errorf("Trace profile JSON 键重复或结构非法: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("Trace profile JSON 包含多余内容")
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 64 {
		return fmt.Errorf("JSON 嵌套层数超过上限")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("对象键必须是字符串")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("重复对象键 %q", key)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("意外的 JSON 结束符")
	}
	expected := json.Delim('}')
	if delimiter == '[' {
		expected = ']'
	}
	end, err := decoder.Token()
	if err != nil || end != expected {
		return fmt.Errorf("JSON 容器未正确结束")
	}
	return nil
}
