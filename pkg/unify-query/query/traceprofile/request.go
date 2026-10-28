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

// MemberPair 是 KAPM 授权目录下发的一个应用/业务组合，不允许拆成独立 IN 条件。
type MemberPair struct {
	AppID   int64 `json:"app_id"`
	BkBizID int64 `json:"bk_biz_id"`
}

// MemberLookup 仅用于 Summary 内部精确 Trace ID 成员查询。
type MemberLookup struct {
	Backend              string       `json:"backend,omitempty"`
	TraceID              string       `json:"trace_id"`
	ExpectedRouteVersion int64        `json:"expected_route_version"`
	AllowedPairs         []MemberPair `json:"allowed_pairs"`
}

// ESSummaryList 仅声明受信应用和已发布版本；物理 RT 由 UQ 路由解析。
type ESSummaryList struct {
	Backend              string       `json:"backend"`
	AppID                int64        `json:"app_id"`
	BkBizID              int64        `json:"bk_biz_id"`
	ExpectedRouteVersion int64        `json:"expected_route_version"`
	ParentAllowedPairs   []MemberPair `json:"parent_allowed_pairs,omitempty"`
}

const (
	MemberBackendDoris         = "doris"
	MemberBackendElasticsearch = "elasticsearch"
)

const maxMemberPairs = 128

// Request contains only logical query fields. Physical coordinates and SQL are
// absent from the wire type and rejected during strict JSON decoding.
type Request struct {
	Profile             metadata.TraceProfile
	ScopeFingerprint    string
	ObservationDomainID string
	Scene               string
	StartUS             int64
	EndUS               int64
	Filters             Filters
	OrderBy             []string
	Limit               int64
	Offset              *int64
	MemberLookup        *MemberLookup
	ESSummaryList       *ESSummaryList
	Aggregation         *CountAggregation

	// esSummaryPredicate 是 ParseRequest 解析过的 ES Summary 列表入口筛选，避免重复解析。
	esSummaryPredicate *ESSummaryListPredicate
}

// CountAggregation is a bounded, server-side count query over one trusted Trace profile.
type CountAggregation struct {
	Method                string   `json:"method"`
	IntervalSeconds       int64    `json:"interval_seconds"`
	TimezoneOffsetSeconds int64    `json:"timezone_offset_seconds"`
	GroupBy               []string `json:"group_by"`
}

type wireRequest struct {
	ObservationDomainID      string          `json:"observation_domain_id"`
	ExpectedScopeFingerprint string          `json:"expected_scope_fingerprint"`
	Scene                    json.RawMessage `json:"scene,omitempty"`
	StartTime                string          `json:"start_time"`
	EndTime                  string          `json:"end_time"`
	Filters                  *Filters        `json:"filters"`
	OrderBy                  []string        `json:"order_by"`
	Limit                    int64           `json:"limit"`
	Offset                   json.RawMessage `json:"offset,omitempty"`
	PageToken                json.RawMessage `json:"page_token,omitempty"`
	MemberLookup             json.RawMessage `json:"member_lookup,omitempty"`
	ESSummaryList            json.RawMessage `json:"es_summary_list,omitempty"`
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
	if wire.TraceProfile != metadata.TraceProfileSpan && wire.TraceProfile != metadata.TraceProfileSummary {
		return Request{}, fmt.Errorf("Trace profile 类型不受支持")
	}
	query := wire.TraceRequest
	if wire.TraceContractVersion == contractVersion && len(query.Aggregation) != 0 ||
		wire.TraceContractVersion == aggregateContractVersion && len(query.Aggregation) == 0 {
		return Request{}, fmt.Errorf("Trace profile 聚合与合同版本不匹配")
	}
	scene := ""
	if len(query.Scene) != 0 {
		if bytes.Equal(query.Scene, []byte("null")) || json.Unmarshal(query.Scene, &scene) != nil {
			return Request{}, fmt.Errorf("Trace profile 场景字段非法")
		}
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
	if query.ObservationDomainID == "" || strings.ContainsRune(query.ObservationDomainID, 0) ||
		!fingerprintPattern.MatchString(query.ExpectedScopeFingerprint) || query.Filters == nil || query.OrderBy == nil {
		return Request{}, fmt.Errorf("Trace profile 缺少受信范围或查询字段")
	}
	if wire.TraceProfile == metadata.TraceProfileSummary {
		if scene != "app" && scene != "namespace" && scene != "global" {
			return Request{}, fmt.Errorf("Summary profile 场景不受支持")
		}
	} else if len(query.Scene) != 0 {
		return Request{}, fmt.Errorf("Span profile 不支持场景")
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
		if bytes.Equal(query.Aggregation, []byte("null")) || offset != nil || len(query.OrderBy) != 0 ||
			len(query.MemberLookup) != 0 || len(query.ESSummaryList) != 0 {
			return Request{}, fmt.Errorf("Trace 聚合不能混用分页、排序或成员查询")
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
			if wire.TraceProfile != metadata.TraceProfileSpan || query.Limit > 5000 ||
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
	var memberLookup *MemberLookup
	if len(query.MemberLookup) != 0 {
		if bytes.Equal(query.MemberLookup, []byte("null")) || len(query.ESSummaryList) != 0 ||
			wire.TraceProfile != metadata.TraceProfileSummary ||
			scene != "global" || offset != nil || len(query.Filters.FieldList) != 0 ||
			len(query.Filters.ConditionList) != 0 || len(query.OrderBy) != 0 {
			return Request{}, fmt.Errorf("Summary 成员查询模式不能混用列表条件或分页")
		}
		lookupDecoder := json.NewDecoder(bytes.NewReader(query.MemberLookup))
		lookupDecoder.DisallowUnknownFields()
		var lookup MemberLookup
		if err := lookupDecoder.Decode(&lookup); err != nil {
			return Request{}, fmt.Errorf("Summary 成员查询形状非法: %w", err)
		}
		if err := lookupDecoder.Decode(&extra); err != io.EOF {
			return Request{}, fmt.Errorf("Summary 成员查询包含多余内容")
		}
		if lookup.Backend == "" {
			lookup.Backend = MemberBackendDoris
		}
		if err := validateMemberLookup(lookup); err != nil {
			return Request{}, err
		}
		memberLookup = &lookup
	}
	var esSummaryList *ESSummaryList
	var esSummaryPredicate *ESSummaryListPredicate
	if len(query.ESSummaryList) != 0 {
		if bytes.Equal(query.ESSummaryList, []byte("null")) || memberLookup != nil || aggregation != nil ||
			wire.TraceProfile != metadata.TraceProfileSummary || scene != "global" ||
			!WindowFits(startUS, endUS, MaxWindowUS(MaxRangeDays)) {
			return Request{}, fmt.Errorf("ES Summary 列表模式、场景或三十天上限非法")
		}
		listDecoder := json.NewDecoder(bytes.NewReader(query.ESSummaryList))
		listDecoder.DisallowUnknownFields()
		var target ESSummaryList
		if err := listDecoder.Decode(&target); err != nil {
			return Request{}, fmt.Errorf("ES Summary 列表身份非法: %w", err)
		}
		if err := listDecoder.Decode(&extra); err != io.EOF || target.Backend != MemberBackendElasticsearch ||
			target.AppID <= 0 || target.BkBizID <= 0 || target.ExpectedRouteVersion <= 0 {
			return Request{}, fmt.Errorf("ES Summary 列表缺少单应用和发布版本")
		}
		if !validESSummaryListOrder(query.OrderBy) {
			return Request{}, fmt.Errorf("ES Summary 列表必须按开始时间或耗时与 Trace ID 稳定排序")
		}
		predicate, err := ParseESSummaryListPredicate(*query.Filters)
		if err != nil {
			return Request{}, err
		}
		if err := validateESParentPairs(target, predicate.ParentState); err != nil {
			return Request{}, err
		}
		esSummaryList = &target
		esSummaryPredicate = &predicate
	}
	for _, order := range query.OrderBy {
		if !orderPattern.MatchString(order) {
			return Request{}, fmt.Errorf("Trace profile 排序字段非法")
		}
	}
	return Request{
		Profile: wire.TraceProfile, ScopeFingerprint: query.ExpectedScopeFingerprint,
		ObservationDomainID: query.ObservationDomainID, Scene: scene,
		StartUS: startUS, EndUS: endUS, Filters: *query.Filters,
		OrderBy: append([]string(nil), query.OrderBy...), Limit: query.Limit, Offset: offset,
		MemberLookup:  memberLookup,
		ESSummaryList: esSummaryList,
		Aggregation:   aggregation,

		esSummaryPredicate: esSummaryPredicate,
	}, nil
}

func validESSummaryListOrder(order []string) bool {
	if len(order) != 2 || order[1] != "trace_id" {
		return false
	}
	switch order[0] {
	case "-min_start_time", "min_start_time", "-trace_duration", "trace_duration":
		return true
	default:
		return false
	}
}

func validateESParentPairs(target ESSummaryList, parentState string) error {
	if parentState != "observed" && parentState != "missing" {
		if len(target.ParentAllowedPairs) != 0 {
			return fmt.Errorf("ES Summary 无跨应用父关系条件时不得提交授权应用集合")
		}
		return nil
	}
	if len(target.ParentAllowedPairs) == 0 || len(target.ParentAllowedPairs) > 64 {
		return fmt.Errorf("ES Summary 跨应用父关系缺少有界授权应用集合")
	}
	previous := MemberPair{}
	includesTarget := false
	for index, pair := range target.ParentAllowedPairs {
		if pair.AppID <= 0 || pair.BkBizID <= 0 || index > 0 &&
			(pair.BkBizID < previous.BkBizID || pair.BkBizID == previous.BkBizID && pair.AppID <= previous.AppID) {
			return fmt.Errorf("ES Summary 跨应用父关系授权集合未严格排序或含无效应用")
		}
		includesTarget = includesTarget || pair.AppID == target.AppID && pair.BkBizID == target.BkBizID
		previous = pair
	}
	if !includesTarget {
		return fmt.Errorf("ES Summary 跨应用父关系授权集合缺少目标应用")
	}
	return nil
}

// ESParentOwnerKeys only derives indexed owner terms from a bounded, validated typed request.
func ESParentOwnerKeys(target ESSummaryList, parentState string) ([]string, error) {
	if err := validateESParentPairs(target, parentState); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(target.ParentAllowedPairs))
	for _, pair := range target.ParentAllowedPairs {
		keys = append(keys, fmt.Sprintf("%d:%d", pair.BkBizID, pair.AppID))
	}
	return keys, nil
}

func validateMemberLookup(lookup MemberLookup) error {
	if (lookup.Backend != "" && lookup.Backend != MemberBackendDoris && lookup.Backend != MemberBackendElasticsearch) ||
		lookup.Backend == MemberBackendElasticsearch && len(lookup.AllowedPairs) != 1 ||
		!traceIDPattern.MatchString(lookup.TraceID) || lookup.ExpectedRouteVersion <= 0 ||
		len(lookup.AllowedPairs) == 0 ||
		len(lookup.AllowedPairs) > maxMemberPairs {
		return fmt.Errorf("Summary 成员查询缺少精确 Trace ID 或有界允许集")
	}
	var previous MemberPair
	for index, pair := range lookup.AllowedPairs {
		if pair.AppID <= 0 || pair.BkBizID <= 0 || index > 0 &&
			(pair.BkBizID < previous.BkBizID || pair.BkBizID == previous.BkBizID && pair.AppID <= previous.AppID) {
			return fmt.Errorf("Summary 成员允许集必须按业务/应用严格排序且不重复")
		}
		previous = pair
	}
	return nil
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
