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
	"strconv"
	"strings"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

// Limits 是查询预算；超预算明确失败，不做截断或降级。
type Limits struct {
	// MaxRangeUS 是单次查询允许的最大时间跨度（微秒）。
	MaxRangeUS int64
	// MaxOffset 是 Summary 列表允许的最大偏移量。
	MaxOffset int64
}

const maxOrderTerms = 4

// CompiledList 保留精确成员的预检计数语句，并在同一快照语句中读取 total/页。
type CompiledList struct {
	CountStatement    string
	CountArgs         []any
	SnapshotStatement string
	SnapshotArgs      []any
}

// jsonProjectionColumns 是 profile 内不能直接用于过滤/排序的 JSON/VARIANT 列。
var jsonProjectionColumns = map[metadata.TraceProfile]map[string]struct{}{
	metadata.TraceProfileSpan: {
		"resource": {}, "attributes": {}, "events": {}, "links": {},
	},
	metadata.TraceProfileSummary: {
		"root_service_status_code": {}, "category_statistics": {}, "kind_statistics": {},
	},
}

// identityScopeColumns 只能由受信 transport 供值的列，不接受调用方改写。
var identityScopeColumns = map[string]struct{}{
	"bk_tenant_id": {}, "observation_domain_id": {}, "space_uid": {}, "bk_biz_id": {},
}

// narrowOnlyIdentityColumns 是允许调用方收窄、但不能扩宽的成员列。
var narrowOnlyIdentityColumns = map[string]struct{}{
	"app_id": {}, "data_id": {},
}

func timeColumnForProfile(profile metadata.TraceProfile) string {
	if profile == metadata.TraceProfileSummary {
		return "trace_start_time_us"
	}
	return "start_time_us"
}

func dateColumnForProfile(profile metadata.TraceProfile) string {
	if profile == metadata.TraceProfileSummary {
		return "trace_date"
	}
	return "span_date"
}

// canonicalOrder 给出稳定全序：时间倒序 + 唯一化尾键。
// Span 的同一 trace 有多行，Summary 在没有 app_id 收窄时同一 trace 也可能跨应用成行，
// 因此尾键必须能把行区分开，否则 OFFSET/LIMIT 会出现重读或漏读。
func canonicalOrder(profile metadata.TraceProfile) []orderTerm {
	if profile == metadata.TraceProfileSummary {
		return []orderTerm{
			{column: "trace_start_time_us", desc: true},
			{column: "trace_id"},
			{column: "app_id"},
		}
	}
	return []orderTerm{
		{column: "start_time_us", desc: true},
		{column: "trace_id"},
		{column: "span_id"},
	}
}

type orderTerm struct {
	column string
	desc   bool
}

// CompileList 把 closed typed 请求编译成受控只读 SQL。
// 物理坐标只来自可信读目标，值一律绑定，宏一律由 UQ 生成。
func CompileList(
	profile metadata.TraceProfile, target DorisTarget, scope Scope, request Request, limits Limits,
) (CompiledList, error) {
	if _, supported := profilePhysicalTable[profile]; !supported {
		return CompiledList{}, reject(CodeUnsupportedSchemaCapability, "Trace profile 不受支持: %s", profile)
	}
	expectedTable, err := businessPhysicalTable(profile, scope.BkBizID)
	if err != nil || target.Table != expectedTable {
		return CompiledList{}, reject(CodeRouteNotFound, "Trace 读目标与 profile 不匹配")
	}
	if request.EndUS <= request.StartUS {
		return CompiledList{}, reject(CodeUnboundedRange, "Trace 查询时间范围非法")
	}
	if limits.MaxRangeUS > 0 && request.EndUS-request.StartUS > limits.MaxRangeUS {
		return CompiledList{}, reject(CodeUnboundedRange, "Trace 查询时间范围超过预算")
	}
	offset := int64(0)
	if request.Offset != nil {
		offset = *request.Offset
	}
	if limits.MaxOffset > 0 && offset > limits.MaxOffset {
		return CompiledList{}, reject(CodeLimitExceeded, "Trace 查询偏移量超过预算")
	}
	if request.Limit <= 0 {
		return CompiledList{}, reject(CodeLimitExceeded, "Trace 查询行数预算非法")
	}
	if lookup := request.MemberLookup; lookup != nil {
		if profile != metadata.TraceProfileSummary || request.Scene != "global" || request.Offset != nil ||
			len(request.Filters.FieldList) != 0 || len(request.Filters.ConditionList) != 0 ||
			len(request.OrderBy) != 0 {
			return CompiledList{}, reject(CodePredicateConflict, "Summary 成员查询不能混用列表条件或分页")
		}
		if err := validateMemberLookup(*lookup); err != nil {
			return CompiledList{}, reject(CodePredicateConflict, "Summary 成员查询范围非法: %s", err)
		}
		for _, pair := range lookup.AllowedPairs {
			if pair.BkBizID != scope.BkBizID {
				return CompiledList{}, reject(CodeUnauthorizedMember,
					"Summary 成员业务与受信物理表不一致")
			}
		}
	}

	table := quoteIdentifier(target.Database) + "." + quoteIdentifier(target.Table)
	where, args, err := buildWhere(profile, scope, request)
	if err != nil {
		return CompiledList{}, err
	}
	order, err := buildOrder(profile, request)
	if err != nil {
		return CompiledList{}, err
	}

	projection := make([]string, 0, len(projectionForProfile(profile)))
	for _, column := range projectionForProfile(profile) {
		projection = append(projection, quoteIdentifier(column))
	}
	orderColumns := make([]string, 0, len(order))
	outerOrderColumns := make([]string, 0, len(order))
	for _, term := range order {
		direction := " ASC"
		if term.desc {
			direction = " DESC"
		}
		orderColumns = append(orderColumns, quoteIdentifier(term.column)+direction)
		outerOrderColumns = append(outerOrderColumns, "paged."+quoteIdentifier(term.column)+direction)
	}
	outerProjection := make([]string, 0, len(projection))
	for _, column := range projectionForProfile(profile) {
		outerProjection = append(outerProjection, "paged."+quoteIdentifier(column))
	}

	// LIMIT/OFFSET 使用已校验并有界的整数，不使用占位符：Doris 对 LIMIT 占位符的支持随版本变化，
	// 而这两个值不是可控输入。
	countStatement := fmt.Sprintf("SELECT COUNT(*) AS `total` FROM %s WHERE %s", table, where)
	snapshotStatement := fmt.Sprintf(
		"WITH filtered AS (SELECT %s FROM %s WHERE %s), "+
			"counted AS (SELECT COUNT(*) AS `total` FROM filtered), "+
			"paged AS (SELECT %s FROM filtered ORDER BY %s LIMIT %d OFFSET %d) "+
			"SELECT counted.`total`, %s FROM counted LEFT JOIN paged ON TRUE ORDER BY %s",
		strings.Join(projection, ", "), table, where,
		strings.Join(projection, ", "), strings.Join(orderColumns, ", "), request.Limit, offset,
		strings.Join(outerProjection, ", "), strings.Join(outerOrderColumns, ", "),
	)

	return CompiledList{
		CountStatement:    countStatement,
		CountArgs:         append([]any(nil), args...),
		SnapshotStatement: snapshotStatement,
		SnapshotArgs:      args,
	}, nil
}

func buildWhere(profile metadata.TraceProfile, scope Scope, request Request) (string, []any, error) {
	startDate := utcDate(request.StartUS)
	// end 是排他边界：最后包含的 UTC 日期取 end_us-1。
	lastDate := utcDate(request.EndUS - 1)

	parts := []string{
		quoteIdentifier(dateColumnForProfile(profile)) + " >= ?",
		quoteIdentifier(dateColumnForProfile(profile)) + " <= ?",
		quoteIdentifier(timeColumnForProfile(profile)) + " >= ?",
		quoteIdentifier(timeColumnForProfile(profile)) + " < ?",
	}
	args := []any{startDate, lastDate, request.StartUS, request.EndUS}

	for _, identity := range scope.isolationPredicates(profile) {
		parts = append(parts, quoteIdentifier(identity.column)+" = ?")
		args = append(args, identity.value)
	}
	if lookup := request.MemberLookup; lookup != nil {
		parts = append(parts, "`trace_id` = ?")
		args = append(args, lookup.TraceID)
		pairs := make([]string, 0, len(lookup.AllowedPairs))
		for _, pair := range lookup.AllowedPairs {
			pairs = append(pairs, "(`app_id` = ? AND `bk_biz_id` = ?)")
			args = append(args, strconv.FormatInt(pair.AppID, 10), pair.BkBizID)
		}
		parts = append(parts, "("+strings.Join(pairs, " OR ")+")")
	}

	clientParts := make([]string, 0, len(request.Filters.FieldList))
	clientArgs := make([]any, 0, len(request.Filters.FieldList))
	for _, field := range request.Filters.FieldList {
		if _, ok := identityScopeColumns[field.FieldName]; ok {
			if err := assertTrustedIdentityFilter(scope, field); err != nil {
				return "", nil, err
			}
			// 同值条件按原样编译而非丢弃：受信隔离条件已单独注入，多一条等价等值条件不影响
			// 语义，但丢弃会让 condition_list 与保留下来的条件错位（"true OR X" 会被收窄成 X）。
		}
		statement, statementArgs, err := compileFilterField(profile, field)
		if err != nil {
			return "", nil, err
		}
		if statement == "" {
			continue
		}
		clientParts = append(clientParts, statement)
		clientArgs = append(clientArgs, statementArgs...)
	}
	if len(clientParts) > 0 {
		parts = append(parts, joinPredicates(clientParts, request.Filters.ConditionList))
		args = append(args, clientArgs...)
	}
	return strings.Join(parts, " AND "), args, nil
}

// joinPredicates 按左到右显式加括号组合条件，不依赖引擎的 AND/OR 优先级。
func joinPredicates(parts []string, connectors []string) string {
	expression := parts[0]
	for index, connector := range connectors {
		if index+1 >= len(parts) {
			break
		}
		keyword := " AND "
		if connector == "or" {
			keyword = " OR "
		}
		expression = "(" + expression + keyword + parts[index+1] + ")"
	}
	if len(parts) == 1 {
		return "(" + expression + ")"
	}
	return expression
}

func assertTrustedIdentityFilter(scope Scope, field FilterField) error {
	expected, trusted := scope.trustedIdentityValue(field.FieldName)
	if field.Operator != "eq" || len(field.Value) != 1 {
		return reject(CodePredicateConflict, "Trace 受信作用域列 %s 只允许单值精确匹配", field.FieldName)
	}
	if !trusted {
		return reject(CodePredicateConflict, "Trace 受信作用域列 %s 不接受调用方条件", field.FieldName)
	}
	if formatFilterValue(expected) != field.Value[0] {
		return reject(CodePredicateConflict, "Trace 受信作用域列 %s 与请求范围冲突", field.FieldName)
	}
	return nil
}

func compileFilterField(profile metadata.TraceProfile, field FilterField) (string, []any, error) {
	projection := projectionForProfile(profile)
	supported := false
	for _, column := range projection {
		if column == field.FieldName {
			supported = true
			break
		}
	}
	if !supported {
		return "", nil, reject(
			CodeUnsupportedSchemaCapability, "Trace 过滤字段不在 %s 投影内: %s", profile, field.FieldName,
		)
	}
	if _, json := jsonProjectionColumns[profile][field.FieldName]; json {
		return "", nil, reject(
			CodeUnsupportedSchemaCapability,
			"Trace JSON/VARIANT 列过滤需要半连接能力，尚未交付: %s", field.FieldName,
		)
	}
	if _, narrowOnly := narrowOnlyIdentityColumns[field.FieldName]; narrowOnly {
		if field.Operator != "eq" {
			return "", nil, reject(
				CodePredicateConflict, "Trace 成员列 %s 只允许精确匹配", field.FieldName,
			)
		}
	}
	column := quoteIdentifier(field.FieldName)

	switch field.Operator {
	case "eq", "exact", "ne":
		values, err := bindValues(field)
		if err != nil {
			return "", nil, err
		}
		if len(values) == 1 {
			operator := " = ?"
			if field.Operator == "ne" {
				operator = " != ?"
			}
			return column + operator, values, nil
		}
		operator := " IN ("
		if field.Operator == "ne" {
			operator = " NOT IN ("
		}
		return column + operator + placeholders(len(values)) + ")", values, nil
	case "contains", "ncontains":
		patterns := make([]string, 0, len(field.Value))
		args := make([]any, 0, len(field.Value))
		for _, value := range field.Value {
			patterns = append(patterns, column+" LIKE ?")
			args = append(args, likePattern(value, field.IsWildcard, field.IsPrefix, field.IsSuffix))
		}
		expression := strings.Join(patterns, " OR ")
		if len(patterns) > 1 {
			expression = "(" + expression + ")"
		}
		if field.Operator == "ncontains" {
			return "NOT " + expression, args, nil
		}
		return expression, args, nil
	case "gt", "gte", "lt", "lte":
		if len(field.Value) != 1 {
			return "", nil, reject(CodePredicateConflict, "Trace 比较条件只允许单值: %s", field.FieldName)
		}
		values, err := bindValues(field)
		if err != nil {
			return "", nil, err
		}
		operator := map[string]string{"gt": " > ?", "gte": " >= ?", "lt": " < ?", "lte": " <= ?"}[field.Operator]
		return column + operator, values, nil
	case "existed":
		return column + " IS NOT NULL", nil, nil
	case "nexisted":
		return column + " IS NULL", nil, nil
	default:
		return "", nil, reject(
			CodeUnsupportedSchemaCapability, "Trace 过滤操作符尚未在直连路径交付: %s", field.Operator,
		)
	}
}

func bindValues(field FilterField) ([]any, error) {
	values := make([]any, 0, len(field.Value))
	for _, value := range field.Value {
		bound, err := bindValue(field.FieldName, value)
		if err != nil {
			return nil, reject(CodePredicateConflict, "Trace 过滤值非法: %s", field.FieldName)
		}
		values = append(values, bound)
	}
	return values, nil
}

func bindValue(column, value string) (any, error) {
	if column == "partial" {
		return exactTraceBool(value)
	}
	if _, integer := traceIntegerFields[column]; integer {
		return exactTraceInt64(value)
	}
	return value, nil
}

func exactTraceBool(value string) (bool, error) {
	switch value {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("不是布尔值")
	}
}

func likePattern(value string, isWildcard, isPrefix, isSuffix bool) string {
	pattern := value
	if !isWildcard {
		replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
		pattern = replacer.Replace(value)
	}
	if !isPrefix {
		pattern = "%" + pattern
	}
	if !isSuffix {
		pattern = pattern + "%"
	}
	return pattern
}

func buildOrder(profile metadata.TraceProfile, request Request) ([]orderTerm, error) {
	if len(request.OrderBy) > maxOrderTerms {
		return nil, reject(CodeLimitExceeded, "Trace 排序字段数量超过预算")
	}
	order := make([]orderTerm, 0, len(request.OrderBy)+3)
	seen := make(map[string]struct{}, len(request.OrderBy)+3)
	for _, raw := range request.OrderBy {
		term := orderTerm{column: raw, desc: false}
		if strings.HasPrefix(raw, "-") {
			term = orderTerm{column: raw[1:], desc: true}
		}
		if err := assertSortableColumn(profile, term.column); err != nil {
			return nil, err
		}
		if _, duplicated := seen[term.column]; duplicated {
			continue
		}
		seen[term.column] = struct{}{}
		order = append(order, term)
	}
	// 收敛到稳定全序：调用方给出的顺序在前，随后补齐时间倒序与唯一化尾键。
	for _, term := range canonicalOrder(profile) {
		if _, present := seen[term.column]; present {
			continue
		}
		seen[term.column] = struct{}{}
		order = append(order, term)
	}
	return order, nil
}

func assertSortableColumn(profile metadata.TraceProfile, column string) error {
	for _, projected := range projectionForProfile(profile) {
		if projected != column {
			continue
		}
		if _, json := jsonProjectionColumns[profile][column]; json {
			return reject(
				CodeUnsupportedSchemaCapability, "Trace JSON/VARIANT 列排序尚未交付: %s", column,
			)
		}
		return nil
	}
	return reject(CodeUnsupportedSchemaCapability, "Trace 排序字段不在 %s 投影内: %s", profile, column)
}

func placeholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", count), ", ")
}

func quoteIdentifier(identifier string) string {
	return "`" + identifier + "`"
}

func utcDate(microseconds int64) string {
	return time.UnixMicro(microseconds).UTC().Format("2006-01-02")
}

func formatFilterValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case int64:
		return strconv.FormatInt(typed, 10)
	default:
		return fmt.Sprintf("%v", typed)
	}
}
