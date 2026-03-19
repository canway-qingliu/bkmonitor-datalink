// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package datadog

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ======== OTLP 兼容的属性定义 ========

// AnyValue 代表 OTLP 中的多态值对象
// 根据 OTLP 规范，同一时刻只有一个字段有值
type AnyValue struct {
	StringValue string        `json:"stringValue,omitempty"`
	IntValue    int64         `json:"intValue,omitempty"`
	DoubleValue float64       `json:"doubleValue,omitempty"`
	BoolValue   bool          `json:"boolValue,omitempty"`
	ArrayValue  *ArrayValue   `json:"arrayValue,omitempty"`
	KvlistValue *KeyValueList `json:"kvlistValue,omitempty"`
}

// ArrayValue 代表数组值
type ArrayValue struct {
	Values []AnyValue `json:"values"`
}

// KeyValueList 代表键值对列表
type KeyValueList struct {
	Values []KeyValue `json:"values"`
}

// KeyValue 代表 OTLP 中的键值对
type KeyValue struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

// toAnyValue 将 Go 值转换为 AnyValue
func toAnyValue(value interface{}) AnyValue {
	if value == nil {
		return AnyValue{}
	}

	switch v := value.(type) {
	case string:
		return AnyValue{StringValue: v}
	case int:
		return AnyValue{IntValue: int64(v)}
	case int32:
		return AnyValue{IntValue: int64(v)}
	case int64:
		return AnyValue{IntValue: v}
	case uint:
		return AnyValue{IntValue: int64(v)}
	case uint32:
		return AnyValue{IntValue: int64(v)}
	case uint64:
		return AnyValue{StringValue: strconv.FormatUint(v, 10)}
	case float32:
		return AnyValue{DoubleValue: float64(v)}
	case float64:
		return AnyValue{DoubleValue: v}
	case bool:
		return AnyValue{BoolValue: v}
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return AnyValue{DoubleValue: f}
		}
		return AnyValue{StringValue: v.String()}
	case []interface{}:
		arr := &ArrayValue{Values: make([]AnyValue, len(v))}
		for i, item := range v {
			arr.Values[i] = toAnyValue(item)
		}
		return AnyValue{ArrayValue: arr}
	case map[string]interface{}:
		kvlist := &KeyValueList{Values: make([]KeyValue, 0, len(v))}
		for k, val := range v {
			kvlist.Values = append(kvlist.Values, KeyValue{
				Key:   k,
				Value: toAnyValue(val),
			})
		}
		return AnyValue{KvlistValue: kvlist}
	default:
		// 默认转换为字符串
		return AnyValue{StringValue: fmt.Sprintf("%v", value)}
	}
}

// toKeyValueSlice 将 map[string]interface{} 转换为 KeyValue 数组
func toKeyValueSlice(attrs map[string]interface{}) []KeyValue {
	if attrs == nil {
		return []KeyValue{}
	}
	kvs := make([]KeyValue, 0, len(attrs))
	for key, value := range attrs {
		kvs = append(kvs, KeyValue{
			Key:   key,
			Value: toAnyValue(value),
		})
	}
	return kvs
}

// ======== Conversion Strategy Pattern ========

// ConversionStrategy 定义事件转换策略
type ConversionStrategy interface {
	CanHandle(event RUMEventV2) bool
	Convert(event RUMEventV2, converter *OtelConverter) ConversionResult
}

// baseStrategy 基础转换策略
type baseStrategy struct {
	eventType string
}

func (s *baseStrategy) CanHandle(event RUMEventV2) bool {
	return event.Type == s.eventType
}

// errorEventStrategy 错误事件转换策略
type errorEventStrategy struct {
	baseStrategy
}

func (s *errorEventStrategy) Convert(event RUMEventV2, converter *OtelConverter) ConversionResult {
	return ConversionResult{
		Logs:    converter.convertToLogsInternal(event, true),
		Traces:  converter.convertExceptionToTraces(event),
		Metrics: OtelMetricsData{},
	}
}

// performanceEventStrategy 性能事件转换策略
type performanceEventStrategy struct {
	baseStrategy
}

func (s *performanceEventStrategy) Convert(event RUMEventV2, converter *OtelConverter) ConversionResult {
	return ConversionResult{
		Logs:    converter.convertToLogsInternal(event, false),
		Traces:  converter.convertPerformanceToTraces(event),
		Metrics: converter.convertToMetricsInternal(event),
	}
}

// simpleEventStrategy 简单事件转换策略（view、action、log）
type simpleEventStrategy struct {
	baseStrategy
}

func (s *simpleEventStrategy) Convert(event RUMEventV2, converter *OtelConverter) ConversionResult {
	return ConversionResult{
		Logs:    converter.convertToLogsInternal(event, false),
		Traces:  converter.convertSimpleEventToTraces(event),
		Metrics: OtelMetricsData{},
	}
}

// actionEventStrategy action 时间转换器

type actionEventStrategy struct {
	baseStrategy
}

func (s *actionEventStrategy) Convert(event RUMEventV2, converter *OtelConverter) ConversionResult {
	return ConversionResult{
		Traces: converter.convertSimpleEventToTraces(event),
	}
}

// resourceEventStrategy 资源事件转换策略
type resourceEventStrategy struct {
	baseStrategy
}

func (s *resourceEventStrategy) Convert(event RUMEventV2, converter *OtelConverter) ConversionResult {
	return ConversionResult{
		Traces: converter.convertResourceToTraces(event),
	}
}

// longTaskEventStrategy 长任务事件转换策略
type longTaskEventStrategy struct {
	baseStrategy
}

func (s *longTaskEventStrategy) Convert(event RUMEventV2, converter *OtelConverter) ConversionResult {
	return ConversionResult{
		Logs:    converter.convertToLogsInternal(event, false),
		Traces:  converter.convertLongTaskToTraces(event),
		Metrics: converter.convertLongTaskToMetrics(event),
	}
}

// ======== Builder Pattern for Data Construction ========

// LogRecordBuilder 日志记录构建器
type LogRecordBuilder struct {
	record OtelLogRecord
	attrs  map[string]interface{}
}

func NewLogRecordBuilder(timestamp int64) *LogRecordBuilder {
	if timestamp == 0 {
		timestamp = time.Now().UnixMilli()
	}
	return &LogRecordBuilder{
		record: OtelLogRecord{
			Timestamp:  timestamp * 1_000_000,
			Attributes: make([]KeyValue, 0, 32),
		},
		attrs: make(map[string]interface{}, 32),
	}
}

func (b *LogRecordBuilder) SetBody(body string) *LogRecordBuilder {
	b.record.Body = body
	return b
}

func (b *LogRecordBuilder) SetSeverity(level string) *LogRecordBuilder {
	b.record.SeverityText = level
	b.record.SeverityNumber = mapSeverityLevel(level)
	return b
}

func (b *LogRecordBuilder) SetTraceInfo(traceID, spanID string) *LogRecordBuilder {
	b.record.TraceID = traceID
	b.record.SpanID = spanID
	return b
}

func (b *LogRecordBuilder) AddAttribute(key string, value interface{}) *LogRecordBuilder {
	b.record.Attributes = append(b.record.Attributes, KeyValue{
		Key:   key,
		Value: toAnyValue(value),
	})
	return b
}

func (b *LogRecordBuilder) AddAttributes(prefix string, data map[string]interface{}) *LogRecordBuilder {
	for key, value := range data {
		if _, isMap := value.(map[string]interface{}); isMap {
			continue
		}
		if _, isList := value.([]interface{}); isList {
			continue
		}
		fullKey := fmt.Sprintf("%s.%s", prefix, key)
		b.record.Attributes = append(b.record.Attributes, KeyValue{
			Key:   fullKey,
			Value: toAnyValue(value),
		})
	}
	return b
}

func (b *LogRecordBuilder) Build() OtelLogRecord {
	return b.record
}

// SpanKind 定义 OpenTelemetry Span 的 Kind
// 参考: https://opentelemetry.io/docs/specs/otel/trace/api/#spankind
const (
	SpanKindUnspecified = 0
	SpanKindInternal    = 1
	SpanKindServer      = 2
	SpanKindClient      = 3
	SpanKindProducer    = 4
	SpanKindConsumer    = 5
)

// SpanBuilder 跨度构建器
type SpanBuilder struct {
	span OtelSpan
}

func NewSpanBuilder(timestamp int64) *SpanBuilder {
	if timestamp == 0 {
		timestamp = time.Now().UnixMilli()
	}
	return &SpanBuilder{
		span: OtelSpan{
			StartTimeUnixNano: timestamp * 1_000_000,
			EndTimeUnixNano:   (timestamp + 1) * 1_000_000,
			Attributes:        make([]KeyValue, 0, 32),
			Status:            make(map[string]interface{}, 2),
			Kind:              SpanKindInternal,
		},
	}
}

func (b *SpanBuilder) SetKind(kind int) *SpanBuilder {
	b.span.Kind = kind
	return b
}

func (b *SpanBuilder) SetName(name string) *SpanBuilder {
	b.span.Name = name
	return b
}

func (b *SpanBuilder) SetTraceInfo(traceID, spanID string) *SpanBuilder {
	b.span.TraceID = traceID
	b.span.SpanID = spanID
	return b
}

func (b *SpanBuilder) SetDuration(durationMs float64) *SpanBuilder {
	b.span.EndTimeUnixNano = b.span.StartTimeUnixNano + int64(durationMs*1_000_000)
	return b
}

func (b *SpanBuilder) SetStatus(code string, description string) *SpanBuilder {
	b.span.Status["code"] = code
	if description != "" {
		b.span.Status["description"] = description
	}
	return b
}

func (b *SpanBuilder) AddAttribute(key string, value interface{}) *SpanBuilder {
	b.span.Attributes = append(b.span.Attributes, KeyValue{
		Key:   key,
		Value: toAnyValue(value),
	})
	return b
}

func (b *SpanBuilder) AddAttributes(prefix string, data map[string]interface{}) *SpanBuilder {
	for key, value := range data {
		if _, isMap := value.(map[string]interface{}); isMap {
			continue
		}
		if _, isList := value.([]interface{}); isList {
			continue
		}
		fullKey := fmt.Sprintf("%s.%s", prefix, key)
		b.span.Attributes = append(b.span.Attributes, KeyValue{
			Key:   fullKey,
			Value: toAnyValue(value),
		})
	}
	return b
}

func (b *SpanBuilder) AddEvent(name string, timeUnixNano int64) *SpanBuilder {
	if b.span.Events == nil {
		b.span.Events = make([]OtelSpanEvent, 0, 8)
	}
	b.span.Events = append(b.span.Events, OtelSpanEvent{
		Name:         name,
		TimeUnixNano: timeUnixNano,
		Attributes:   make([]KeyValue, 0),
	})
	return b
}

func (b *SpanBuilder) Build() OtelSpan {
	if b.span.Status["code"] == nil {
		b.span.Status["code"] = "UNSET"
	}
	return b.span
}

// ======== Object Pool for Performance ========

// 属性池，用于减少内存分配
var attrPoolFactory = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 64)
	},
}

func getAttrMap() map[string]interface{} {
	return attrPoolFactory.Get().(map[string]interface{})
}

func putAttrMap(m map[string]interface{}) {
	for k := range m {
		delete(m, k)
	}
	attrPoolFactory.Put(m)
}

// ======== Main Converter ========

// OtelConverter 优化后的 OTEL 转换器
type OtelConverter struct {
	strategies map[string]ConversionStrategy
	mu         sync.RWMutex
	resource   OtelResource
}

// NewOtelConverter 创建转换器实例
func NewOtelConverter() *OtelConverter {
	converter := &OtelConverter{
		strategies: make(map[string]ConversionStrategy),
		resource:   createOtelResource(),
	}

	// 注册所有策略
	strategies := map[string]ConversionStrategy{
		"error":       &errorEventStrategy{baseStrategy: baseStrategy{"error"}},
		"performance": &performanceEventStrategy{baseStrategy: baseStrategy{"performance"}},
		"view":        &simpleEventStrategy{baseStrategy: baseStrategy{"view"}},
		"action":      &actionEventStrategy{baseStrategy: baseStrategy{"action"}},
		"log":         &simpleEventStrategy{baseStrategy: baseStrategy{"log"}},
		"resource":    &resourceEventStrategy{baseStrategy: baseStrategy{"resource"}},
		"long_task":   &longTaskEventStrategy{baseStrategy: baseStrategy{"long_task"}},
	}

	for eventType, strategy := range strategies {
		converter.strategies[eventType] = strategy
	}

	return converter
}

// ToOTEL 根据事件类型进行转换
func (c *OtelConverter) ToOTEL(event RUMEventV2) ConversionResult {
	c.mu.RLock()
	strategy := c.strategies[event.Type]
	c.mu.RUnlock()

	if strategy != nil {
		return strategy.Convert(event, c)
	}
	return ConversionResult{
		Logs:    c.convertToLogsInternal(event, false),
		Traces:  OtelTracesData{},
		Metrics: OtelMetricsData{},
	}
}

// convertToLogsInternal 内部日志转换方法
func (c *OtelConverter) convertToLogsInternal(event RUMEventV2, isError bool) OtelLogsData {
	builder := NewLogRecordBuilder(event.Date)

	// 根据事件类型提取消息和级别
	message := ""
	severity := "INFO"

	// 根据事件类型从相应字段提取，如果专用字段为空则从 Data 提取
	var dataSource map[string]interface{}
	switch event.Type {
	case "error":
		if event.Error != nil {
			dataSource = event.Error
		} else {
			dataSource = event.Data
		}
	case "action":
		if event.Action != nil {
			dataSource = event.Action
		} else {
			dataSource = event.Data
		}
	case "view":
		if event.View != nil {
			dataSource = event.View
		} else {
			dataSource = event.Data
		}
	case "long_task":
		if event.LongTask != nil {
			dataSource = event.LongTask
		} else {
			dataSource = event.Data
		}
	case "resource":
		if event.Resource != nil {
			dataSource = event.Resource
		} else {
			dataSource = event.Data
		}
	default:
		// 其他类型，尝试从 Data 字段
		dataSource = event.Data
	}

	if dataSource != nil {
		if msg, ok := dataSource["message"].(string); ok {
			message = msg
		}
		if level, ok := dataSource["level"].(string); ok {
			severity = level
		}
	}

	// 如果是错误事件，强制设置严重级别
	if isError {
		severity = "ERROR"
	}

	// 生成 Trace ID
	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)

	// 构建日志记录
	builder.SetBody(message).
		SetSeverity(severity).
		SetTraceInfo(traceID, spanID).
		AddAttribute("event.type", event.Type).
		AddAttribute("event.domain", event.EventType)

	// 添加事件数据（从相应的专用字段）
	if dataSource != nil {
		builder.AddAttributes("rum", dataSource)
	}
	if event.Meta != nil {
		builder.AddAttributes("telemetry.sdk", event.Meta)
	}
	if event.Context != nil {
		builder.AddAttributes("telemetry.sdk.context", event.Context)
	}

	record := builder.Build()

	return OtelLogsData{
		ResourceLogs: []OtelResourceLogs{
			{
				Resource: c.resource,
				ScopeLogs: []OtelScopeLogs{
					{
						LogRecords: []OtelLogRecord{record},
					},
				},
			},
		},
	}
}

// convertSimpleEventToTraces 简单事件（view, action, log）转换为 Trace
// view: page.view, action: ui.action, log: 使用 log 类型
func (c *OtelConverter) convertSimpleEventToTraces(event RUMEventV2) OtelTracesData {
	// 确定 Span Name 和 Kind
	spanName := c.getSpanNameForEvent(event)
	kind := SpanKindInternal

	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)

	builder := NewSpanBuilder(event.Date).
		SetName(spanName).
		SetTraceInfo(traceID, spanID).
		SetKind(kind).
		AddAttribute("event.type", event.Type).
		AddAttribute("event.type", event.Type).
		AddAttribute("event.domain", event.EventType)

	// 根据具体类型添加属性
	switch event.Type {
	case "view":
		c.addViewAttributes(builder, event)
	case "action":
		c.addActionAttributes(builder, event)
	}

	span := builder.Build()

	return OtelTracesData{
		ResourceSpans: []OtelResourceSpans{
			{
				Resource: c.resource,
				ScopeSpans: []OtelScopeSpans{
					{
						Spans: []OtelSpan{span},
					},
				},
			},
		},
	}
}

// convertExceptionToTraces error 事件转换为异常 Trace
// exception 类型的 Span，包含异常详情
func (c *OtelConverter) convertExceptionToTraces(event RUMEventV2) OtelTracesData {
	spanName := "exception"
	kind := SpanKindInternal

	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)

	builder := NewSpanBuilder(event.Date).
		SetName(spanName).
		SetTraceInfo(traceID, spanID).
		SetKind(kind).
		AddAttribute("event.type", event.Type).
		AddAttribute("event.domain", event.EventType)

	// 处理错误状态和异常属性（优先使用专用字段，否则从 Data 提取）
	errorMsg := ""
	errorData := event.Error
	if errorData == nil && event.Data != nil {
		if e, ok := event.Data["error"].(map[string]interface{}); ok {
			errorData = e
		} else {
			errorData = event.Data
		}
	}
	if errorData != nil {
		if msg, ok := errorData["message"].(string); ok {
			errorMsg = msg
		}
		c.addExceptionAttributes(builder, event)
	}

	builder.SetStatus("ERROR", errorMsg)

	span := builder.Build()

	return OtelTracesData{
		ResourceSpans: []OtelResourceSpans{
			{
				Resource: c.resource,
				ScopeSpans: []OtelScopeSpans{
					{
						Spans: []OtelSpan{span},
					},
				},
			},
		},
	}
}

// convertPerformanceToTraces performance 事件转换为 Trace
func (c *OtelConverter) convertPerformanceToTraces(event RUMEventV2) OtelTracesData {
	spanName := "resource.load"
	kind := SpanKindInternal

	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)

	builder := NewSpanBuilder(event.Date).
		SetName(spanName).
		SetTraceInfo(traceID, spanID).
		SetKind(kind).
		AddAttribute("event.type", event.Type).
		AddAttribute("event.domain", event.EventType)

	// 处理性能数据（兼容 Data 字段）
	var resourceData map[string]interface{}
	if event.Data != nil {
		resourceData, _ = event.Data["resource"].(map[string]interface{})
	}
	if resourceData != nil {
		if duration, ok := resourceData["duration"].(float64); ok {
			builder.SetDuration(duration).
				AddAttribute("http.duration_ms", duration)
		}
		if size, ok := resourceData["size"].(float64); ok {
			builder.AddAttribute("http.response_content_length", int64(size))
		}
		if name, ok := resourceData["name"].(string); ok {
			builder.AddAttribute("http.url", name)
		}
	}

	span := builder.Build()

	return OtelTracesData{
		ResourceSpans: []OtelResourceSpans{
			{
				Resource: c.resource,
				ScopeSpans: []OtelScopeSpans{
					{
						Spans: []OtelSpan{span},
					},
				},
			},
		},
	}
}

// convertResourceToTraces resource 事件转换为 Trace
// resource 类型表示 HTTP 请求或资源加载
func (c *OtelConverter) convertResourceToTraces(event RUMEventV2) OtelTracesData {
	spanName := c.getResourceSpanName(event)
	kind := SpanKindClient // CLIENT 类型

	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)

	builder := NewSpanBuilder(event.Date).
		SetName(spanName).
		SetTraceInfo(traceID, spanID).
		SetKind(kind).
		AddAttribute("event.type", event.Type)

	// 添加上下文信息
	if event.View != nil {
		if viewID, ok := event.View["id"].(string); ok {
			builder.AddAttribute("view.id", viewID)
		}
		if viewURL, ok := event.View["url"].(string); ok {
			builder.AddAttribute("view.url", viewURL)
		}
		if referrer, ok := event.View["referrer"].(string); ok {
			builder.AddAttribute("view.referrer", referrer)
		}
	}

	// 添加网络连接信息
	if event.Connectivity != nil {
		if status, ok := event.Connectivity["status"].(string); ok {
			builder.AddAttribute("connectivity.status", status)
		}
		if effectiveType, ok := event.Connectivity["effective_type"].(string); ok {
			builder.AddAttribute("connectivity.effective_type", effectiveType)
		}
	}

	// 从 event.Resource 提取资源信息，如果为空则从 Data 中提取
	resourceData := event.Resource
	if resourceData == nil && event.Data != nil {
		// 尝试从 Data 中获取 resource 子对象
		if res, ok := event.Data["resource"].(map[string]interface{}); ok {
			resourceData = res
		} else {
			// 如果没有 resource 子对象，直接使用 Data
			resourceData = event.Data
		}
	}

	// 处理资源属性
	if resourceData != nil {
		// URL
		if resourceURL := extractURLFromResource(resourceData); resourceURL != "" {
			builder.AddAttribute("http.url", resourceURL)
		}

		// 状态码
		if statusCode, ok := resourceData["status_code"].(float64); ok {
			builder.AddAttribute("http.status_code", int(statusCode))
		}

		// 资源类型
		if resType, ok := resourceData["type"].(string); ok {
			builder.AddAttribute("resource.type", resType)
		}

		// 协议
		if protocol, ok := resourceData["protocol"].(string); ok {
			builder.AddAttribute("http.protocol", protocol)
		}

		// 响应内容长度
		transferSize := extractTransferSize(resourceData)
		if transferSize > 0 {
			builder.AddAttribute("http.response_content_length", int(transferSize))
		}

		// Duration（自动单位转换）
		if duration, ok := resourceData["duration"].(float64); ok {
			durationMs := duration
			if duration > 1000000 {
				durationMs = duration / 1_000_000 // 纳秒转毫秒
			}
			builder.SetDuration(durationMs).
				AddAttribute("http.duration_ms", durationMs)
		}

		// 添加资源加载时间点事件
		c.addResourceTimingEvents(builder, event)

		// 添加所有资源属性（带 rum.resource 前缀）
		for key, value := range resourceData {
			if _, isMap := value.(map[string]interface{}); !isMap {
				if _, isList := value.([]interface{}); !isList {
					builder.AddAttribute(fmt.Sprintf("rum.resource.%s", key), value)
				}
			}
		}
	}

	span := builder.Build()

	// 使用增强的 Resource（包含服务名、版本、会话信息等）
	resource := createEnrichedOtelResource(event)

	return OtelTracesData{
		ResourceSpans: []OtelResourceSpans{
			{
				Resource: resource,
				ScopeSpans: []OtelScopeSpans{
					{
						Spans: []OtelSpan{span},
					},
				},
			},
		},
	}
}

// extractURLFromResource 从资源对象提取 URL
func extractURLFromResource(resourceData map[string]interface{}) string {
	if url, ok := resourceData["url"].(string); ok {
		return url
	}
	if name, ok := resourceData["name"].(string); ok {
		return name
	}
	return ""
}

// extractTransferSize 从资源对象提取传输大小
func extractTransferSize(resourceData map[string]interface{}) int64 {
	if ts, ok := resourceData["transfer_size"].(float64); ok && ts > 0 {
		return int64(ts)
	}
	if ds, ok := resourceData["decoded_body_size"].(float64); ok && ds > 0 {
		return int64(ds)
	}
	if size, ok := resourceData["size"].(float64); ok && size > 0 {
		return int64(size)
	}
	return 0
}

// addResourceTimingEvents 添加资源加载时间点事件
// 根据 Resource Timing API 规范添加各个阶段的时间点
func (c *OtelConverter) addResourceTimingEvents(builder *SpanBuilder, event RUMEventV2) {
	// 从 event.Resource 提取，如果为空则从 Data 中提取
	resourceData := event.Resource
	if resourceData == nil && event.Data != nil {
		// 尝试从 Data 中获取 resource 子对象
		if res, ok := event.Data["resource"].(map[string]interface{}); ok {
			resourceData = res
		} else {
			resourceData = event.Data
		}
	}

	if resourceData == nil {
		return
	}

	// 基准时间：span 的开始时间（毫秒）
	baseTimeMs := event.Date
	baseTimeNs := baseTimeMs * 1e6 // 转换为纳秒

	// 提取时间点（单位：纳秒相对偏移）
	var (
		fetchStart        int64 = 0
		domainLookupStart int64 = 0
		domainLookupEnd   int64 = 0
		connectStart      int64 = 0
		connectEnd        int64 = 0
		requestStart      int64 = 0
		responseStart     int64 = 0
		responseEnd       int64 = 0
	)

	// 从 download 提取
	if download, ok := resourceData["download"].(map[string]interface{}); ok {
		if start, ok := download["start"].(float64); ok {
			downloadStart := int64(start)
			if duration, ok := download["duration"].(float64); ok {
				responseEnd = downloadStart + int64(duration)
			}
		}
	}

	// 从 first_byte 提取
	if firstByte, ok := resourceData["first_byte"].(map[string]interface{}); ok {
		if start, ok := firstByte["start"].(float64); ok {
			requestStart = int64(start)
			if duration, ok := firstByte["duration"].(float64); ok {
				responseStart = requestStart + int64(duration)
			}
		}
	}

	// 从 connect 提取（如果存在）
	if connect, ok := resourceData["connect"].(map[string]interface{}); ok {
		if start, ok := connect["start"].(float64); ok {
			connectStart = int64(start)
		}
		if duration, ok := connect["duration"].(float64); ok {
			connectEnd = connectStart + int64(duration)
		}
	}

	// 从 dns 提取（如果存在）
	if dns, ok := resourceData["dns"].(map[string]interface{}); ok {
		if start, ok := dns["start"].(float64); ok {
			domainLookupStart = int64(start)
		}
		if duration, ok := dns["duration"].(float64); ok {
			domainLookupEnd = domainLookupStart + int64(duration)
		}
	}

	// 如果没有 DNS/Connect 数据，使用默认值（等同于 fetchStart）
	if domainLookupStart == 0 && domainLookupEnd == 0 && connectStart == 0 && connectEnd == 0 {
		domainLookupStart = fetchStart
		domainLookupEnd = fetchStart
		connectStart = fetchStart
		connectEnd = fetchStart
	}

	// 添加事件（按照 Resource Timing API 顺序）
	events := []struct {
		name   string
		offset int64
	}{
		{"fetchStart", fetchStart},
		{"domainLookupStart", domainLookupStart},
		{"domainLookupEnd", domainLookupEnd},
		{"connectStart", connectStart},
		{"connectEnd", connectEnd},
		{"requestStart", requestStart},
		{"responseStart", responseStart},
		{"responseEnd", responseEnd},
	}

	// 添加所有事件（即使 offset 相同也要添加）
	for _, evt := range events {
		builder.AddEvent(evt.name, baseTimeNs+evt.offset)
	}
}

// convertLongTaskToTraces long_task 事件转换为 Trace
func (c *OtelConverter) convertLongTaskToTraces(event RUMEventV2) OtelTracesData {
	spanName := "browser.long_task"
	kind := SpanKindInternal

	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)

	builder := NewSpanBuilder(event.Date).
		SetName(spanName).
		SetTraceInfo(traceID, spanID).
		SetKind(kind).
		AddAttribute("event.type", event.Type).
		AddAttribute("event.domain", event.EventType)

	// 处理长任务属性（优先使用专用字段，否则从 Data 提取）
	longTaskData := event.LongTask
	if longTaskData == nil && event.Data != nil {
		if lt, ok := event.Data["long_task"].(map[string]interface{}); ok {
			longTaskData = lt
		} else {
			longTaskData = event.Data
		}
	}

	if longTaskData != nil {
		if duration, ok := longTaskData["duration"].(float64); ok {
			builder.SetDuration(duration).
				AddAttribute("longtask.duration", duration)
		}
		if attribution, ok := longTaskData["attribution"].(string); ok {
			builder.AddAttribute("longtask.attribution", attribution)
		}
		builder.AddAttributes("rum", longTaskData)
	}

	span := builder.Build()

	return OtelTracesData{
		ResourceSpans: []OtelResourceSpans{
			{
				Resource: c.resource,
				ScopeSpans: []OtelScopeSpans{
					{
						Spans: []OtelSpan{span},
					},
				},
			},
		},
	}
}

// convertToMetricsInternal 内部指标转换方法
// 支持 performance 和 resource 事件类型
func (c *OtelConverter) convertToMetricsInternal(event RUMEventV2) OtelMetricsData {
	if event.Type != "performance" && event.Type != "resource" {
		return OtelMetricsData{}
	}

	// 检查是否有资源数据
	if event.Type == "resource" {
		// 优先使用专用字段，否则从 Data 提取
		resourceData := event.Resource
		if resourceData == nil && event.Data != nil {
			if res, ok := event.Data["resource"].(map[string]interface{}); ok {
				resourceData = res
			} else {
				resourceData = event.Data
			}
		}
		if resourceData == nil {
			return OtelMetricsData{}
		}
	}

	timestamp := event.Date
	if timestamp == 0 {
		timestamp = time.Now().UnixMilli()
	}
	timestampNano := timestamp * 1_000_000

	metrics := make([]OtelMetric, 0, 2)

	// performance 类型的处理（保持向后兼容，可能在 Data 中）
	if event.Type == "performance" {
		var resourceData map[string]interface{}
		if event.Data != nil {
			resourceData, _ = event.Data["resource"].(map[string]interface{})
		}
		if resourceData != nil {
			// 处理时间指标
			if duration, ok := resourceData["duration"].(float64); ok {
				metrics = append(metrics, OtelMetric{
					Name:        "rum.request.duration_ms",
					Description: "Duration of RUM request in milliseconds",
					Unit:        "ms",
					Type:        "Histogram",
					DataPoints: []OtelMetricDataPoint{
						{
							Timestamp:  timestampNano,
							Count:      1,
							Sum:        duration,
							Min:        duration,
							Max:        duration,
							Attributes: toKeyValueSlice(map[string]interface{}{"event.type": "performance"}),
						},
					},
				})
			}

			// 处理大小指标
			if size, ok := resourceData["size"].(float64); ok {
				metrics = append(metrics, OtelMetric{
					Name:        "rum.response.size_bytes",
					Description: "Size of RUM response in bytes",
					Unit:        "bytes",
					Type:        "Gauge",
					DataPoints: []OtelMetricDataPoint{
						{
							Timestamp:  timestampNano,
							Value:      size,
							Attributes: toKeyValueSlice(map[string]interface{}{"event.type": "performance"}),
						},
					},
				})
			}
		}
	}

	// resource 类型的处理
	if event.Type == "resource" {
		// 获取资源数据（优先使用专用字段，否则从 Data 提取）
		var resourceData map[string]interface{}
		if event.Resource != nil {
			resourceData = event.Resource
		} else if event.Data != nil {
			if res, ok := event.Data["resource"].(map[string]interface{}); ok {
				resourceData = res
			} else {
				resourceData = event.Data
			}
		}

		if resourceData != nil {
			if duration, ok := resourceData["duration"].(float64); ok {
				metrics = append(metrics, OtelMetric{
					Name:        "http.client.duration_ms",
					Description: "HTTP client request duration",
					Unit:        "ms",
					Type:        "Histogram",
					DataPoints: []OtelMetricDataPoint{
						{
							Timestamp:  timestampNano,
							Count:      1,
							Sum:        duration,
							Min:        duration,
							Max:        duration,
							Attributes: toKeyValueSlice(map[string]interface{}{"event.type": "resource"}),
						},
					},
				})
			}

			if size, ok := resourceData["size"].(float64); ok {
				metrics = append(metrics, OtelMetric{
					Name:        "http.client.response_size_bytes",
					Description: "HTTP client response size",
					Unit:        "bytes",
					Type:        "Gauge",
					DataPoints: []OtelMetricDataPoint{
						{
							Timestamp:  timestampNano,
							Value:      size,
							Attributes: toKeyValueSlice(map[string]interface{}{"event.type": "resource"}),
						},
					},
				})
			}
		}
	}

	return OtelMetricsData{
		ResourceMetrics: []OtelResourceMetrics{
			{
				Resource: c.resource,
				ScopeMetrics: []OtelScopeMetrics{
					{
						Metrics: metrics,
					},
				},
			},
		},
	}
}

// ======== Private Helper Methods ========

// getSpanNameForEvent 根据事件类型获取 Span Name
// 遵循规范：
// - view → page.view
// - action → ui.action
// - log → log
// - resource → resourceFetch（js/css/jpg 等静态资源）或 resource.load
// - long_task → browser.long_task
// - error → exception
// - performance → resource.load
func (c *OtelConverter) getSpanNameForEvent(event RUMEventV2) string {
	switch event.Type {
	case "view":
		return "page.view"
	case "action":
		return "ui.action"
	case "log":
		return "log"
	case "resource":
		return c.getResourceSpanName(event)
	case "long_task":
		return "browser.long_task"
	case "error":
		return "exception"
	case "performance":
		return "resource.load"
	default:
		return fmt.Sprintf("%s.%s", event.Type, event.EventType)
	}
}

// getResourceSpanName 根据 resource URL 识别 Span Name
// 静态资源（如 .js/.css/.jpg）映射为 resourceFetch，其余映射为 resource.load
func (c *OtelConverter) getResourceSpanName(event RUMEventV2) string {
	resourceURL := c.extractResourceURL(event)
	if isStaticResourceURL(resourceURL) {
		return "resourceFetch"
	}

	return "resource.load"
}

// extractResourceURL 提取 resource URL
func (c *OtelConverter) extractResourceURL(event RUMEventV2) string {
	// 优先使用专用字段，否则从 Data 提取
	resourceData := event.Resource
	if resourceData == nil && event.Data != nil {
		if res, ok := event.Data["resource"].(map[string]interface{}); ok {
			resourceData = res
		} else {
			resourceData = event.Data
		}
	}
	if resourceData == nil {
		return ""
	}

	if resourceURL, ok := resourceData["url"].(string); ok && resourceURL != "" {
		return resourceURL
	}

	if name, ok := resourceData["name"].(string); ok && name != "" {
		return name
	}

	return ""
}

// isStaticResourceURL 判断 URL 是否为静态资源
func isStaticResourceURL(resourceURL string) bool {
	if resourceURL == "" {
		return false
	}

	ext := ""
	if parsedURL, err := url.Parse(resourceURL); err == nil {
		ext = strings.ToLower(path.Ext(parsedURL.Path))
	}

	if ext == "" {
		cleanURL := strings.SplitN(resourceURL, "?", 2)[0]
		cleanURL = strings.SplitN(cleanURL, "#", 2)[0]
		ext = strings.ToLower(path.Ext(cleanURL))
	}

	switch ext {
	case ".js", ".css", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp":
		return true
	default:
		return false
	}
}

// addViewAttributes 添加 view 事件的属性
func (c *OtelConverter) addViewAttributes(builder *SpanBuilder, event RUMEventV2) {
	// 优先使用专用字段，否则从 Data 提取
	viewData := event.View
	if viewData == nil && event.Data != nil {
		if v, ok := event.Data["view"].(map[string]interface{}); ok {
			viewData = v
		} else {
			viewData = event.Data
		}
	}
	if viewData == nil {
		return
	}

	// 提取 view 类型特定的属性
	if viewID, ok := viewData["id"].(string); ok {
		builder.AddAttribute("view.id", viewID)
	}
	if loadingTime, ok := viewData["loading_time"].(float64); ok {
		builder.AddAttribute("view.loading_time", int64(loadingTime))
	}
	if url, ok := viewData["url"].(string); ok {
		builder.AddAttribute("http.url", url)
	}

	// Web Vitals: LCP, FID, CLS 等
	if vitals, ok := viewData["vitals"].(map[string]interface{}); ok {
		if lcp, ok := vitals["lcp"].(float64); ok {
			builder.AddAttribute("web.vital.lcp", lcp)
		}
		if fid, ok := vitals["fid"].(float64); ok {
			builder.AddAttribute("web.vital.fid", fid)
		}
		if cls, ok := vitals["cls"].(float64); ok {
			builder.AddAttribute("web.vital.cls", cls)
		}
	}
}

// addActionAttributes 添加 action 事件的属性
func (c *OtelConverter) addActionAttributes(builder *SpanBuilder, event RUMEventV2) {
	// 优先使用专用字段，否则从 Data 提取
	actionData := event.Action
	if actionData == nil && event.Data != nil {
		if a, ok := event.Data["action"].(map[string]interface{}); ok {
			actionData = a
		} else {
			actionData = event.Data
		}
	}
	if actionData == nil {
		return
	}

	// 提取 action 类型特定的属性
	if actionType, ok := actionData["type"].(string); ok {
		builder.AddAttribute("action.type", actionType)
	}
	if actionID, ok := actionData["id"].(string); ok {
		builder.AddAttribute("action.id", actionID)
	}
	if target, ok := actionData["target"].(map[string]interface{}); ok {
		if targetName, ok := target["name"].(string); ok {
			builder.AddAttribute("action.target.name", targetName)
		}
	}
}

// addLogAttributes 添加 log 事件的属性
func (c *OtelConverter) addLogAttributes(builder *SpanBuilder, event RUMEventV2) {
	// log 类型通常没有专用字段，从 Data 提取
	if event.Data == nil {
		return
	}

	if level, ok := event.Data["level"].(string); ok {
		builder.AddAttribute("log.level", level)
	}
	if message, ok := event.Data["message"].(string); ok {
		builder.AddAttribute("log.message", message)
	}
}

// addExceptionAttributes 添加 error 事件的异常属性
func (c *OtelConverter) addExceptionAttributes(builder *SpanBuilder, event RUMEventV2) {
	// 优先使用专用字段，否则从 Data 提取
	errorData := event.Error
	if errorData == nil && event.Data != nil {
		if e, ok := event.Data["error"].(map[string]interface{}); ok {
			errorData = e
		} else {
			errorData = event.Data
		}
	}
	if errorData == nil {
		return
	}

	// 提取异常属性
	if message, ok := errorData["message"].(string); ok {
		builder.AddAttribute("exception.message", message)
	}
	if exceptionType, ok := errorData["type"].(string); ok {
		builder.AddAttribute("exception.type", exceptionType)
	}
	if stacktrace, ok := errorData["stacktrace"].(string); ok {
		builder.AddAttribute("exception.stacktrace", stacktrace)
	}
	if code, ok := errorData["code"].(string); ok {
		builder.AddAttribute("exception.code", code)
	}
}

// convertLongTaskToMetrics long_task 事件转换为 Metrics
func (c *OtelConverter) convertLongTaskToMetrics(event RUMEventV2) OtelMetricsData {
	if event.Type != "long_task" {
		return OtelMetricsData{}
	}

	// 获取长任务数据（优先使用专用字段，否则从 Data 提取）
	longTaskData := event.LongTask
	if longTaskData == nil && event.Data != nil {
		if lt, ok := event.Data["long_task"].(map[string]interface{}); ok {
			longTaskData = lt
		} else {
			longTaskData = event.Data
		}
	}
	if longTaskData == nil {
		return OtelMetricsData{}
	}

	timestamp := event.Date
	if timestamp == 0 {
		timestamp = time.Now().UnixMilli()
	}
	timestampNano := timestamp * 1_000_000

	metrics := make([]OtelMetric, 0, 1)

	if duration, ok := longTaskData["duration"].(float64); ok {
		metrics = append(metrics, OtelMetric{
			Name:        "browser.long_task.duration_ms",
			Description: "Duration of browser long task in milliseconds",
			Unit:        "ms",
			Type:        "Histogram",
			DataPoints: []OtelMetricDataPoint{
				{
					Timestamp:  timestampNano,
					Count:      1,
					Sum:        duration,
					Min:        duration,
					Max:        duration,
					Attributes: toKeyValueSlice(map[string]interface{}{"event.type": "long_task"}),
				},
			},
		})
	}

	return OtelMetricsData{
		ResourceMetrics: []OtelResourceMetrics{
			{
				Resource: c.resource,
				ScopeMetrics: []OtelScopeMetrics{
					{
						Metrics: metrics,
					},
				},
			},
		},
	}
}

// generateTraceID 生成 Trace ID（16 个十六进制字符）
// 设计方案：
// 1. 优先使用会话 ID（Session ID）作为 Trace ID 的源
// 2. 如果没有会话 ID，使用时间戳、事件类型、事件域组合
// 3. 这样可以确保同一会话内的事件有相同的 Trace ID
func (c *OtelConverter) generateTraceID(event RUMEventV2) string {
	var source string

	// 从 Session 字段优先获取，如果为空则从 Data 中提取
	sessionData := event.Session
	if sessionData == nil && event.Data != nil {
		if s, ok := event.Data["session"].(map[string]interface{}); ok {
			sessionData = s
		}
	}

	if sessionData != nil {
		if sid, ok := sessionData["id"].(string); ok && sid != "" {
			source = sid
		}
	}

	// 如果没有会话 ID，使用时间戳和事件类型
	if source == "" {
		source = fmt.Sprintf("%d-%s-%s", event.Date, event.Type, event.EventType)
	}
	//
	return hashToHex(source, 16)
}

// generateSpanID 生成 Span ID（16 个十六进制字符）
// 设计方案：
// 使用事件类型和时间戳生成唯一的 Span ID
// 确保同一事件类型在不同时间的 Span 有不同的 ID
func (c *OtelConverter) generateSpanID(event RUMEventV2) string {
	source := fmt.Sprintf("%s-%d", event.EventType, event.Date)
	return hashToHex(source, 16)
}

// hashToHex 简单哈希算法生成十六进制字符串
func hashToHex(source string, maxLen int) string {
	hash := 0
	for i, ch := range source {
		hash = hash*31 + int(ch)
		if i >= maxLen {
			break
		}
	}
	return fmt.Sprintf("%032x", hash)[:maxLen]
}

// mapSeverityLevel 映射日志级别
func mapSeverityLevel(level string) int {
	switch level {
	case "TRACE":
		return 1
	case "DEBUG":
		return 5
	case "INFO":
		return 9
	case "WARN":
		return 13
	case "ERROR":
		return 17
	case "FATAL":
		return 21
	default:
		return 0
	}
}

// createOtelResource 创建 OTEL 资源
func createOtelResource() OtelResource {
	return OtelResource{
		Attributes: toKeyValueSlice(map[string]interface{}{
			"service.name":           "datadog-rum",
			"service.source":         "datadog",
			"telemetry.sdk.name":     "datadog-browser",
			"telemetry.sdk.language": "javascript",
		}),
	}
}

// createEnrichedOtelResource 从 RUM 事件创建增强的 OTEL Resource
// 包含服务名称、版本、应用 ID、会话 ID 等信息
func createEnrichedOtelResource(event RUMEventV2) OtelResource {
	attrs := map[string]interface{}{
		"telemetry.sdk.name":     "datadog-browser",
		"telemetry.sdk.language": "javascript",
		"service.source":         "datadog",
	}

	// 服务名称
	if event.Service != "" {
		attrs["service.name"] = event.Service
	} else {
		attrs["service.name"] = "datadog-rum"
	}

	// 服务版本
	if event.Version != "" {
		attrs["service.version"] = event.Version
	}

	// 应用 ID
	if event.Application != nil {
		if appID, ok := event.Application["id"].(string); ok {
			attrs["application.id"] = appID
		}
	}

	// 会话 ID
	if event.Session != nil {
		if sessionID, ok := event.Session["id"].(string); ok {
			attrs["session.id"] = sessionID
		}
		if sessionType, ok := event.Session["type"].(string); ok {
			attrs["session.type"] = sessionType
		}
	}

	// 视图 ID
	if event.View != nil {
		if viewID, ok := event.View["id"].(string); ok {
			attrs["view.id"] = viewID
		}
		if viewURL, ok := event.View["url"].(string); ok {
			attrs["view.url"] = viewURL
		}
	}

	// 用户信息
	if event.User != nil {
		if userID, ok := event.User["id"].(string); ok {
			attrs["user.id"] = userID
		}
		if anonymousID, ok := event.User["anonymous_id"].(string); ok {
			attrs["user.anonymous_id"] = anonymousID
		}
	}

	// 来源
	if event.Source != "" {
		attrs["rum.source"] = event.Source
	}

	// DD 标签
	if event.DDTags != "" {
		attrs["dd.tags"] = event.DDTags
	}

	// DD 元数据
	if event.DD != nil {
		if sdkName, ok := event.DD["sdk_name"].(string); ok {
			attrs["telemetry.sdk.name"] = sdkName
		}
		if formatVersion, ok := event.DD["format_version"].(float64); ok {
			attrs["dd.format_version"] = int(formatVersion)
		}
	}

	return OtelResource{Attributes: toKeyValueSlice(attrs)}
}

// ======== OTEL Data Structures ========

// OtelResource 表示资源
type OtelResource struct {
	Attributes []KeyValue `json:"attributes"`
}

// OtelLogRecord 日志记录
type OtelLogRecord struct {
	Timestamp      int64      `json:"timestamp_nanos"`
	SeverityNumber int        `json:"severity_number"`
	SeverityText   string     `json:"severity_text"`
	Body           string     `json:"body"`
	Attributes     []KeyValue `json:"attributes"`
	TraceID        string     `json:"trace_id,omitempty"`
	SpanID         string     `json:"span_id,omitempty"`
}

// OtelScopeLogs 作用域日志
type OtelScopeLogs struct {
	Scope      map[string]string `json:"scope,omitempty"`
	LogRecords []OtelLogRecord   `json:"log_records"`
}

// OtelResourceLogs 资源日志
type OtelResourceLogs struct {
	Resource  OtelResource    `json:"resource"`
	ScopeLogs []OtelScopeLogs `json:"scope_logs"`
}

// OtelLogsData 完整日志数据
type OtelLogsData struct {
	ResourceLogs []OtelResourceLogs `json:"resource_logs"`
}

// OtelSpan 跨度
type OtelSpan struct {
	TraceID           string                 `json:"trace_id"`
	SpanID            string                 `json:"span_id"`
	ParentSpanID      string                 `json:"parent_span_id,omitempty"`
	Name              string                 `json:"name"`
	Kind              int                    `json:"kind"`
	StartTimeUnixNano int64                  `json:"start_time_unix_nano"`
	EndTimeUnixNano   int64                  `json:"end_time_unix_nano"`
	Attributes        []KeyValue             `json:"attributes"`
	Events            []OtelSpanEvent        `json:"events,omitempty"`
	Status            map[string]interface{} `json:"status"`
}

// OtelSpanEvent Span 事件
type OtelSpanEvent struct {
	Name         string     `json:"name"`
	TimeUnixNano int64      `json:"time_unix_nano"`
	Attributes   []KeyValue `json:"attributes,omitempty"`
}

// OtelScopeSpans 作用域跨度
type OtelScopeSpans struct {
	Scope map[string]string `json:"scope,omitempty"`
	Spans []OtelSpan        `json:"spans"`
}

// OtelResourceSpans 资源跨度
type OtelResourceSpans struct {
	Resource   OtelResource     `json:"resource"`
	ScopeSpans []OtelScopeSpans `json:"scope_spans"`
}

// OtelTracesData 完整跨度数据
type OtelTracesData struct {
	ResourceSpans []OtelResourceSpans `json:"resource_spans"`
}

// OtelMetricDataPoint 数据点
type OtelMetricDataPoint struct {
	Timestamp  int64      `json:"timestamp_unix_nano"`
	Value      float64    `json:"value,omitempty"`
	Count      int64      `json:"count,omitempty"`
	Sum        float64    `json:"sum,omitempty"`
	Min        float64    `json:"min,omitempty"`
	Max        float64    `json:"max,omitempty"`
	Attributes []KeyValue `json:"attributes,omitempty"`
}

// OtelMetric 指标
type OtelMetric struct {
	Name        string                `json:"name"`
	Description string                `json:"description,omitempty"`
	Unit        string                `json:"unit,omitempty"`
	Type        string                `json:"type"`
	DataPoints  []OtelMetricDataPoint `json:"data_points"`
}

// OtelScopeMetrics 作用域指标
type OtelScopeMetrics struct {
	Scope   map[string]string `json:"scope,omitempty"`
	Metrics []OtelMetric      `json:"metrics"`
}

// OtelResourceMetrics 资源指标
type OtelResourceMetrics struct {
	Resource     OtelResource       `json:"resource"`
	ScopeMetrics []OtelScopeMetrics `json:"scope_metrics"`
}

// OtelMetricsData 完整指标数据
type OtelMetricsData struct {
	ResourceMetrics []OtelResourceMetrics `json:"resource_metrics"`
}

// ConversionResult 转换结果
type ConversionResult struct {
	Logs    OtelLogsData
	Traces  OtelTracesData
	Metrics OtelMetricsData
}
