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
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ======== 转换器入口 ========

// Converter 高性能的 OTEL 转换器（使用官方 pdata 库）
type Converter struct {
	strategies map[string]ConversionStrategy
	mu         sync.RWMutex
}

// ConversionStrategy 转换策略接口
type ConversionStrategy interface {
	CanHandle(event *RUMEventV2) bool
	Convert(event *RUMEventV2, converter *Converter) ConversionOutput
}

// ConversionOutput 转换输出
type ConversionOutput struct {
	Logs    plog.Logs
	Traces  ptrace.Traces
	Metrics pmetric.Metrics
}

// ConversionResult 兼容旧调用方。
type ConversionResult = ConversionOutput

// NewConverter 创建新的转换器
func NewConverter() *Converter {
	converter := &Converter{
		strategies: make(map[string]ConversionStrategy),
	}

	// 注册所有策略
	strategies := map[string]ConversionStrategy{
		"error":       &errorEventStrategy{},
		"performance": &performanceEventStrategy{},
		"view":        &simpleEventStrategy{eventType: "view"},
		"action":      &actionEventStrategy{},
		"log":         &simpleEventStrategy{eventType: "log"},
		"resource":    &resourceEventStrategy{},
		"long_task":   &longTaskEventStrategy{},
	}

	for eventType, strategy := range strategies {
		converter.strategies[eventType] = strategy
	}

	return converter
}

// NewOtelConverter 兼容旧调用方。
func NewOtelConverter() *Converter {
	return NewConverter()
}

// ToOTEL 根据事件类型进行转换
func (c *Converter) ToOTEL(event *RUMEventV2) ConversionOutput {
	c.mu.RLock()
	strategy := c.strategies[event.Type]
	c.mu.RUnlock()

	if strategy != nil {
		return strategy.Convert(event, c)
	}

	// 默认转换为日志
	return c.defaultConvert(event)
}

// defaultConvert 默认转换为日志数据
func (c *Converter) defaultConvert(event *RUMEventV2) ConversionOutput {
	logs := plog.NewLogs()
	resourceLog := logs.ResourceLogs().AppendEmpty()

	// 配置 Resource
	c.enrichResource(resourceLog.Resource(), event)

	scopeLog := resourceLog.ScopeLogs().AppendEmpty()
	logRecord := scopeLog.LogRecords().AppendEmpty()

	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(event.Date)))
	logRecord.Body().SetStringVal("unknown event")
	logRecord.SetSeverityNumber(plog.SeverityNumberINFO)
	logRecord.Attributes().UpsertString("event.type", event.Type)
	logRecord.Attributes().UpsertString("event.domain", event.EventType)

	return ConversionOutput{
		Logs:    logs,
		Traces:  ptrace.NewTraces(),
		Metrics: pmetric.NewMetrics(),
	}
}

// ======== 转换策略实现 ========

// errorEventStrategy 错误事件转换策略
type errorEventStrategy struct{}

func (s *errorEventStrategy) CanHandle(event *RUMEventV2) bool {
	return event.Type == "error"
}

func (s *errorEventStrategy) Convert(event *RUMEventV2, converter *Converter) ConversionOutput {
	output := ConversionOutput{
		Logs:    converter.convertToLogs(event, true),
		Traces:  converter.convertExceptionToTraces(event),
		Metrics: pmetric.NewMetrics(),
	}
	return output
}

// performanceEventStrategy 性能事件转换策略
type performanceEventStrategy struct{}

func (s *performanceEventStrategy) CanHandle(event *RUMEventV2) bool {
	return event.Type == "performance"
}

func (s *performanceEventStrategy) Convert(event *RUMEventV2, converter *Converter) ConversionOutput {
	output := ConversionOutput{
		Logs:    converter.convertToLogs(event, false),
		Traces:  converter.convertPerformanceToTraces(event),
		Metrics: converter.convertToMetrics(event),
	}
	return output
}

// simpleEventStrategy 简单事件转换策略（view、action、log）
type simpleEventStrategy struct {
	eventType string
}

func (s *simpleEventStrategy) CanHandle(event *RUMEventV2) bool {
	return event.Type == s.eventType
}

func (s *simpleEventStrategy) Convert(event *RUMEventV2, converter *Converter) ConversionOutput {
	output := ConversionOutput{
		Logs:    converter.convertToLogs(event, false),
		Traces:  converter.convertSimpleEventToTraces(event),
		Metrics: pmetric.NewMetrics(),
	}
	return output
}

// actionEventStrategy action 事件转换策略
type actionEventStrategy struct{}

func (s *actionEventStrategy) CanHandle(event *RUMEventV2) bool {
	return event.Type == "action"
}

func (s *actionEventStrategy) Convert(event *RUMEventV2, converter *Converter) ConversionOutput {
	output := ConversionOutput{
		Traces:  converter.convertSimpleEventToTraces(event),
		Logs:    plog.NewLogs(),
		Metrics: pmetric.NewMetrics(),
	}
	return output
}

// resourceEventStrategy 资源事件转换策略
type resourceEventStrategy struct{}

func (s *resourceEventStrategy) CanHandle(event *RUMEventV2) bool {
	return event.Type == "resource"
}

func (s *resourceEventStrategy) Convert(event *RUMEventV2, converter *Converter) ConversionOutput {
	logs := plog.NewLogs()
	if converter.shouldGenerateLogForResource(event) {
		logs = converter.convertToLogs(event, false)
	}

	output := ConversionOutput{
		Traces:  converter.convertResourceToTraces(event),
		Logs:    logs,
		Metrics: converter.convertToMetrics(event),
	}
	return output
}

// longTaskEventStrategy 长任务事件转换策略
type longTaskEventStrategy struct{}

func (s *longTaskEventStrategy) CanHandle(event *RUMEventV2) bool {
	return event.Type == "long_task"
}

func (s *longTaskEventStrategy) Convert(event *RUMEventV2, converter *Converter) ConversionOutput {
	output := ConversionOutput{
		Logs:    converter.convertToLogs(event, false),
		Traces:  converter.convertLongTaskToTraces(event),
		Metrics: converter.convertLongTaskToMetrics(event),
	}
	return output
}

// ======== 日志转换 ========

// convertToLogs 将事件转换为日志数据
func (c *Converter) convertToLogs(event *RUMEventV2, isError bool) plog.Logs {
	logs := plog.NewLogs()
	resourceLog := logs.ResourceLogs().AppendEmpty()

	// 配置 Resource
	c.enrichResource(resourceLog.Resource(), event)

	scopeLog := resourceLog.ScopeLogs().AppendEmpty()
	logRecord := scopeLog.LogRecords().AppendEmpty()

	// 设置时间戳
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(event.Date)))

	// 提取消息和级别
	message, severity := c.extractMessageAndLevel(event, isError)
	logRecord.Body().SetStringVal(message)
	logRecord.SetSeverityText(severity)
	logRecord.SetSeverityNumber(c.mapSeverityNumber(severity))

	// 设置 Trace 信息
	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)
	logRecord.SetTraceID(c.stringToTraceID(traceID))
	logRecord.SetSpanID(c.stringToSpanID(spanID))

	// 添加属性
	attrs := logRecord.Attributes()
	attrs.UpsertString("event.type", event.Type)
	attrs.UpsertString("event.domain", event.EventType)

	// 根据事件类型添加特定数据
	c.addEventAttributes(attrs, event)

	return logs
}

// extractMessageAndLevel 从事件提取消息和级别
func (c *Converter) extractMessageAndLevel(event *RUMEventV2, isError bool) (string, string) {
	message := ""
	severity := "INFO"

	// 优先使用专用字段
	switch event.Type {
	case "error":
		if event.Error != nil {
			if msg, ok := event.Error["message"].(string); ok {
				message = msg
			}
		}
	case "action":
		if event.Action != nil {
			if msg, ok := event.Action["message"].(string); ok {
				message = msg
			}
		}
	case "view":
		if event.View != nil {
			if msg, ok := event.View["message"].(string); ok {
				message = msg
			}
		}
	case "long_task":
		if event.LongTask != nil {
			if msg, ok := event.LongTask["message"].(string); ok {
				message = msg
			}
		}
	case "resource":
		if event.Resource != nil {
			if msg, ok := event.Resource["message"].(string); ok {
				message = msg
			}
		}
	}

	// 如果由专用字段未得到消息，尝试从 Data 提取
	if message == "" && event.Data != nil {
		if msg, ok := event.Data["message"].(string); ok {
			message = msg
		}
	}

	// 如果是错误事件，强制设置 ERROR 级别
	if isError {
		severity = "ERROR"
	}

	return message, severity
}

// addEventAttributes 根据事件类型添加属性
func (c *Converter) addEventAttributes(attrs pcommon.Map, event *RUMEventV2) {
	switch event.Type {
	case "view":
		if event.View != nil {
			if id, ok := event.View["id"].(string); ok {
				attrs.UpsertString("view.id", id)
			}
			if url, ok := event.View["url"].(string); ok {
				attrs.UpsertString("view.url", url)
			}
		}
	case "action":
		if event.Action != nil {
			if id, ok := event.Action["id"].(string); ok {
				attrs.UpsertString("action.id", id)
			}
			if actionType, ok := event.Action["type"].(string); ok {
				attrs.UpsertString("action.type", actionType)
			}
		}
	case "error":
		if event.Error != nil {
			if errorType, ok := event.Error["type"].(string); ok {
				attrs.UpsertString("error.type", errorType)
			}
		}
	}

	// 添加会话和用户信息
	if event.Session != nil {
		if sid, ok := event.Session["id"].(string); ok {
			attrs.UpsertString("session.id", sid)
		}
	}
	if event.User != nil {
		if uid, ok := event.User["id"].(string); ok {
			attrs.UpsertString("user.id", uid)
		}
	}
}

// mapSeverityNumber 将文本级别映射到数字
func (c *Converter) mapSeverityNumber(level string) plog.SeverityNumber {
	switch level {
	case "TRACE":
		return plog.SeverityNumberTRACE
	case "DEBUG":
		return plog.SeverityNumberDEBUG
	case "INFO":
		return plog.SeverityNumberINFO
	case "WARN":
		return plog.SeverityNumberWARN
	case "ERROR":
		return plog.SeverityNumberERROR
	case "FATAL":
		return plog.SeverityNumberFATAL
	default:
		return plog.SeverityNumberUNDEFINED
	}
}

// stringToTraceID 将十六进制字符串转换为 TraceID。
func (c *Converter) stringToTraceID(hexStr string) pcommon.TraceID {
	var traceID [16]byte
	if len(hexStr) >= 32 {
		hexStr = hexStr[:32]
	} else {
		// Pad with zeros
		hexStr = strings.Repeat("0", 32-len(hexStr)) + hexStr
	}
	_, _ = hex.Decode(traceID[:], []byte(hexStr))
	return pcommon.NewTraceID(traceID)
}

// stringToSpanID 将十六进制字符串转换为 SpanID。
func (c *Converter) stringToSpanID(hexStr string) pcommon.SpanID {
	var spanID [8]byte
	if len(hexStr) >= 16 {
		hexStr = hexStr[:16]
	} else {
		// Pad with zeros
		hexStr = strings.Repeat("0", 16-len(hexStr)) + hexStr
	}
	_, _ = hex.Decode(spanID[:], []byte(hexStr))
	return pcommon.NewSpanID(spanID)
}

// ======== Trace 转换 ========

// convertSimpleEventToTraces 简单事件（view, action, log）转换为 Trace
func (c *Converter) convertSimpleEventToTraces(event *RUMEventV2) ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	c.enrichResource(resourceSpans.Resource(), event)

	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	span := scopeSpans.Spans().AppendEmpty()

	// 确定 Span Name
	spanName := c.getSpanNameForEvent(event)
	span.SetName(spanName)
	span.SetKind(ptrace.SpanKindInternal)

	// 时间戳
	ts := pcommon.NewTimestampFromTime(time.UnixMilli(event.Date))
	span.SetStartTimestamp(ts)
	span.SetEndTimestamp(ts + (pcommon.Timestamp(time.Millisecond)))

	// Trace & Span ID
	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)
	span.SetTraceID(c.stringToTraceID(traceID))
	span.SetSpanID(c.stringToSpanID(spanID))

	// 属性
	attrs := span.Attributes()
	attrs.UpsertString("event.type", event.Type)
	attrs.UpsertString("event.domain", event.EventType)

	// 根据类型添加特定属性
	switch event.Type {
	case "view":
		if event.View != nil {
			if id, ok := event.View["id"].(string); ok {
				attrs.UpsertString("view.id", id)
			}
			if viewURL, ok := event.View["url"].(string); ok {
				attrs.UpsertString("view.url", viewURL)
			}
			if loadingTime, ok := event.View["loading_time"].(float64); ok {
				attrs.UpsertInt("view.loading_time", int64(loadingTime))
			}
		}
	case "action":
		if event.Action != nil {
			if actionType, ok := event.Action["type"].(string); ok {
				attrs.UpsertString("action.type", actionType)
			}
			if id, ok := event.Action["id"].(string); ok {
				attrs.UpsertString("action.id", id)
			}
		}
	}

	return traces
}

// convertExceptionToTraces 错误事件转换为异常 Trace
func (c *Converter) convertExceptionToTraces(event *RUMEventV2) ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	c.enrichResource(resourceSpans.Resource(), event)

	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	span := scopeSpans.Spans().AppendEmpty()

	span.SetName("exception")
	span.SetKind(ptrace.SpanKindInternal)

	// 时间戳
	ts := pcommon.NewTimestampFromTime(time.UnixMilli(event.Date))
	span.SetStartTimestamp(ts)
	span.SetEndTimestamp(ts + (pcommon.Timestamp(time.Millisecond)))

	// Trace & Span ID
	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)
	span.SetTraceID(c.stringToTraceID(traceID))
	span.SetSpanID(c.stringToSpanID(spanID))

	// 属性
	attrs := span.Attributes()
	attrs.UpsertString("event.type", event.Type)
	attrs.UpsertString("event.domain", event.EventType)

	// 错误状态和属性
	errorMsg := ""
	if event.Error != nil {
		if msg, ok := event.Error["message"].(string); ok {
			errorMsg = msg
		}
		if errorType, ok := event.Error["type"].(string); ok {
			attrs.UpsertString("exception.type", errorType)
		}
		if stacktrace, ok := event.Error["stacktrace"].(string); ok {
			attrs.UpsertString("exception.stacktrace", stacktrace)
		}
	}

	span.Status().SetCode(ptrace.StatusCodeError)
	if errorMsg != "" {
		span.Status().SetMessage(errorMsg)
	}

	return traces
}

// convertPerformanceToTraces performance 事件转换为 Trace
func (c *Converter) convertPerformanceToTraces(event *RUMEventV2) ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	c.enrichResource(resourceSpans.Resource(), event)

	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	span := scopeSpans.Spans().AppendEmpty()

	span.SetName("resource.load")
	span.SetKind(ptrace.SpanKindInternal)

	// 时间戳
	ts := pcommon.NewTimestampFromTime(time.UnixMilli(event.Date))
	span.SetStartTimestamp(ts)

	// 设置结束时间（由 duration 决定）
	endTs := ts + (pcommon.Timestamp(time.Millisecond))
	if event.Data != nil {
		if resourceData, ok := event.Data["resource"].(map[string]interface{}); ok {
			if duration, ok := resourceData["duration"].(float64); ok {
				endTs = ts + (pcommon.Timestamp(time.Duration(duration) * time.Millisecond))
			}
		}
	}
	span.SetEndTimestamp(endTs)

	// Trace & Span ID
	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)
	span.SetTraceID(c.stringToTraceID(traceID))
	span.SetSpanID(c.stringToSpanID(spanID))

	// 属性
	attrs := span.Attributes()
	attrs.UpsertString("event.type", event.Type)
	attrs.UpsertString("event.domain", event.EventType)

	return traces
}

// convertResourceToTraces resource 事件转换为 Trace
func (c *Converter) convertResourceToTraces(event *RUMEventV2) ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	c.enrichResource(resourceSpans.Resource(), event)

	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	span := scopeSpans.Spans().AppendEmpty()

	// 确定 Span Name
	resourceURL := c.extractResourceURL(event)
	if c.isStaticResourceURL(resourceURL) {
		span.SetName("resourceFetch")
	} else {
		span.SetName("resource.load")
	}
	span.SetKind(ptrace.SpanKindClient)

	// 时间戳
	ts := pcommon.NewTimestampFromTime(time.UnixMilli(event.Date))
	span.SetStartTimestamp(ts)

	// 计算结束时间
	endTs := ts + (pcommon.Timestamp(time.Millisecond))
	if event.Resource != nil {
		if duration, ok := event.Resource["duration"].(float64); ok {
			endTs = ts + (pcommon.Timestamp(time.Duration(duration) * time.Millisecond))
		}
	}
	span.SetEndTimestamp(endTs)

	// Trace & Span ID
	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)
	span.SetTraceID(c.stringToTraceID(traceID))
	span.SetSpanID(c.stringToSpanID(spanID))

	// 属性
	attrs := span.Attributes()
	attrs.UpsertString("event.type", event.Type)

	// 添加视图信息
	if event.View != nil {
		if viewID, ok := event.View["id"].(string); ok {
			attrs.UpsertString("view.id", viewID)
		}
		if viewURL, ok := event.View["url"].(string); ok {
			attrs.UpsertString("view.url", viewURL)
		}
		if referrer, ok := event.View["referrer"].(string); ok {
			attrs.UpsertString("view.referrer", referrer)
		}
	}

	// 添加网络连接信息
	if event.Connectivity != nil {
		if status, ok := event.Connectivity["status"].(string); ok {
			attrs.UpsertString("connectivity.status", status)
		}
		if effectiveType, ok := event.Connectivity["effective_type"].(string); ok {
			attrs.UpsertString("connectivity.effective_type", effectiveType)
		}
	}

	// 添加资源属性
	if event.Resource != nil {
		if resourceURL != "" {
			attrs.UpsertString("http.url", resourceURL)
		}
		if statusCode, ok := event.Resource["status_code"].(float64); ok {
			attrs.UpsertInt("http.status_code", int64(statusCode))
		}
		if resType, ok := event.Resource["type"].(string); ok {
			attrs.UpsertString("resource.type", resType)
		}
		if protocol, ok := event.Resource["protocol"].(string); ok {
			attrs.UpsertString("http.protocol", protocol)
		}
	}

	return traces
}

// convertLongTaskToTraces long_task 事件转换为 Trace
func (c *Converter) convertLongTaskToTraces(event *RUMEventV2) ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	c.enrichResource(resourceSpans.Resource(), event)

	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	span := scopeSpans.Spans().AppendEmpty()

	span.SetName("browser.long_task")
	span.SetKind(ptrace.SpanKindInternal)

	// 时间戳
	ts := pcommon.NewTimestampFromTime(time.UnixMilli(event.Date))
	span.SetStartTimestamp(ts)

	// 设置结束时间
	endTs := ts + (pcommon.Timestamp(time.Millisecond))
	if event.LongTask != nil {
		if duration, ok := event.LongTask["duration"].(float64); ok {
			endTs = ts + (pcommon.Timestamp(time.Duration(duration) * time.Millisecond))
		}
	}
	span.SetEndTimestamp(endTs)

	// Trace & Span ID
	traceID := c.generateTraceID(event)
	spanID := c.generateSpanID(event)
	span.SetTraceID(c.stringToTraceID(traceID))
	span.SetSpanID(c.stringToSpanID(spanID))

	// 属性
	attrs := span.Attributes()
	attrs.UpsertString("event.type", event.Type)
	attrs.UpsertString("event.domain", event.EventType)

	if event.LongTask != nil {
		if duration, ok := event.LongTask["duration"].(float64); ok {
			attrs.UpsertDouble("longtask.duration", duration)
		}
		if attribution, ok := event.LongTask["attribution"].(string); ok {
			attrs.UpsertString("longtask.attribution", attribution)
		}
	}

	return traces
}

// ======== 指标转换 ========

// convertToMetrics 将事件转换为指标数据
func (c *Converter) convertToMetrics(event *RUMEventV2) pmetric.Metrics {
	if event.Type != "performance" && event.Type != "resource" && event.Type != "long_task" {
		return pmetric.NewMetrics()
	}

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	c.enrichResource(resourceMetrics.Resource(), event)

	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()

	ts := pcommon.NewTimestampFromTime(time.UnixMilli(event.Date))

	switch event.Type {
	case "performance":
		if event.Data != nil {
			if resourceData, ok := event.Data["resource"].(map[string]interface{}); ok {
				c.addPerformanceMetrics(scopeMetrics, resourceData, ts)
			}
		}
	case "resource":
		if event.Resource != nil {
			c.addResourceMetrics(scopeMetrics, event.Resource, ts)
		}
	case "long_task":
		if event.LongTask != nil {
			c.addLongTaskMetrics(scopeMetrics, event.LongTask, ts)
		}
	}

	return metrics
}

// addPerformanceMetrics 添加性能指标
func (c *Converter) addPerformanceMetrics(scopeMetrics pmetric.ScopeMetrics, resourceData map[string]interface{}, ts pcommon.Timestamp) {
	// Duration 指标
	if duration, ok := resourceData["duration"].(float64); ok {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("rum.request.duration_ms")
		metric.SetDescription("Duration of RUM request in milliseconds")
		metric.SetUnit("ms")

		metric.SetDataType(pmetric.MetricDataTypeHistogram)
		histogram := metric.Histogram()
		histogram.SetAggregationTemporality(pmetric.MetricAggregationTemporalityCumulative)
		dataPoint := histogram.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(ts)
		dataPoint.SetCount(1)
		dataPoint.SetSum(duration)
		dataPoint.SetMExplicitBounds([]float64{10, 50, 100, 500, 1000})
		bucketCounts := []uint64{0, 0, 0, 0, 0, 1}
		dataPoint.SetMBucketCounts(bucketCounts)
		dataPoint.Attributes().UpsertString("event.type", "performance")
	}

	// Size 指标
	if size, ok := resourceData["size"].(float64); ok {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("rum.response.size_bytes")
		metric.SetDescription("Size of RUM response in bytes")
		metric.SetUnit("bytes")

		metric.SetDataType(pmetric.MetricDataTypeGauge)
		gauge := metric.Gauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(ts)
		dataPoint.SetDoubleVal(size)
		dataPoint.Attributes().UpsertString("event.type", "performance")
	}
}

// addResourceMetrics 添加资源指标
func (c *Converter) addResourceMetrics(scopeMetrics pmetric.ScopeMetrics, resourceData map[string]interface{}, ts pcommon.Timestamp) {
	// Duration 指标
	if duration, ok := resourceData["duration"].(float64); ok {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("http.client.duration_ms")
		metric.SetDescription("HTTP client request duration")
		metric.SetUnit("ms")

		metric.SetDataType(pmetric.MetricDataTypeHistogram)
		histogram := metric.Histogram()
		histogram.SetAggregationTemporality(pmetric.MetricAggregationTemporalityCumulative)
		dataPoint := histogram.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(ts)
		dataPoint.SetCount(1)
		dataPoint.SetSum(duration)
		dataPoint.SetMExplicitBounds([]float64{10, 50, 100, 500, 1000})
		bucketCounts := []uint64{0, 0, 0, 0, 0, 1}
		dataPoint.SetMBucketCounts(bucketCounts)
		dataPoint.Attributes().UpsertString("event.type", "resource")
	}

	// Size 指标
	if size, ok := resourceData["size"].(float64); ok {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("http.client.response_size_bytes")
		metric.SetDescription("HTTP client response size")
		metric.SetUnit("bytes")

		metric.SetDataType(pmetric.MetricDataTypeGauge)
		gauge := metric.Gauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(ts)
		dataPoint.SetDoubleVal(size)
		dataPoint.Attributes().UpsertString("event.type", "resource")
	}
}

// addLongTaskMetrics 添加长任务指标
func (c *Converter) addLongTaskMetrics(scopeMetrics pmetric.ScopeMetrics, longTaskData map[string]interface{}, ts pcommon.Timestamp) {
	if duration, ok := longTaskData["duration"].(float64); ok {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("browser.long_task.duration_ms")
		metric.SetDescription("Duration of browser long task in milliseconds")
		metric.SetUnit("ms")

		metric.SetDataType(pmetric.MetricDataTypeHistogram)
		histogram := metric.Histogram()
		histogram.SetAggregationTemporality(pmetric.MetricAggregationTemporalityCumulative)
		dataPoint := histogram.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(ts)
		dataPoint.SetCount(1)
		dataPoint.SetSum(duration)
		dataPoint.SetMExplicitBounds([]float64{10, 50, 100, 500, 1000})
		bucketCounts := []uint64{0, 0, 0, 0, 0, 1}
		dataPoint.SetMBucketCounts(bucketCounts)
		dataPoint.Attributes().UpsertString("event.type", "long_task")
	}
}

// convertLongTaskToMetrics 长任务事件转换为 Metrics
func (c *Converter) convertLongTaskToMetrics(event *RUMEventV2) pmetric.Metrics {
	if event.Type != "long_task" {
		return pmetric.NewMetrics()
	}

	if event.LongTask == nil {
		return pmetric.NewMetrics()
	}

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	c.enrichResource(resourceMetrics.Resource(), event)

	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	ts := pcommon.NewTimestampFromTime(time.UnixMilli(event.Date))

	c.addLongTaskMetrics(scopeMetrics, event.LongTask, ts)

	return metrics
}

// ======== 辅助方法 ========

// enrichResource 将事件信息添加到 Resource。
func (c *Converter) enrichResource(resource pcommon.Resource, event *RUMEventV2) {
	attrs := resource.Attributes()

	// 复制基础 Resource 属性
	attrs.UpsertString("service.name", "datadog-rum")
	attrs.UpsertString("service.source", "datadog")
	attrs.UpsertString("telemetry.sdk.name", "datadog-browser")
	attrs.UpsertString("telemetry.sdk.language", "javascript")

	// 添加服务信息
	if event.Service != "" {
		attrs.UpsertString("service.name", event.Service)
	}
	if event.Version != "" {
		attrs.UpsertString("service.version", event.Version)
	}

	// 添加应用信息
	if event.Application != nil {
		if appID, ok := event.Application["id"].(string); ok {
			attrs.UpsertString("application.id", appID)
		}
	}

	// 添加会话信息
	if event.Session != nil {
		if sessionID, ok := event.Session["id"].(string); ok {
			attrs.UpsertString("session.id", sessionID)
		}
		if sessionType, ok := event.Session["type"].(string); ok {
			attrs.UpsertString("session.type", sessionType)
		}
	}

	// 添加用户信息
	if event.User != nil {
		if userID, ok := event.User["id"].(string); ok {
			attrs.UpsertString("user.id", userID)
		}
		if anonymousID, ok := event.User["anonymous_id"].(string); ok {
			attrs.UpsertString("user.anonymous_id", anonymousID)
		}
	}

	// 添加源和标签
	if event.Source != "" {
		attrs.UpsertString("rum.source", event.Source)
	}
	if event.DDTags != "" {
		attrs.UpsertString("dd.tags", event.DDTags)
	}
}

// getSpanNameForEvent 根据事件类型获取 Span Name
func (c *Converter) getSpanNameForEvent(event *RUMEventV2) string {
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
func (c *Converter) getResourceSpanName(event *RUMEventV2) string {
	resourceURL := c.extractResourceURL(event)
	if c.isStaticResourceURL(resourceURL) {
		return "resourceFetch"
	}
	return "resource.load"
}

// extractResourceURL 提取 resource URL
func (c *Converter) extractResourceURL(event *RUMEventV2) string {
	if event.Resource != nil {
		if resourceURL, ok := event.Resource["url"].(string); ok && resourceURL != "" {
			return resourceURL
		}
		if name, ok := event.Resource["name"].(string); ok && name != "" {
			return name
		}
	}

	if event.Data != nil {
		if resourceData, ok := event.Data["resource"].(map[string]interface{}); ok {
			if resourceURL, ok := resourceData["url"].(string); ok && resourceURL != "" {
				return resourceURL
			}
			if name, ok := resourceData["name"].(string); ok && name != "" {
				return name
			}
		}
	}

	return ""
}

// shouldGenerateLogForResource 判断 resource 事件是否需要额外日志。
func (c *Converter) shouldGenerateLogForResource(event *RUMEventV2) bool {
	if event == nil || event.Resource == nil {
		return false
	}

	statusCode, ok := event.Resource["status_code"].(float64)
	if !ok {
		return false
	}

	return statusCode < 200 || statusCode >= 300
}

// isStaticResourceURL 判断 URL 是否为静态资源
func (c *Converter) isStaticResourceURL(resourceURL string) bool {
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

// generateTraceID 生成 Trace ID（16 字节）
func (c *Converter) generateTraceID(event *RUMEventV2) string {
	var source string

	// 优先使用会话 ID
	if event.Session != nil {
		if sid, ok := event.Session["id"].(string); ok && sid != "" {
			source = sid
		}
	}

	// 如果没有会话 ID，使用时间戳和事件类型
	if source == "" {
		source = fmt.Sprintf("%d-%s-%s", event.Date, event.Type, event.EventType)
	}

	return c.hashToFixedHex(source, 16)
}

// generateSpanID 生成 Span ID（8 字节）
func (c *Converter) generateSpanID(event *RUMEventV2) string {
	source := fmt.Sprintf("%s-%d", event.EventType, event.Date)
	return c.hashToFixedHex(source, 8)
}

// hashToFixedHex 简单且快速的哈希函数，生成固定长度的十六进制字符串
// 避免使用加密哈希函数，提高性能
func (c *Converter) hashToFixedHex(source string, length int) string {
	hash := uint64(0)
	for i, ch := range source {
		hash = hash*31 + uint64(ch)
		if i >= 64 { // 防止无限循环
			break
		}
	}
	result := fmt.Sprintf("%032x", hash)
	if len(result) > length {
		return result[:length]
	}
	return result
}
