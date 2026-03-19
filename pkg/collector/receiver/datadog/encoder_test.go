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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
)

func TestNewOtelConverterDefaultConvert(t *testing.T) {
	converter := NewOtelConverter()

	result := converter.ToOTEL(&RUMEventV2{
		Type:      "custom",
		EventType: "custom",
		Date:      1700000000000,
	})

	assert.Equal(t, 1, result.Logs.LogRecordCount())
	assert.Equal(t, 0, result.Traces.SpanCount())
	assert.Equal(t, 0, result.Metrics.MetricCount())
}

func TestSplitConversionResultUsesPdataCounts(t *testing.T) {
	converter := NewConverter()

	result := converter.ToOTEL(&RUMEventV2{
		Type:      "performance",
		EventType: "resource",
		Date:      1700000000000,
		Data: map[string]interface{}{
			"resource": map[string]interface{}{
				"duration": float64(123),
				"size":     float64(456),
			},
		},
	})

	records := splitConversionResult(result)

	assert.Len(t, records, 3)
	assert.Equal(t, define.RecordLogs, records[0].rtype)
	assert.Equal(t, define.RecordTraces, records[1].rtype)
	assert.Equal(t, define.RecordMetrics, records[2].rtype)
}

func TestConverterResourceEventLogsOnlyOnErrorStatus(t *testing.T) {
	converter := NewConverter()

	successResult := converter.ToOTEL(&RUMEventV2{
		Type:      "resource",
		EventType: "resource",
		Date:      1700000000000,
		Resource: map[string]interface{}{
			"status_code": float64(200),
			"duration":    float64(42),
			"size":        float64(512),
			"url":         "https://example.com/app.js",
		},
	})

	errorResult := converter.ToOTEL(&RUMEventV2{
		Type:      "resource",
		EventType: "resource",
		Date:      1700000001000,
		Resource: map[string]interface{}{
			"status_code": float64(500),
			"duration":    float64(42),
			"size":        float64(512),
			"url":         "https://example.com/api",
		},
	})

	assert.Equal(t, 0, successResult.Logs.LogRecordCount())
	assert.Equal(t, 1, successResult.Traces.SpanCount())
	assert.Equal(t, 2, successResult.Metrics.MetricCount())

	assert.Equal(t, 1, errorResult.Logs.LogRecordCount())
	assert.Equal(t, 1, errorResult.Traces.SpanCount())
	assert.Equal(t, 2, errorResult.Metrics.MetricCount())
}

func TestConverterTraceAndSpanIDsAreNotZero(t *testing.T) {
	converter := NewConverter()
	event := &RUMEventV2{
		Type:      "resource",
		EventType: "resource",
		Date:      1700000000000,
		Session: map[string]interface{}{
			"id": "session-123",
		},
		Resource: map[string]interface{}{
			"status_code": float64(200),
			"duration":    float64(42),
			"size":        float64(512),
			"url":         "https://example.com/app.js",
		},
	}

	traces := converter.convertResourceToTraces(event)
	resourceSpans := traces.ResourceSpans()
	if !assert.Equal(t, 1, resourceSpans.Len()) {
		return
	}

	scopeSpans := resourceSpans.At(0).ScopeSpans()
	if !assert.Equal(t, 1, scopeSpans.Len()) {
		return
	}

	spans := scopeSpans.At(0).Spans()
	if !assert.Equal(t, 1, spans.Len()) {
		return
	}

	span := spans.At(0)
	assert.Equal(t, converter.generateTraceID(event), span.TraceID().HexString())
	assert.Equal(t, converter.generateSpanID(event), span.SpanID().HexString())
	assert.NotEqual(t, strings.Repeat("0", 32), span.TraceID().HexString())
	assert.NotEqual(t, strings.Repeat("0", 16), span.SpanID().HexString())
}

func TestConverterUsesSessionIDAsTraceID(t *testing.T) {
	converter := NewConverter()
	firstEvent := &RUMEventV2{
		Type:      "view",
		EventType: "view",
		Date:      1700000000000,
		Session: map[string]interface{}{
			"id": "session-abc",
		},
		View: map[string]interface{}{
			"id":  "view-1",
			"url": "https://example.com/first",
		},
	}
	secondEvent := &RUMEventV2{
		Type:      "view",
		EventType: "view",
		Date:      1700000005000,
		Session: map[string]interface{}{
			"id": "session-abc",
		},
		View: map[string]interface{}{
			"id":  "view-2",
			"url": "https://example.com/second",
		},
	}

	firstResult := converter.ToOTEL(firstEvent)
	secondResult := converter.ToOTEL(secondEvent)

	firstLogRecord, ok := getSingleLogRecord(firstResult.Logs)
	if !assert.True(t, ok) {
		return
	}

	firstSpan, ok := getSingleSpan(firstResult.Traces)
	if !assert.True(t, ok) {
		return
	}

	secondSpan, ok := getSingleSpan(secondResult.Traces)
	if !assert.True(t, ok) {
		return
	}

	expectedTraceID := converter.generateTraceID(firstEvent)
	assert.Equal(t, expectedTraceID, converter.generateTraceID(secondEvent))
	assert.Equal(t, expectedTraceID, firstLogRecord.TraceID().HexString())
	assert.Equal(t, expectedTraceID, firstSpan.TraceID().HexString())
	assert.Equal(t, expectedTraceID, secondSpan.TraceID().HexString())
	assert.NotEqual(t, firstSpan.SpanID().HexString(), secondSpan.SpanID().HexString())
}

func TestConverterDifferentSessionIDsYieldDifferentTraceIDs(t *testing.T) {
	converter := NewConverter()
	firstEvent := &RUMEventV2{
		Type:      "view",
		EventType: "view",
		Date:      1700000000000,
		Session: map[string]interface{}{
			"id": "session-abc",
		},
		View: map[string]interface{}{
			"id":  "view-1",
			"url": "https://example.com/first",
		},
	}
	secondEvent := &RUMEventV2{
		Type:      "view",
		EventType: "view",
		Date:      1700000000000,
		Session: map[string]interface{}{
			"id": "session-def",
		},
		View: map[string]interface{}{
			"id":  "view-2",
			"url": "https://example.com/second",
		},
	}

	firstSpan, ok := getSingleSpan(converter.ToOTEL(firstEvent).Traces)
	if !assert.True(t, ok) {
		return
	}

	secondSpan, ok := getSingleSpan(converter.ToOTEL(secondEvent).Traces)
	if !assert.True(t, ok) {
		return
	}

	assert.NotEqual(t, converter.generateTraceID(firstEvent), converter.generateTraceID(secondEvent))
	assert.NotEqual(t, firstSpan.TraceID().HexString(), secondSpan.TraceID().HexString())
	assert.NotEqual(t, strings.Repeat("0", 32), firstSpan.TraceID().HexString())
	assert.NotEqual(t, strings.Repeat("0", 32), secondSpan.TraceID().HexString())
}

func TestConverterSameSessionSameTimestampUsesDifferentSpanIDs(t *testing.T) {
	converter := NewConverter()
	firstEvent := &RUMEventV2{
		Type:      "view",
		EventType: "view",
		Date:      1700000000000,
		Session: map[string]interface{}{
			"id": "session-abc",
		},
		View: map[string]interface{}{
			"id":  "view-1",
			"url": "https://example.com/first",
		},
	}
	secondEvent := &RUMEventV2{
		Type:      "view",
		EventType: "view",
		Date:      1700000000000,
		Session: map[string]interface{}{
			"id": "session-abc",
		},
		View: map[string]interface{}{
			"id":  "view-2",
			"url": "https://example.com/second",
		},
	}

	firstResult := converter.ToOTEL(firstEvent)
	secondResult := converter.ToOTEL(secondEvent)

	firstLogRecord, ok := getSingleLogRecord(firstResult.Logs)
	if !assert.True(t, ok) {
		return
	}

	firstSpan, ok := getSingleSpan(firstResult.Traces)
	if !assert.True(t, ok) {
		return
	}

	secondSpan, ok := getSingleSpan(secondResult.Traces)
	if !assert.True(t, ok) {
		return
	}

	assert.Equal(t, firstSpan.TraceID().HexString(), secondSpan.TraceID().HexString())
	assert.Equal(t, firstLogRecord.SpanID().HexString(), firstSpan.SpanID().HexString())
	assert.NotEqual(t, firstSpan.SpanID().HexString(), secondSpan.SpanID().HexString())
	assert.NotEqual(t, strings.Repeat("0", 16), firstSpan.SpanID().HexString())
	assert.NotEqual(t, strings.Repeat("0", 16), secondSpan.SpanID().HexString())
}

func TestConverterResourceSpanIDUsesResourceID(t *testing.T) {
	converter := NewConverter()
	firstEvent := &RUMEventV2{
		Type:      "resource",
		EventType: "resource",
		Date:      1700000000000,
		Session: map[string]interface{}{
			"id": "session-abc",
		},
		Resource: map[string]interface{}{
			"id":          "resource-1",
			"status_code": float64(200),
			"duration":    float64(42),
			"size":        float64(512),
			"url":         "https://example.com/api/orders",
		},
	}
	secondEvent := &RUMEventV2{
		Type:      "resource",
		EventType: "resource",
		Date:      1700000000000,
		Session: map[string]interface{}{
			"id": "session-abc",
		},
		Resource: map[string]interface{}{
			"id":          "resource-2",
			"status_code": float64(200),
			"duration":    float64(42),
			"size":        float64(512),
			"url":         "https://example.com/api/orders",
		},
	}

	firstSpan, ok := getSingleSpan(converter.ToOTEL(firstEvent).Traces)
	if !assert.True(t, ok) {
		return
	}

	secondSpan, ok := getSingleSpan(converter.ToOTEL(secondEvent).Traces)
	if !assert.True(t, ok) {
		return
	}

	expectedFirstSpanID := converter.hashToFixedHex("resource-1", 16)
	expectedSecondSpanID := converter.hashToFixedHex("resource-2", 16)

	assert.Equal(t, expectedFirstSpanID, converter.generateSpanID(firstEvent))
	assert.Equal(t, expectedSecondSpanID, converter.generateSpanID(secondEvent))
	assert.Equal(t, expectedFirstSpanID, firstSpan.SpanID().HexString())
	assert.Equal(t, expectedSecondSpanID, secondSpan.SpanID().HexString())
	assert.NotEqual(t, firstSpan.SpanID().HexString(), secondSpan.SpanID().HexString())
}

func TestConverterEventSpecificSpanIDUsesEventID(t *testing.T) {
	converter := NewConverter()
	testCases := []struct {
		name           string
		event          *RUMEventV2
		expectedIDSeed string
	}{
		{
			name: "view id",
			event: &RUMEventV2{
				Type:      "view",
				EventType: "view",
				Date:      1700000000000,
				Session: map[string]interface{}{
					"id": "session-abc",
				},
				View: map[string]interface{}{
					"id":  "view-123",
					"url": "https://example.com/view-a",
				},
			},
			expectedIDSeed: "view-123",
		},
		{
			name: "action id",
			event: &RUMEventV2{
				Type:      "action",
				EventType: "action",
				Date:      1700000001000,
				Session: map[string]interface{}{
					"id": "session-abc",
				},
				Action: map[string]interface{}{
					"id":   "action-123",
					"type": "click",
				},
			},
			expectedIDSeed: "action-123",
		},
		{
			name: "long task id",
			event: &RUMEventV2{
				Type:      "long_task",
				EventType: "long_task",
				Date:      1700000002000,
				Session: map[string]interface{}{
					"id": "session-abc",
				},
				LongTask: map[string]interface{}{
					"id":          "longtask-123",
					"duration":    float64(71435000),
					"attribution": "script",
				},
			},
			expectedIDSeed: "longtask-123",
		},
		{
			name: "performance resource id",
			event: &RUMEventV2{
				Type:      "performance",
				EventType: "resource",
				Date:      1700000003000,
				Session: map[string]interface{}{
					"id": "session-abc",
				},
				Data: map[string]interface{}{
					"resource": map[string]interface{}{
						"id":       "perf-resource-123",
						"duration": float64(123),
						"size":     float64(456),
					},
				},
			},
			expectedIDSeed: "perf-resource-123",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			span, ok := getSingleSpan(converter.ToOTEL(testCase.event).Traces)
			if !assert.True(t, ok) {
				return
			}

			expectedSpanID := converter.hashToFixedHex(testCase.expectedIDSeed, 16)

			assert.Equal(t, expectedSpanID, converter.generateSpanID(testCase.event))
			assert.Equal(t, expectedSpanID, span.SpanID().HexString())
		})
	}
}

func getSingleLogRecord(logs plog.Logs) (plog.LogRecord, bool) {
	resourceLogs := logs.ResourceLogs()
	if resourceLogs.Len() != 1 {
		return plog.NewLogRecord(), false
	}

	scopeLogs := resourceLogs.At(0).ScopeLogs()
	if scopeLogs.Len() != 1 {
		return plog.NewLogRecord(), false
	}

	logRecords := scopeLogs.At(0).LogRecords()
	if logRecords.Len() != 1 {
		return plog.NewLogRecord(), false
	}

	return logRecords.At(0), true
}

func getSingleSpan(traces ptrace.Traces) (ptrace.Span, bool) {
	resourceSpans := traces.ResourceSpans()
	if resourceSpans.Len() != 1 {
		return ptrace.NewSpan(), false
	}

	scopeSpans := resourceSpans.At(0).ScopeSpans()
	if scopeSpans.Len() != 1 {
		return ptrace.NewSpan(), false
	}

	spans := scopeSpans.At(0).Spans()
	if spans.Len() != 1 {
		return ptrace.NewSpan(), false
	}

	return spans.At(0), true
}
