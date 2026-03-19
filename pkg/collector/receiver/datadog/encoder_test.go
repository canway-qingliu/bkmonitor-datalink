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
	"testing"

	"github.com/stretchr/testify/assert"

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
