// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package skywalkingprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/processor"
)

func TestProcessIgnoresNonSkywalkingMeterData(t *testing.T) {
	factory := newTestProcessor(t)
	for _, token := range []string{"enabled-token", "disabled-token"} {
		t.Run(token, func(t *testing.T) {
			record := newMetricsRecord(token, "", metricSpec{
				name: "numActive", framework: "dbcp-2.x", value: 1,
			})

			derived, err := factory.Process(record)
			assert.NoError(t, err)
			assert.Nil(t, derived)
			metrics := record.Data.(pmetric.Metrics)
			assert.Equal(t, "numActive", firstMetric(metrics).Name())
		})
	}
}

func TestProcessSkipsDisabledApplication(t *testing.T) {
	factory := newTestProcessor(t)
	record := newMetricsRecord("disabled-token", define.SourceSkywalkingMeterData, metricSpec{
		name: "numActive", framework: "dbcp-2.x", value: 1,
	})

	derived, err := factory.Process(record)
	assert.ErrorIs(t, err, define.ErrSkipEmptyRecord)
	assert.Nil(t, derived)
}

func TestProcessMapsAndFiltersMetrics(t *testing.T) {
	factory := newTestProcessor(t)
	record := newMetricsRecord(
		"enabled-token",
		define.SourceSkywalkingMeterData,
		metricSpec{name: "numActive", framework: "dbcp-2.x", value: 1},
		metricSpec{name: "activeConnections", framework: "hikaricp-3.x-4.x", value: 2},
		metricSpec{name: "unknown", framework: "dbcp-2.x", value: 3},
	)

	derived, err := factory.Process(record)
	require.NoError(t, err)
	assert.Nil(t, derived)
	metrics := record.Data.(pmetric.Metrics)
	assert.Equal(t, 2, metrics.MetricCount())
	metricSlice := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	assert.Equal(t, "activeConnections", metricSlice.At(0).Name())
	assert.Equal(t, "commonActiveConnections", metricSlice.At(1).Name())
	_, ok := metrics.ResourceMetrics().At(0).Resource().Attributes().Get(define.ResourceAttributeDataSource)
	assert.False(t, ok)
}

func TestProcessUsesExactMappingBeforeWildcard(t *testing.T) {
	factory := newTestProcessor(t)
	record := newMetricsRecord("enabled-token", define.SourceSkywalkingMeterData, metricSpec{
		name: "activeConnections", framework: "dbcp-2.x", value: 1,
	})

	_, err := factory.Process(record)
	require.NoError(t, err)
	assert.Equal(t, "dbcpActiveConnections", firstMetric(record.Data.(pmetric.Metrics)).Name())
}

func TestProcessEndsPipelineWhenMetricsEmptyAfterFiltering(t *testing.T) {
	factory := newTestProcessor(t)
	tests := []struct {
		name string
		spec metricSpec
	}{
		{name: "unknown metric", spec: metricSpec{name: "unknown", framework: "dbcp-2.x", value: 1}},
		{name: "missing framework", spec: metricSpec{name: "activeConnections", value: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := newMetricsRecord("enabled-token", define.SourceSkywalkingMeterData, tt.spec)
			_, err := factory.Process(record)
			assert.ErrorIs(t, err, define.ErrEndOfPipeline)
			assert.Equal(t, 0, record.Data.(pmetric.Metrics).MetricCount())
		})
	}
}

func TestProcessMeterDataCollectsDropReasons(t *testing.T) {
	factory := newTestProcessor(t)
	config := factory.configs.GetByToken("enabled-token").(Config)
	record := newMetricsRecord(
		"enabled-token",
		define.SourceSkywalkingMeterData,
		metricSpec{name: "activeConnections", value: 1, attributes: map[string]string{"pool_name": "primary"}},
		metricSpec{name: "unknown", framework: "dbcp-2.x", value: 2},
	)
	metrics := record.Data.(pmetric.Metrics)
	metric := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().AppendEmpty()
	metric.SetName("unsupported")
	metric.SetEmptyHistogram()

	result := processMeterData(metrics, config.MeterData)

	assert.True(t, result.matched)
	assert.Equal(t, 1, result.missingType)
	assert.Equal(t, 1, result.notMapped)
	assert.Equal(t, 1, result.invalid)
	assert.Equal(t, []meterDropDetail{
		{
			MetricName: "activeConnections",
			DataType:   "Gauge",
			Types:      []any{"<missing>"},
			Dimensions: []map[string]any{{"pool_name": "primary"}},
		},
	}, result.missingTypeMeters)
	assert.Equal(t, []meterDropDetail{
		{
			MetricName: "unknown",
			DataType:   "Gauge",
			Types:      []any{"dbcp-2.x"},
			Dimensions: []map[string]any{{frameworkTypeAttribute: "dbcp-2.x"}},
		},
	}, result.notMappedMeters)
	assert.Equal(t, []meterDropDetail{
		{MetricName: "unsupported", DataType: "Histogram"},
	}, result.invalidMeters)
	assert.Equal(t, 0, metrics.MetricCount())
}

func TestProcessEndsPipelineForUnsupportedMetricType(t *testing.T) {
	factory := newTestProcessor(t)
	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	resourceMetrics.Resource().Attributes().PutString(
		define.ResourceAttributeDataSource,
		define.SourceSkywalkingMeterData,
	)
	metric := resourceMetrics.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("activeConnections")
	metric.SetEmptyHistogram()
	metric.Histogram().DataPoints().AppendEmpty().Attributes().PutString(frameworkTypeAttribute, "custom-framework")
	record := &define.Record{
		RecordType: define.RecordMetrics,
		Token:      define.Token{Original: "enabled-token"},
		Data:       metrics,
	}

	_, err := factory.Process(record)
	assert.ErrorIs(t, err, define.ErrEndOfPipeline)
	assert.Equal(t, 0, metrics.MetricCount())
}

func newTestProcessor(t *testing.T) *skywalkingProcessor {
	t.Helper()
	mainConf := mustLoadProcessorConfig(`
processor:
  - name: "skywalking_processor/default"
    config:
      meter_data:
        enabled: false
`)
	customConf := mustLoadProcessorConfig(`
processor:
  - name: "skywalking_processor/default"
    config:
      meter_data:
        enabled: true
        rules:
          - types: ["*"]
            metrics:
              activeConnections: commonActiveConnections
          - types: ["dbcp-2.x"]
            metrics:
              activeConnections: dbcpActiveConnections
              numActive: activeConnections
`)

	obj, err := NewFactory(mainConf, []processor.SubConfigProcessor{
		{
			Token:  "enabled-token",
			Type:   define.SubConfigFieldDefault,
			Config: processor.Config{Config: customConf},
		},
	})
	require.NoError(t, err)
	return obj.(*skywalkingProcessor)
}

type metricSpec struct {
	name       string
	framework  string
	value      float64
	attributes map[string]string
}

func newMetricsRecord(token, source string, specs ...metricSpec) *define.Record {
	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	if source != "" {
		resourceMetrics.Resource().Attributes().PutString(define.ResourceAttributeDataSource, source)
	}
	metricSlice := resourceMetrics.ScopeMetrics().AppendEmpty().Metrics()
	for _, spec := range specs {
		metric := metricSlice.AppendEmpty()
		metric.SetName(spec.name)
		metric.SetEmptyGauge()
		dataPoint := metric.Gauge().DataPoints().AppendEmpty()
		dataPoint.SetDoubleVal(spec.value)
		if spec.framework != "" {
			dataPoint.Attributes().PutString(frameworkTypeAttribute, spec.framework)
		}
		for key, value := range spec.attributes {
			dataPoint.Attributes().PutString(key, value)
		}
	}
	return &define.Record{
		RecordType: define.RecordMetrics,
		Token:      define.Token{Original: token},
		Data:       metrics,
	}
}

func firstMetric(metrics pmetric.Metrics) pmetric.Metric {
	return metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
}
