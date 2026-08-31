// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package skywalking

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	agentv3 "skywalking.apache.org/repo/goapi/collect/language/agent/v3"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
)

func TestMeterConverterConvertSingleValue(t *testing.T) {
	const timestamp = int64(1758276000000)
	converter := newMeterConverter("service", "instance", timestamp, "my-token")
	meters := []*agentv3.MeterData{
		newSingleValueMeter("activeConnections", 1, map[string]string{
			"type": "hikaricp-3.x-4.x",
			"name": "1.1.1.1:3306",
		}),
		newSingleValueMeter("numActive", 2, map[string]string{
			"type": "dbcp-2.x",
			"name": "1.1.1.2:3306",
		}),
		newSingleValueMeter("customMetric", 3, map[string]string{
			"type": "custom-pool",
		}),
	}
	for _, meter := range meters {
		converter.Convert(meter)
	}

	metrics := converter.Get()
	require.Equal(t, 3, metrics.MetricCount())
	resourceMetrics := metrics.ResourceMetrics().At(0)
	resource := resourceMetrics.Resource().Attributes()
	assertResourceString(t, resource, "service_name", "service")
	assertResourceString(t, resource, "bk_instance_id", "instance")
	assertResourceString(t, resource, "bk.data.token", "my-token")
	assertResourceString(t, resource, define.ResourceAttributeDataSource, define.SourceSkywalkingMeterData)

	actualMetrics := resourceMetrics.ScopeMetrics().At(0).Metrics()
	for i, expected := range []struct {
		name      string
		value     float64
		framework string
	}{
		{name: "activeConnections", value: 1, framework: "hikaricp-3.x-4.x"},
		{name: "numActive", value: 2, framework: "dbcp-2.x"},
		{name: "customMetric", value: 3, framework: "custom-pool"},
	} {
		metric := actualMetrics.At(i)
		assert.Equal(t, expected.name, metric.Name())
		assert.Equal(t, pmetric.MetricDataTypeGauge, metric.DataType())
		dataPoint := metric.Gauge().DataPoints().At(0)
		assert.Equal(t, expected.value, dataPoint.DoubleVal())
		assert.Equal(t, microsecondsToTimestamp(timestamp), dataPoint.Timestamp())
		framework, ok := dataPoint.Attributes().Get("type")
		require.True(t, ok)
		assert.Equal(t, expected.framework, framework.AsString())
	}
}

func TestMeterConverterKeepsValidLabels(t *testing.T) {
	tests := []struct {
		name     string
		labels   []*agentv3.Label
		expected map[string]string
	}{
		{
			name: "ignore nil and empty label names",
			labels: []*agentv3.Label{
				nil,
				{Name: "  ", Value: "ignored"},
				{Name: "type", Value: "dbcp-2.x"},
			},
			expected: map[string]string{"type": "dbcp-2.x"},
		},
		{
			name: "deduplicate labels with identical values",
			labels: []*agentv3.Label{
				{Name: "type", Value: "dbcp-2.x"},
				{Name: "type", Value: "dbcp-2.x"},
			},
			expected: map[string]string{"type": "dbcp-2.x"},
		},
		{
			name: "trim label names",
			labels: []*agentv3.Label{
				{Name: " type ", Value: "dbcp-2.x"},
				{Name: " name ", Value: "orders-db"},
			},
			expected: map[string]string{"type": "dbcp-2.x", "name": "orders-db"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter := newMeterConverter("service", "instance", 1758276000000, "my-token")
			converted := converter.Convert(&agentv3.MeterData{Metric: &agentv3.MeterData_SingleValue{
				SingleValue: &agentv3.MeterSingleValue{
					Name:   "activeConnections",
					Labels: tt.labels,
					Value:  1,
				},
			}})
			require.True(t, converted)

			attributes := converter.Get().ResourceMetrics().At(0).ScopeMetrics().At(0).
				Metrics().At(0).Gauge().DataPoints().At(0).Attributes()
			assert.Equal(t, len(tt.expected), attributes.Len())
			for key, expected := range tt.expected {
				actual, ok := attributes.Get(key)
				require.True(t, ok)
				assert.Equal(t, expected, actual.AsString())
			}
		})
	}
}

func TestMeterConverterDropInvalidData(t *testing.T) {
	tests := []struct {
		name  string
		meter *agentv3.MeterData
	}{
		{name: "nil meter"},
		{name: "empty name", meter: newSingleValueMeter("", 1, nil)},
		{name: "invalid value", meter: newSingleValueMeter("activeConnections", math.NaN(), nil)},
		{name: "no labels", meter: newSingleValueMeter("activeConnections", 1, nil)},
		{name: "all labels invalid", meter: &agentv3.MeterData{Metric: &agentv3.MeterData_SingleValue{
			SingleValue: &agentv3.MeterSingleValue{
				Name:   "activeConnections",
				Labels: []*agentv3.Label{nil, {Name: " ", Value: "ignored"}},
				Value:  1,
			},
		}}},
		{name: "conflicting duplicate labels", meter: &agentv3.MeterData{Metric: &agentv3.MeterData_SingleValue{
			SingleValue: &agentv3.MeterSingleValue{
				Name: "activeConnections",
				Labels: []*agentv3.Label{
					{Name: "type", Value: "first"},
					{Name: "type", Value: "second"},
				},
				Value: 1,
			},
		}}},
		{name: "unsupported histogram", meter: &agentv3.MeterData{Metric: &agentv3.MeterData_Histogram{
			Histogram: &agentv3.MeterHistogram{
				Name: "requestDuration",
			},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter := newMeterConverter("service", "instance", 1758276000000, "my-token")
			assert.False(t, converter.Convert(tt.meter))
			assert.Equal(t, 0, converter.Get().MetricCount())
		})
	}
}

func newSingleValueMeter(name string, value float64, labels map[string]string) *agentv3.MeterData {
	meterLabels := make([]*agentv3.Label, 0, len(labels))
	for key, val := range labels {
		meterLabels = append(meterLabels, &agentv3.Label{Name: key, Value: val})
	}
	return &agentv3.MeterData{Metric: &agentv3.MeterData_SingleValue{
		SingleValue: &agentv3.MeterSingleValue{Name: name, Value: value, Labels: meterLabels},
	}}
}

func assertResourceString(t *testing.T, resource pcommon.Map, key, expected string) {
	t.Helper()
	value, ok := resource.Get(key)
	require.True(t, ok)
	assert.Equal(t, expected, value.AsString())
}
