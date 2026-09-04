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
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	agentv3 "skywalking.apache.org/repo/goapi/collect/language/agent/v3"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/utils"
)

func newMeterConverter(service, instance string, ts int64, token string) *meterConverter {
	pbMetrics := pmetric.NewMetrics()
	resourceMetrics := pbMetrics.ResourceMetrics().AppendEmpty()
	resource := resourceMetrics.Resource().Attributes()
	utils.InsertString(resource, "service_name", service)
	utils.InsertString(resource, "bk_instance_id", instance)
	utils.InsertString(resource, "bk.data.token", token)
	utils.InsertString(resource, define.ResourceAttributeDataSource, define.SourceSkywalkingMeterData)

	return &meterConverter{
		pbMetrics: pbMetrics,
		metrics:   resourceMetrics.ScopeMetrics().AppendEmpty().Metrics(),
		timestamp: ts,
	}
}

type meterConverter struct {
	timestamp int64
	pbMetrics pmetric.Metrics
	metrics   pmetric.MetricSlice
}

func (c *meterConverter) Get() pmetric.Metrics {
	return c.pbMetrics
}

func (c *meterConverter) Convert(meter *agentv3.MeterData) bool {
	if meter == nil {
		return false
	}

	return c.convertSingleValue(meter.GetSingleValue())
}

func (c *meterConverter) convertSingleValue(value *agentv3.MeterSingleValue) bool {
	if value == nil || strings.TrimSpace(value.GetName()) == "" || !isFinite(value.GetValue()) {
		return false
	}
	attributes := pcommon.NewMap()
	if !copyMeterLabels(value.GetLabels(), attributes) {
		return false
	}

	metric := c.metrics.AppendEmpty()
	metric.SetName(value.GetName())
	metric.SetEmptyGauge()
	dataPoint := metric.Gauge().DataPoints().AppendEmpty()
	dataPoint.SetDoubleVal(value.GetValue())
	dataPoint.SetTimestamp(microsecondsToTimestamp(c.timestamp))
	attributes.CopyTo(dataPoint.Attributes())
	return true
}

func copyMeterLabels(labels []*agentv3.Label, attributes pcommon.Map) bool {
	seen := make(map[string]string, len(labels))
	copied := 0
	for _, label := range labels {
		if label == nil {
			continue
		}

		name := strings.TrimSpace(label.GetName())
		if name == "" {
			continue
		}
		value := label.GetValue()
		if previous, ok := seen[name]; ok {
			if previous == value {
				continue
			}
			return false
		}

		seen[name] = value
		utils.InsertString(attributes, name, value)
		copied++
	}
	return copied > 0
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
