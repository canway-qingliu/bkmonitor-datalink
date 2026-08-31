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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
)

const (
	frameworkTypeAttribute = "type"
	dropReasonMissingType  = "missing_type"
	dropReasonNotMapped    = "not_mapped"
	dropReasonInvalid      = "invalid"
)

type meterProcessResult struct {
	matched           bool
	missingType       int
	notMapped         int
	invalid           int
	missingTypeMeters []meterDropDetail
	notMappedMeters   []meterDropDetail
	invalidMeters     []meterDropDetail
}

type meterDropDetail struct {
	MetricName string
	DataType   string
	Types      []any
	Dimensions []map[string]any
}

func (r *meterProcessResult) addDrop(metric pmetric.Metric, reason string) {
	detail := newMeterDropDetail(metric)
	switch reason {
	case dropReasonMissingType:
		r.missingType++
		r.missingTypeMeters = append(r.missingTypeMeters, detail)
	case dropReasonNotMapped:
		r.notMapped++
		r.notMappedMeters = append(r.notMappedMeters, detail)
	default:
		r.invalid++
		r.invalidMeters = append(r.invalidMeters, detail)
	}
}

func newMeterDropDetail(metric pmetric.Metric) meterDropDetail {
	detail := meterDropDetail{
		MetricName: metric.Name(),
		DataType:   metric.DataType().String(),
	}
	if metric.DataType() != pmetric.MetricDataTypeGauge {
		return detail
	}

	dataPoints := metric.Gauge().DataPoints()
	for i := 0; i < dataPoints.Len(); i++ {
		dimensions := dataPoints.At(i).Attributes().AsRaw()
		detail.Dimensions = append(detail.Dimensions, dimensions)
		framework, ok := dimensions[frameworkTypeAttribute]
		switch {
		case !ok:
			detail.Types = append(detail.Types, "<missing>")
		case framework == "":
			detail.Types = append(detail.Types, "<empty>")
		default:
			detail.Types = append(detail.Types, framework)
		}
	}
	return detail
}

func (p *skywalkingProcessor) meterDataAction(record *define.Record, action MeterDataAction) error {
	if record.RecordType != define.RecordMetrics {
		return nil
	}

	metrics := record.Data.(pmetric.Metrics)
	if !action.Enabled {
		if hasSkywalkingMeterData(metrics) {
			return define.ErrSkipEmptyRecord
		}
		return nil
	}

	result := processMeterData(metrics, action)
	if !result.matched {
		return nil
	}
	if metrics.MetricCount() == 0 {
		logger.Debugf(
			"skywalking meter data filtered empty: app_name=%q, missing_type=%d, not_mapped=%d, invalid=%d, "+
				"missing_type_metrics=%+v, not_mapped_metrics=%+v, invalid_metrics=%+v",
			record.Token.AppName,
			result.missingType,
			result.notMapped,
			result.invalid,
			result.missingTypeMeters,
			result.notMappedMeters,
			result.invalidMeters,
		)
		return define.ErrEndOfPipeline
	}
	return nil
}

func hasSkywalkingMeterData(metrics pmetric.Metrics) bool {
	resourceMetrics := metrics.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		if isSkywalkingMeterDataResource(resourceMetrics.At(i)) {
			return true
		}
	}
	return false
}

func processMeterData(metrics pmetric.Metrics, action MeterDataAction) meterProcessResult {
	result := meterProcessResult{}
	metrics.ResourceMetrics().RemoveIf(func(resourceMetrics pmetric.ResourceMetrics) bool {
		if !isSkywalkingMeterDataResource(resourceMetrics) {
			return false
		}
		result.matched = true

		resourceMetrics.ScopeMetrics().RemoveIf(func(scopeMetrics pmetric.ScopeMetrics) bool {
			scopeMetrics.Metrics().RemoveIf(func(metric pmetric.Metric) bool {
				framework, reason, ok := metricFramework(metric)
				if !ok {
					result.addDrop(metric, reason)
					return true
				}

				destination, ok := action.Match(framework, metric.Name())
				if !ok {
					result.addDrop(metric, dropReasonNotMapped)
					return true
				}
				metric.SetName(destination)
				return false
			})
			return scopeMetrics.Metrics().Len() == 0
		})

		if resourceMetrics.ScopeMetrics().Len() == 0 {
			return true
		}
		resourceMetrics.Resource().Attributes().Remove(define.ResourceAttributeDataSource)
		return false
	})
	return result
}

func isSkywalkingMeterDataResource(resourceMetrics pmetric.ResourceMetrics) bool {
	source, ok := resourceMetrics.Resource().Attributes().Get(define.ResourceAttributeDataSource)
	return ok && source.AsString() == define.SourceSkywalkingMeterData
}

func metricFramework(metric pmetric.Metric) (string, string, bool) {
	var framework string
	var found bool
	check := func(attributes pcommon.Map) string {
		value, ok := attributes.Get(frameworkTypeAttribute)
		if !ok || value.AsString() == "" {
			return dropReasonMissingType
		}
		if !found {
			framework = value.AsString()
			found = true
			return ""
		}
		if framework != value.AsString() {
			return dropReasonInvalid
		}
		return ""
	}

	if metric.DataType() != pmetric.MetricDataTypeGauge {
		return "", dropReasonInvalid, false
	}
	dataPoints := metric.Gauge().DataPoints()
	for i := 0; i < dataPoints.Len(); i++ {
		if reason := check(dataPoints.At(i).Attributes()); reason != "" {
			return "", reason, false
		}
	}
	if !found {
		return "", dropReasonInvalid, false
	}
	return framework, "", true
}
