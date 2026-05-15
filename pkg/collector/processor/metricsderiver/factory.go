// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metricsderiver

import (
	"regexp"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/confengine"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/mapstructure"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/metricsbuilder"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/processor"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
)

func init() {
	processor.Register(define.ProcessorMetricsDeriver, NewFactory)
}

func NewFactory(conf map[string]any, customized []processor.SubConfigProcessor) (processor.Processor, error) {
	return newFactory(conf, customized)
}

func newFactory(conf map[string]any, customized []processor.SubConfigProcessor) (*metricsDeriver, error) {
	configs := confengine.NewTierConfig()

	c := &Config{}
	if err := mapstructure.Decode(conf, c); err != nil {
		return nil, err
	}
	configs.SetGlobal(*c)

	for _, custom := range customized {
		cfg := &Config{}
		if err := mapstructure.Decode(custom.Config.Config, cfg); err != nil {
			logger.Errorf("failed to decode config: %v", err)
			continue
		}
		configs.Set(custom.Token, custom.Type, custom.ID, *cfg)
	}

	return &metricsDeriver{
		CommonProcessor: processor.NewCommonProcessor(conf, customized),
		configs:         configs,
		metricBuilder:   NewMetricBuilder(),
		mu:              sync.Mutex{},
	}, nil
}

type metricsDeriver struct {
	processor.CommonProcessor
	configs       *confengine.TierConfig // type: Config
	metricBuilder *MetricBuilder
	mu            sync.Mutex
}

func (p *metricsDeriver) Name() string {
	return define.ProcessorMetricsDeriver
}

func (p *metricsDeriver) IsDerived() bool {
	return false
}

func (p *metricsDeriver) IsPreCheck() bool {
	return false
}

func (p *metricsDeriver) Reload(config map[string]any, customized []processor.SubConfigProcessor) {
	f, err := newFactory(config, customized)
	if err != nil {
		logger.Errorf("failed to reload processor: %v", err)
		return
	}

	p.CommonProcessor = f.CommonProcessor
	p.configs = f.configs
	p.metricBuilder = f.metricBuilder
}

func (p *metricsDeriver) Process(record *define.Record) (*define.Record, error) {
	config := p.configs.GetByToken(record.Token.Original).(Config)
	if config.OTJavaAgent.Enabled {
		p.otMetricDerive(record, config)
	}

	return nil, nil
}

func (p *metricsDeriver) otMetricDerive(record *define.Record, config Config) {
	if record.RecordType == define.RecordMetrics {
		pdMetrics := record.Data.(pmetric.Metrics)
		pdMetrics.ResourceMetrics().RemoveIf(func(resourceMetrics pmetric.ResourceMetrics) bool {
			rs := resourceMetrics.Resource().Attributes()
			if !IsOtJavaAgentDatasource(rs) {
				return false
			}

			// 仅对来自 OT Java Agent 的指标数据进行处理
			resourceMetrics.ScopeMetrics().RemoveIf(func(scopeMetrics pmetric.ScopeMetrics) bool {
				p.mu.Lock()
				p.metricBuilder.metricSlice = p.metricBuilder.metricSlice[:0]
				scopeMetrics.Metrics().RemoveIf(func(metric pmetric.Metric) bool {
					switch metric.Name() {
					case jvmGcDuration:
						p.processGcMetrics(metric)
						return !config.OTJavaAgent.KeepOriginMetric
					case jvmMemoryInit, jvmMemoryUsed, jvmMemoryCommitted, jvmMemoryLimit:
						p.processMemoryMetrics(metric)
						return !config.OTJavaAgent.KeepOriginMetric
					case jvmThreadCount:
						p.processThreadMetrics(metric)
						return !config.OTJavaAgent.KeepOriginMetric
					default:
						return false
					}
				})
				p.metricBuilder.build(scopeMetrics)
				p.mu.Unlock()
				return scopeMetrics.Metrics().Len() == 0
			})
			return resourceMetrics.ScopeMetrics().Len() == 0
		})
	}
}

func IsOtJavaAgentDatasource(rs pcommon.Map) bool {
	val, ok := rs.Get(resourceTelemetryDistroName)
	if ok && val.AsString() == "opentelemetry-java-instrumentation" {
		return true
	}
	return false
}

func (p *metricsDeriver) processGcMetrics(metric pmetric.Metric) {
	// 仅处理 Histogram 类型的 jvm.gc.duration 指标
	if metric.DataType() != pmetric.MetricDataTypeHistogram {
		logger.Errorf("jvm metric(%s) process failed, unsupported data type: %s", metric.Name(), metric.DataType())
		return
	}

	youngGcMapping := make(map[pcommon.Timestamp]*JvmGcDataPoint)
	oldGcMapping := make(map[pcommon.Timestamp]*JvmGcDataPoint)

	aggregateGcDataPoints(metric.Histogram().DataPoints(), youngGcMapping, oldGcMapping)
	p.buildGcMetrics(youngGcMapping, jvmGcYoungCount, jvmGcYoungTime)
	p.buildGcMetrics(oldGcMapping, jvmGcOldCount, jvmGcOldTime)
}

func aggregateGcDataPoints(dps pmetric.HistogramDataPointSlice, youngGcMapping, oldGcMapping map[pcommon.Timestamp]*JvmGcDataPoint) {
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		gcName, ok := dp.Attributes().Get(jvmGcName)
		if !ok {
			continue
		}

		timestamp := dp.Timestamp()
		if timestamp == 0 {
			timestamp = dp.StartTimestamp()
		}
		count := dp.Count()
		sum := dp.Sum()

		var targetMapping map[pcommon.Timestamp]*JvmGcDataPoint
		switch gcName.AsString() {
		case "ParNew", "G1 Young Generation", "PS Scavenge", "Copy":
			targetMapping = youngGcMapping
		case "ConcurrentMarkSweep", "G1 Old Generation", "PS MarkSweep", "MarkSweepCompact":
			targetMapping = oldGcMapping
		default:
			continue
		}

		updateGcMapping(targetMapping, timestamp, count, sum)
	}
}

func updateGcMapping(mapping map[pcommon.Timestamp]*JvmGcDataPoint, timestamp pcommon.Timestamp, count uint64, sum float64) {
	if dataPoint, exists := mapping[timestamp]; exists {
		dataPoint.countVal += count
		dataPoint.timeVal += sum
	} else {
		mapping[timestamp] = &JvmGcDataPoint{countVal: count, timeVal: sum}
	}
}

func (p *metricsDeriver) buildGcMetrics(gcMapping map[pcommon.Timestamp]*JvmGcDataPoint, countMetricName, timeMetricName string) {
	for ts, dataPoint := range gcMapping {
		p.metricBuilder.metricSlice = append(p.metricBuilder.metricSlice,
			&MetricItem{
				Name: countMetricName,
				Metric: metricsbuilder.Metric{
					Val: float64(dataPoint.countVal),
					Ts:  ts,
				},
			},
			&MetricItem{
				Name: timeMetricName,
				Metric: metricsbuilder.Metric{
					Val: dataPoint.timeVal * 1000, // 转换为毫秒
					Ts:  ts,
				},
			},
		)
	}
}

func foreachSumDataPoints(metric pmetric.Metric, f func(attrs pcommon.Map, ts pcommon.Timestamp, val int64)) {
	dps := metric.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		ts := dp.Timestamp()
		if ts == 0 {
			ts = dp.StartTimestamp()
		}
		f(dp.Attributes(), ts, dp.IntVal())
	}
}

func (p *metricsDeriver) processMemoryMetrics(metric pmetric.Metric) {
	// 仅处理 Sum 类型的 jvm.memory.xxx 指标
	if metric.DataType() != pmetric.MetricDataTypeSum {
		logger.Errorf("jvm metric(%s) process failed, unsupported data type: %s", metric.Name(), metric.DataType())
		return
	}

	switch metric.Name() {
	case jvmMemoryInit:
		p.processInitMemoryMetrics(metric)
	case jvmMemoryUsed:
		p.processUsedMemoryMetrics(metric)
	case jvmMemoryCommitted:
		p.processCommittedMemoryMetrics(metric)
	case jvmMemoryLimit:
		p.processLimitMemoryMetrics(metric)
	}
}

func (p *metricsDeriver) processInitMemoryMetrics(metric pmetric.Metric) {
	mappings := newMemoryMappings(false, false, false)

	foreachSumDataPoints(metric, func(attrs pcommon.Map, ts pcommon.Timestamp, val int64) {
		memType, ok := attrs.Get(jvmMemoryType)
		if !ok || memType.AsString() != "non_heap" {
			return
		}
		processNonHeapMemory(attrs, ts, val, mappings)
	})

	p.buildMetrics(mappings.nonHeap, jvmMemoryNoHeapInit)
	p.buildMetrics(mappings.metaspace, jvmMemoryMetaspaceInit)
	p.buildMetrics(mappings.codeCache, jvmMemoryCodeCacheInit)
}

func (p *metricsDeriver) processUsedMemoryMetrics(metric pmetric.Metric) {
	mappings := newMemoryMappings(true, false, false)

	foreachSumDataPoints(metric, func(attrs pcommon.Map, ts pcommon.Timestamp, val int64) {
		processMemoryByType(attrs, ts, val, mappings, false)
	})

	p.buildMetrics(mappings.heap, jvmMemoryHeapUsed)
	p.buildMetrics(mappings.nonHeap, jvmMemoryNoHeapUsed)
	p.buildMetrics(mappings.metaspace, jvmMemoryMetaspaceUsed)
	p.buildMetrics(mappings.codeCache, jvmMemoryCodeCacheUsed)
}

func (p *metricsDeriver) processCommittedMemoryMetrics(metric pmetric.Metric) {
	mappings := newMemoryMappings(true, true, true)

	foreachSumDataPoints(metric, func(attrs pcommon.Map, ts pcommon.Timestamp, val int64) {
		processMemoryByType(attrs, ts, val, mappings, true)
	})

	p.buildMetrics(mappings.heap, jvmMemoryHeapCommitted)
	p.buildMetrics(mappings.nonHeap, jvmMemoryNoHeapCommitted)
	p.buildMetrics(mappings.metaspace, jvmMemoryMetaspaceCommitted)
	p.buildMetrics(mappings.codeCache, jvmMemoryCodeCacheCommitted)
	p.buildMetrics(mappings.newgen, jvmMemoryNewGenCommitted)
	p.buildMetrics(mappings.oldgen, jvmMemoryOldGenCommitted)
	p.buildMetrics(mappings.survivor, jvmMemorySurvivorCommitted)
}

func (p *metricsDeriver) processLimitMemoryMetrics(metric pmetric.Metric) {
	mappings := newMemoryMappings(true, false, false)

	foreachSumDataPoints(metric, func(attrs pcommon.Map, ts pcommon.Timestamp, val int64) {
		processMemoryByType(attrs, ts, val, mappings, false)
	})

	p.buildMetrics(mappings.heap, jvmMemoryHeapMax)
	p.buildMetrics(mappings.nonHeap, jvmMemoryNoHeapMax)
	p.buildMetrics(mappings.metaspace, jvmMemoryMetaspaceMax)
	p.buildMetrics(mappings.codeCache, jvmMemoryCodeCacheMax)
}

func processMemoryByType(attrs pcommon.Map, ts pcommon.Timestamp, val int64, mappings *memoryMappings, includeHeapPool bool) {
	memType, ok := attrs.Get(jvmMemoryType)
	if !ok {
		return
	}

	switch memType.AsString() {
	case "heap":
		mappings.heap[ts] += val
		if includeHeapPool {
			if poolName, exists := attrs.Get(jvmMemoryPoolName); exists {
				processHeapPoolMapping(poolName.AsString(), ts, val, mappings, survivorPattern)
			}
		}
	case "non_heap":
		processNonHeapMemory(attrs, ts, val, mappings)
	}
}

func processNonHeapMemory(attrs pcommon.Map, ts pcommon.Timestamp, val int64, mappings *memoryMappings) {
	mappings.nonHeap[ts] += val

	poolName, exists := attrs.Get(jvmMemoryPoolName)
	if !exists {
		return
	}

	switch poolName.AsString() {
	case "Metaspace":
		mappings.metaspace[ts] += val
	case "Code Cache", "CodeCache":
		mappings.codeCache[ts] += val
	}
}

func processHeapPoolMapping(poolName string, ts pcommon.Timestamp, val int64, mappings *memoryMappings, survivorPattern *regexp.Regexp) {
	if newGenPools[poolName] {
		mappings.newgen[ts] += val
	} else if oldGenPools[poolName] {
		mappings.oldgen[ts] += val
	}

	// Survivor 划分比较特殊，使用正则表达式匹配
	if survivorPattern.MatchString(poolName) {
		mappings.survivor[ts] += val
	}
}

func (p *metricsDeriver) buildMetrics(dataPointMapping map[pcommon.Timestamp]int64, metricName string) {
	for ts, dataPoint := range dataPointMapping {
		p.metricBuilder.metricSlice = append(p.metricBuilder.metricSlice,
			&MetricItem{
				Name: metricName,
				Metric: metricsbuilder.Metric{
					Val: float64(dataPoint),
					Ts:  ts,
				},
			},
		)
	}
}

func (p *metricsDeriver) processThreadMetrics(metric pmetric.Metric) {
	// 仅处理 Sum 类型的 jvm.thread.count 指标
	if metric.DataType() != pmetric.MetricDataTypeSum {
		logger.Errorf("jvm metric(%s) process failed, unsupported data type: %s", metric.Name(), metric.DataType())
		return
	}

	mappings := newThreadMappings()

	foreachSumDataPoints(metric, func(attrs pcommon.Map, ts pcommon.Timestamp, val int64) {
		mappings.live[ts] += val
		processThreadByState(attrs, ts, val, mappings)
		processDaemonThread(attrs, ts, val, mappings)
	})

	p.buildMetrics(mappings.runnable, jvmThreadRunnableCount)
	p.buildMetrics(mappings.blocked, jvmThreadBlockedCount)
	p.buildMetrics(mappings.waiting, jvmThreadWaitingCount)
	p.buildMetrics(mappings.timedWaiting, jvmThreadTimeWaitingCount)
	p.buildMetrics(mappings.daemon, jvmThreadDaemonCount)
	p.buildMetrics(mappings.live, jvmThreadLiveCount)
}

func processThreadByState(attrs pcommon.Map, ts pcommon.Timestamp, val int64, mappings *threadMappings) {
	state, ok := attrs.Get(jvmThreadState)
	if !ok {
		return
	}

	switch state.AsString() {
	case "runnable":
		mappings.runnable[ts] += val
	case "blocked":
		mappings.blocked[ts] += val
	case "waiting":
		mappings.waiting[ts] += val
	case "timed_waiting":
		mappings.timedWaiting[ts] += val
	}
}

func processDaemonThread(attrs pcommon.Map, ts pcommon.Timestamp, val int64, mappings *threadMappings) {
	isDaemon, ok := attrs.Get(jvmThreadDaemon)
	if !ok || !isDaemon.BoolVal() {
		return
	}

	mappings.daemon[ts] += val
}
