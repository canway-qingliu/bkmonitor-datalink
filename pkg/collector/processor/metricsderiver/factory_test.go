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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/mapstructure"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/testkits"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/processor"
)

func TestFactory(t *testing.T) {
	content := `
processor:
  - name: "metrics_deriver/ot_java_agent"
    config:
      ot_java_agent:
        enabled: false
        keep_origin_metric: true
`
	mainConf := processor.MustLoadConfigs(content)[0].Config

	customContent := `
processor:
  - name: "metrics_deriver/ot_java_agent"
    config:
      ot_java_agent:
        enabled: true
        keep_origin_metric: false
`
	customConf := processor.MustLoadConfigs(customContent)[0].Config

	obj, err := NewFactory(mainConf, []processor.SubConfigProcessor{
		{
			Token: "token1",
			Type:  define.SubConfigFieldDefault,
			Config: processor.Config{
				Config: customConf,
			},
		},
	})
	factory := obj.(*metricsDeriver)
	assert.NoError(t, err)
	assert.Equal(t, mainConf, factory.MainConfig())
	assert.Equal(t, customConf, factory.SubConfigs()[0].Config.Config)

	c := &Config{}
	assert.NoError(t, mapstructure.Decode(mainConf, c))

	assert.Equal(t, define.ProcessorMetricsDeriver, factory.Name())
	assert.False(t, factory.IsDerived())
	assert.False(t, factory.IsPreCheck())

	factory.Reload(mainConf, nil)
	assert.Equal(t, mainConf, factory.MainConfig())
}

func makeMetricsRecord(resource map[string]string) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()

	for k, v := range resource {
		rm.Resource().Attributes().UpsertString(k, v)
	}
	return metrics
}

func addHistogramMetric(metrics pmetric.MetricSlice, name string, attrs map[string]string, count uint64, sum float64) {
	metric := metrics.AppendEmpty()
	metric.SetName(name)
	metric.SetDataType(pmetric.MetricDataTypeHistogram)

	dp := metric.Histogram().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetCount(count)
	dp.SetSum(sum)

	for k, v := range attrs {
		dp.Attributes().UpsertString(k, v)
	}
}

func addSumMetric(metrics pmetric.MetricSlice, name string, attrs map[string]string, value int64) {
	metric := metrics.AppendEmpty()
	metric.SetName(name)
	metric.SetDataType(pmetric.MetricDataTypeSum)

	dp := metric.Sum().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntVal(value)

	for k, v := range attrs {
		dp.Attributes().UpsertString(k, v)
	}
}

func makeHistogramMetricData(metricName string, attrs map[string]string, count uint64, sum float64) pmetric.Metric {
	metric := pmetric.NewMetric()
	metric.SetName(metricName)
	metric.SetDataType(pmetric.MetricDataTypeHistogram)

	dp := metric.Histogram().DataPoints().AppendEmpty()
	for k, v := range attrs {
		dp.Attributes().UpsertString(k, v)
	}
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetCount(count)
	dp.SetSum(sum)

	return metric
}

func makeSumMetricData(metricName string, attrs map[string]pcommon.Value, val int64) pmetric.Metric {
	metric := pmetric.NewMetric()
	metric.SetName(metricName)
	metric.SetDataType(pmetric.MetricDataTypeSum)

	dp := metric.Sum().DataPoints().AppendEmpty()
	for k, v := range attrs {
		dp.Attributes().Upsert(k, v)
	}
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntVal(val)

	return metric
}

func TestMetricDerive(t *testing.T) {
	content := `
processor:
  - name: "metrics_deriver/ot_java_agent"
    config:
      ot_java_agent:
        enabled: true
        keep_origin_metric: false
`

	t.Run("Process JVM Metrics", func(t *testing.T) {
		metricsRecord := makeMetricsRecord(map[string]string{
			resourceTelemetryDistroName: "opentelemetry-java-instrumentation",
		})
		rm := metricsRecord.ResourceMetrics().At(0)
		sm := rm.ScopeMetrics().AppendEmpty()
		metricSlice := sm.Metrics()

		// Add GC metric
		addHistogramMetric(metricSlice, jvmGcDuration, map[string]string{
			jvmGcName: "G1 Young Generation",
		}, 10, 0.5)

		// Add Memory metric
		addSumMetric(metricSlice, jvmMemoryUsed, map[string]string{
			jvmMemoryType: "heap",
		}, 1024000)

		// Add Thread metric
		addSumMetric(metricSlice, jvmThreadCount, map[string]string{
			jvmThreadState:  "runnable",
			jvmThreadDaemon: "false",
		}, 5)

		factory := processor.MustCreateFactory(content, NewFactory)

		record := define.Record{
			RecordType: define.RecordMetrics,
			Data:       metricsRecord,
			Token:      define.Token{Original: "test-token"},
		}

		testkits.MustProcess(t, factory, record)
		metrics := record.Data.(pmetric.Metrics).ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
		assert.Equal(t, 5, metrics.Len())
	})

	t.Run("Process Non-OT Java Agent Metrics", func(t *testing.T) {
		metricsRecord := makeMetricsRecord(map[string]string{resourceTelemetryDistroName: "other"})

		record := define.Record{
			RecordType: define.RecordMetrics,
			Data:       metricsRecord,
			Token:      define.Token{Original: "test-token"},
		}

		factory := processor.MustCreateFactory(content, NewFactory)
		testkits.MustProcess(t, factory, record)
	})
}

func TestProcessGcMetrics(t *testing.T) {
	content := `
processor:
  - name: "metrics_deriver/ot_java_agent"
    config:
      ot_java_agent:
        enabled: true
        keep_origin_metric: false
`
	tests := []struct {
		name        string
		gcName      string
		count       uint64
		sum         float64
		expectYoung bool
		expectOld   bool
	}{
		{
			name:        "Young GC - G1 Young Generation",
			gcName:      "G1 Young Generation",
			count:       5,
			sum:         0.3,
			expectYoung: true,
			expectOld:   false,
		},
		{
			name:        "Old GC - G1 Old Generation",
			gcName:      "G1 Old Generation",
			count:       3,
			sum:         2.0,
			expectYoung: false,
			expectOld:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := makeHistogramMetricData(jvmGcDuration, map[string]string{
				jvmGcName: tt.gcName,
			}, tt.count, tt.sum)

			factory := processor.MustCreateFactory(content, NewFactory)
			p := factory.(*metricsDeriver)
			p.processGcMetrics(metric)

			assert.Equal(t, len(p.metricBuilder.metricSlice), 2)

			foundYoungCount := false
			foundYoungTime := false
			foundOldCount := false
			foundOldTime := false

			for _, m := range p.metricBuilder.metricSlice {
				switch m.Name {
				case jvmGcYoungCount:
					foundYoungCount = true
					assert.Equal(t, float64(tt.count), m.Val)
				case jvmGcYoungTime:
					foundYoungTime = true
					assert.Equal(t, tt.sum*1000, m.Val)
				case jvmGcOldCount:
					foundOldCount = true
					assert.Equal(t, float64(tt.count), m.Val)
				case jvmGcOldTime:
					foundOldTime = true
					assert.Equal(t, tt.sum*1000, m.Val)
				}
			}

			if tt.expectYoung {
				assert.True(t, foundYoungCount, "Expected young GC count metric")
				assert.True(t, foundYoungTime, "Expected young GC time metric")
			}
			if tt.expectOld {
				assert.True(t, foundOldCount, "Expected old GC count metric")
				assert.True(t, foundOldTime, "Expected old GC time metric")
			}
		})
	}
}

func TestProcessMemoryMetrics(t *testing.T) {
	content := `
processor:
  - name: "metrics_deriver/ot_java_agent"
    config:
      ot_java_agent:
        enabled: true
        keep_origin_metric: false
`

	tests := []struct {
		name            string
		metricName      string
		memType         string
		poolName        string
		value           int64
		expectedMetrics []string
	}{
		{
			name:            "Heap Memory Used",
			metricName:      jvmMemoryUsed,
			memType:         "heap",
			poolName:        "",
			value:           1024000,
			expectedMetrics: []string{jvmMemoryHeapUsed},
		},
		{
			name:            "Memory Used - No Memory Type",
			metricName:      jvmMemoryUsed,
			memType:         "",
			poolName:        "",
			value:           1024000,
			expectedMetrics: []string{},
		},
		{
			name:            "Non-Heap Memory Used - Metaspace",
			metricName:      jvmMemoryUsed,
			memType:         "non_heap",
			poolName:        "Metaspace",
			value:           512000,
			expectedMetrics: []string{jvmMemoryNoHeapUsed, jvmMemoryMetaspaceUsed},
		},
		{
			name:            "Non-Heap Memory Used - No Pool Name",
			metricName:      jvmMemoryUsed,
			memType:         "non_heap",
			poolName:        "",
			value:           256000,
			expectedMetrics: []string{jvmMemoryNoHeapUsed},
		},
		{
			name:            "Non-Heap Memory Used - Other Pool Name",
			metricName:      jvmMemoryUsed,
			memType:         "non_heap",
			poolName:        "other",
			value:           256000,
			expectedMetrics: []string{jvmMemoryNoHeapUsed},
		},
		{
			name:            "Heap Memory Committed - New Gen and Survivor",
			metricName:      jvmMemoryCommitted,
			memType:         "heap",
			poolName:        "G1 Survivor Space",
			value:           2048000,
			expectedMetrics: []string{jvmMemoryHeapCommitted, jvmMemoryNewGenCommitted, jvmMemorySurvivorCommitted},
		},
		{
			name:            "Heap Memory Committed - No Pool Name",
			metricName:      jvmMemoryCommitted,
			memType:         "heap",
			poolName:        "",
			value:           2048000,
			expectedMetrics: []string{jvmMemoryHeapCommitted},
		},
		{
			name:            "Heap Memory Committed - Old Gen",
			metricName:      jvmMemoryCommitted,
			memType:         "heap",
			poolName:        "G1 Old Gen",
			value:           4096000,
			expectedMetrics: []string{jvmMemoryHeapCommitted, jvmMemoryOldGenCommitted},
		},
		{
			name:            "Heap Memory Committed - Survivor",
			metricName:      jvmMemoryCommitted,
			memType:         "heap",
			poolName:        "Shenandoah Survivor",
			value:           512000,
			expectedMetrics: []string{jvmMemoryHeapCommitted, jvmMemorySurvivorCommitted},
		},
		{
			name:            "Heap Memory Committed - Survivor without Space",
			metricName:      jvmMemoryCommitted,
			memType:         "heap",
			poolName:        "PSSurvivorSpace",
			value:           512000,
			expectedMetrics: []string{jvmMemoryHeapCommitted, jvmMemorySurvivorCommitted},
		},
		{
			name:            "Heap Memory Committed - Survivor with lowercase",
			metricName:      jvmMemoryCommitted,
			memType:         "heap",
			poolName:        "survivor space",
			value:           512000,
			expectedMetrics: []string{jvmMemoryHeapCommitted, jvmMemorySurvivorCommitted},
		},
		{
			name:            "Heap Memory Init",
			metricName:      jvmMemoryInit,
			memType:         "heap",
			poolName:        "",
			value:           262144,
			expectedMetrics: []string{},
		},
		{
			name:            "Other Memory Limit",
			metricName:      jvmMemoryLimit,
			memType:         "other",
			poolName:        "",
			value:           8192000,
			expectedMetrics: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrMap := map[string]pcommon.Value{jvmMemoryType: pcommon.NewValueString(tt.memType)}
			if tt.poolName != "" {
				attrMap[jvmMemoryPoolName] = pcommon.NewValueString(tt.poolName)
			}
			metric := makeSumMetricData(tt.metricName, attrMap, tt.value)

			factory := processor.MustCreateFactory(content, NewFactory)
			p := factory.(*metricsDeriver)
			p.processMemoryMetrics(metric)

			assert.Equal(t, len(p.metricBuilder.metricSlice), len(tt.expectedMetrics))

			foundMetrics := make(map[string]bool)
			for _, m := range p.metricBuilder.metricSlice {
				foundMetrics[m.Name] = true
				assert.Equal(t, float64(tt.value), m.Val)
			}

			for _, expectedMetric := range tt.expectedMetrics {
				assert.True(t, foundMetrics[expectedMetric], "Expected metric: %s", expectedMetric)
			}
		})
	}
}

func TestProcessThreadMetrics(t *testing.T) {
	content := `
processor:
  - name: "metrics_deriver/ot_java_agent"
    config:
      ot_java_agent:
        enabled: true
        keep_origin_metric: false
`
	tests := []struct {
		name            string
		state           string
		hasDaemonType   bool
		isDaemon        bool
		value           int64
		expectedMetrics []string
	}{
		{
			name:            "Is not daemon with state - Runnable",
			state:           "runnable",
			hasDaemonType:   true,
			isDaemon:        false,
			value:           5,
			expectedMetrics: []string{jvmThreadRunnableCount, jvmThreadLiveCount},
		},
		{
			name:            "Is not daemon without stated",
			state:           "",
			hasDaemonType:   true,
			isDaemon:        false,
			value:           2,
			expectedMetrics: []string{jvmThreadLiveCount},
		},
		{
			name:            "Is not daemon with state - other state thread",
			state:           "other",
			hasDaemonType:   true,
			isDaemon:        false,
			value:           3,
			expectedMetrics: []string{jvmThreadLiveCount},
		},
		{
			name:            "Is daemon with state - Waiting",
			state:           "waiting",
			hasDaemonType:   true,
			isDaemon:        true,
			value:           1,
			expectedMetrics: []string{jvmThreadWaitingCount, jvmThreadLiveCount, jvmThreadDaemonCount},
		},
		{
			name:            "Is daemon - other state thread or without state",
			state:           "",
			hasDaemonType:   true,
			isDaemon:        true,
			value:           4,
			expectedMetrics: []string{jvmThreadDaemonCount, jvmThreadLiveCount},
		},
		{
			name:            "No daemon without state",
			state:           "",
			hasDaemonType:   false,
			isDaemon:        false,
			value:           4,
			expectedMetrics: []string{jvmThreadLiveCount},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrMap := make(map[string]pcommon.Value)
			if tt.state != "" {
				attrMap[jvmThreadState] = pcommon.NewValueString(tt.state)
			}
			if tt.hasDaemonType {
				attrMap[jvmThreadDaemon] = pcommon.NewValueBool(tt.isDaemon)
			}
			metric := makeSumMetricData(jvmThreadCount, attrMap, tt.value)

			factory := processor.MustCreateFactory(content, NewFactory)
			p := factory.(*metricsDeriver)
			p.processThreadMetrics(metric)

			assert.Equal(t, len(p.metricBuilder.metricSlice), len(tt.expectedMetrics))

			foundMetrics := make(map[string]bool)
			for _, m := range p.metricBuilder.metricSlice {
				foundMetrics[m.Name] = true
				assert.Equal(t, float64(tt.value), m.Val)
			}

			for _, expectedMetric := range tt.expectedMetrics {
				assert.True(t, foundMetrics[expectedMetric], "Expected metric: %s", expectedMetric)
			}
		})
	}
}

func addHistogramDataPoint(dps pmetric.HistogramDataPointSlice, attrs map[string]pcommon.Value,
	ts pcommon.Timestamp, count uint64, sum float64,
) {
	dp := dps.AppendEmpty()
	for k, v := range attrs {
		dp.Attributes().Upsert(k, v)
	}
	dp.SetStartTimestamp(ts)
	dp.SetCount(count)
	dp.SetSum(sum)
}

func TestAggregateGcDataPoints(t *testing.T) {
	youngGcMapping := make(map[pcommon.Timestamp]*JvmGcDataPoint)
	oldGcMapping := make(map[pcommon.Timestamp]*JvmGcDataPoint)

	dps := pmetric.NewHistogramDataPointSlice()
	now := pcommon.NewTimestampFromTime(time.Now())
	oneMinuteBefore := pcommon.NewTimestampFromTime(time.Now().Add(-time.Minute))

	// Add young GC data point
	addHistogramDataPoint(dps, map[string]pcommon.Value{jvmGcName: pcommon.NewValueString("ParNew")}, now, 10, 0.5)
	// Add old GC data point
	addHistogramDataPoint(dps, map[string]pcommon.Value{jvmGcName: pcommon.NewValueString("ConcurrentMarkSweep")}, now, 2, 1.5)
	// Add another young GC data point with same timestamp
	addHistogramDataPoint(dps, map[string]pcommon.Value{jvmGcName: pcommon.NewValueString("G1 Young Generation")}, now, 5, 0.3)
	// Add another young GC data point with another timestamp
	addHistogramDataPoint(dps, map[string]pcommon.Value{jvmGcName: pcommon.NewValueString("G1 Young Generation")}, oneMinuteBefore, 5, 0.3)
	// Add data point without jvm.gc.name
	addHistogramDataPoint(dps, map[string]pcommon.Value{}, now, 4, 0.2)
	// Add other GC data point
	addHistogramDataPoint(dps, map[string]pcommon.Value{jvmGcName: pcommon.NewValueString("other")}, now, 1, 0.1)

	aggregateGcDataPoints(dps, youngGcMapping, oldGcMapping)

	assert.Len(t, youngGcMapping, 2)
	assert.Len(t, oldGcMapping, 1)

	youngGc := youngGcMapping[now]
	assert.NotNil(t, youngGc)
	assert.Equal(t, uint64(15), youngGc.countVal) // 10 + 5
	assert.Equal(t, 0.8, youngGc.timeVal)         // 0.5 + 0.3

	oldGc := oldGcMapping[now]
	assert.NotNil(t, oldGc)
	assert.Equal(t, uint64(2), oldGc.countVal)
	assert.Equal(t, 1.5, oldGc.timeVal)
}

func TestProcessMemoryByType(t *testing.T) {
	tests := []struct {
		name              string
		memType           string
		poolName          string
		value             int64
		includeHeapPool   bool
		expectedHeap      bool
		expectedNonHeap   bool
		expectedMetaspace bool
		expectedCodeCache bool
		expectedNewGen    bool
		expectedOldGen    bool
		expectedSurvivor  bool
	}{
		{
			name:     "No memory type",
			memType:  "",
			poolName: "",
			value:    111,
		},
		{
			name:     "Other memory type",
			memType:  "other",
			poolName: "",
			value:    222,
		},
		{
			name:         "Heap memory without pool",
			memType:      "heap",
			poolName:     "",
			value:        333,
			expectedHeap: true,
		},
		{
			name:             "Heap memory with New Gen and Survivor pool",
			memType:          "heap",
			poolName:         "G1 Survivor Space",
			value:            444,
			includeHeapPool:  true,
			expectedHeap:     true,
			expectedNewGen:   true,
			expectedSurvivor: true,
		},
		{
			name:            "Heap memory with Old Gen pool",
			memType:         "heap",
			poolName:        "PS Old Gen",
			value:           555,
			includeHeapPool: true,
			expectedHeap:    true,
			expectedOldGen:  true,
		},
		{
			name:            "Heap memory with Other pool",
			memType:         "heap",
			poolName:        "other",
			value:           666,
			includeHeapPool: true,
			expectedHeap:    true,
		},
		{
			name:              "Non-heap Metaspace",
			memType:           "non_heap",
			poolName:          "Metaspace",
			value:             777,
			includeHeapPool:   true,
			expectedNonHeap:   true,
			expectedMetaspace: true,
		},
		{
			name:              "Non-heap CodeCache",
			memType:           "non_heap",
			poolName:          "Code Cache",
			value:             888,
			includeHeapPool:   true,
			expectedNonHeap:   true,
			expectedCodeCache: true,
		},
		{
			name:            "Non-heap memory without pool",
			memType:         "non_heap",
			poolName:        "",
			value:           999,
			expectedNonHeap: true,
		},
		{
			name:            "Non-heap memory with Other pool",
			memType:         "non_heap",
			poolName:        "other",
			value:           1024,
			expectedNonHeap: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mappings := newMemoryMappings(true, true, true)

			attrs := pcommon.NewMap()
			if tt.memType != "" {
				attrs.UpsertString(jvmMemoryType, tt.memType)
			}
			if tt.poolName != "" {
				attrs.UpsertString(jvmMemoryPoolName, tt.poolName)
			}

			now := pcommon.NewTimestampFromTime(time.Now())

			processMemoryByType(attrs, now, tt.value, mappings, tt.includeHeapPool)

			if tt.expectedHeap {
				assert.Equal(t, tt.value, mappings.heap[now])
			} else {
				assert.Len(t, mappings.heap, 0)
			}

			if tt.expectedNonHeap {
				assert.Equal(t, tt.value, mappings.nonHeap[now])
			} else {
				assert.Len(t, mappings.nonHeap, 0)
			}

			if tt.expectedMetaspace {
				assert.Equal(t, tt.value, mappings.metaspace[now])
			} else {
				assert.Len(t, mappings.metaspace, 0)
			}

			if tt.expectedCodeCache {
				assert.Equal(t, tt.value, mappings.codeCache[now])
			} else {
				assert.Len(t, mappings.codeCache, 0)
			}

			if tt.expectedNewGen {
				assert.Equal(t, tt.value, mappings.newgen[now])
			} else {
				assert.Len(t, mappings.newgen, 0)
			}

			if tt.expectedOldGen {
				assert.Equal(t, tt.value, mappings.oldgen[now])
			} else {
				assert.Len(t, mappings.oldgen, 0)
			}

			if tt.expectedSurvivor {
				assert.Equal(t, tt.value, mappings.survivor[now])
			} else {
				assert.Len(t, mappings.survivor, 0)
			}
		})
	}
}

func TestProcessThreadByState(t *testing.T) {
	tests := []struct {
		name       string
		state      string
		value      int64
		checkField string
	}{
		{
			name:       "Runnable state",
			state:      "runnable",
			value:      4,
			checkField: "runnable",
		},
		{
			name:       "Blocked state",
			state:      "blocked",
			value:      2,
			checkField: "blocked",
		},
		{
			name:       "Waiting state",
			state:      "waiting",
			value:      3,
			checkField: "waiting",
		},
		{
			name:       "Timed waiting state",
			state:      "timed_waiting",
			value:      1,
			checkField: "timedWaiting",
		},
		{
			name:       "Other state",
			state:      "",
			value:      5,
			checkField: "Other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mappings := newThreadMappings()

			attrs := pcommon.NewMap()
			if tt.state != "" {
				attrs.UpsertString(jvmThreadState, tt.state)
			}

			now := pcommon.NewTimestampFromTime(time.Now())

			processThreadByState(attrs, now, tt.value, mappings)

			switch tt.checkField {
			case "runnable":
				assert.Equal(t, tt.value, mappings.runnable[now])
			case "blocked":
				assert.Equal(t, tt.value, mappings.blocked[now])
			case "waiting":
				assert.Equal(t, tt.value, mappings.waiting[now])
			case "timedWaiting":
				assert.Equal(t, tt.value, mappings.timedWaiting[now])
			default:
				assert.Len(t, mappings.runnable, 0)
				assert.Len(t, mappings.blocked, 0)
				assert.Len(t, mappings.waiting, 0)
				assert.Len(t, mappings.timedWaiting, 0)
			}
		})
	}
}

func TestProcessDaemonThread(t *testing.T) {
	now := pcommon.NewTimestampFromTime(time.Now())

	t.Run("Daemon thread", func(t *testing.T) {
		mappings := newThreadMappings()
		attrs := pcommon.NewMap()
		attrs.UpsertBool(jvmThreadDaemon, true)

		processDaemonThread(attrs, now, 2, mappings)
		assert.Equal(t, int64(2), mappings.daemon[now])
	})

	t.Run("Non-daemon thread", func(t *testing.T) {
		mappings := newThreadMappings()
		attrs := pcommon.NewMap()
		attrs.UpsertBool(jvmThreadDaemon, false)

		processDaemonThread(attrs, now, 1, mappings)
		assert.Len(t, mappings.daemon, 0)
	})

	t.Run("Missing daemon attribute", func(t *testing.T) {
		mappings := newThreadMappings()
		attrs := pcommon.NewMap()

		processDaemonThread(attrs, now, 3, mappings)
		assert.Len(t, mappings.daemon, 0)
	})
}
