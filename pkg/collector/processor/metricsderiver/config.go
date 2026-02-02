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

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/metricsbuilder"
)

const (
	resourceTelemetryDistroName = "telemetry.distro.name"

	// OT Java Agent 上报的 JVM 指标名
	jvmGcDuration      = "jvm.gc.duration"
	jvmMemoryInit      = "jvm.memory.init"
	jvmMemoryUsed      = "jvm.memory.used"
	jvmMemoryCommitted = "jvm.memory.committed"
	jvmMemoryLimit     = "jvm.memory.limit"
	jvmThreadCount     = "jvm.thread.count"

	// 原始 JVM 指标的属性名
	jvmGcName         = "jvm.gc.name"
	jvmMemoryType     = "jvm.memory.type"
	jvmMemoryPoolName = "jvm.memory.pool.name"
	jvmThreadState    = "jvm.thread.state"
	jvmThreadDaemon   = "jvm.thread.daemon"

	// 衍生出的 JVM 指标名
	jvmGcOldCount               = "jvm_gc_old_count"
	jvmGcOldTime                = "jvm_gc_old_time"
	jvmGcYoungCount             = "jvm_gc_young_count"
	jvmGcYoungTime              = "jvm_gc_young_time"
	jvmMemoryHeapMax            = "jvm_memory_heap_max"
	jvmMemoryHeapUsed           = "jvm_memory_heap_used"
	jvmMemoryHeapCommitted      = "jvm_memory_heap_committed"
	jvmMemoryNoHeapInit         = "jvm_memory_noheap_init"
	jvmMemoryNoHeapMax          = "jvm_memory_noheap_max"
	jvmMemoryNoHeapUsed         = "jvm_memory_noheap_used"
	jvmMemoryNoHeapCommitted    = "jvm_memory_noheap_committed"
	jvmMemoryCodeCacheInit      = "jvm_memory_codecache_init"
	jvmMemoryCodeCacheMax       = "jvm_memory_codecache_max"
	jvmMemoryCodeCacheUsed      = "jvm_memory_codecache_used"
	jvmMemoryCodeCacheCommitted = "jvm_memory_codecache_committed"
	jvmMemoryNewGenCommitted    = "jvm_memory_newgen_committed"
	jvmMemoryOldGenCommitted    = "jvm_memory_oldgen_committed"
	jvmMemorySurvivorCommitted  = "jvm_memory_survivor_committed"
	jvmMemoryMetaspaceInit      = "jvm_memory_metaspace_init"
	jvmMemoryMetaspaceMax       = "jvm_memory_metaspace_max"
	jvmMemoryMetaspaceUsed      = "jvm_memory_metaspace_used"
	jvmMemoryMetaspaceCommitted = "jvm_memory_metaspace_committed"
	jvmThreadLiveCount          = "jvm_thread_live_count"
	jvmThreadDaemonCount        = "jvm_thread_daemon_count"
	jvmThreadRunnableCount      = "jvm_thread_runnable_count"
	jvmThreadBlockedCount       = "jvm_thread_blocked_count"
	jvmThreadWaitingCount       = "jvm_thread_waiting_count"
	jvmThreadTimeWaitingCount   = "jvm_thread_time_waiting_count"
)

type Config struct {
	OTJavaAgent OTJavaAgent `config:"ot_java_agent" mapstructure:"ot_java_agent"`
}

type OTJavaAgent struct {
	Enabled          bool `config:"enabled" mapstructure:"enabled"`
	KeepOriginMetric bool `config:"keep_origin_metric" mapstructure:"keep_origin_metric"`
}

type MetricBuilder struct {
	metricSlice []*MetricItem
}

func NewMetricBuilder() *MetricBuilder {
	return &MetricBuilder{}
}

type MetricItem struct {
	Name string
	metricsbuilder.Metric
}

func (mb *MetricBuilder) build(scopeMetrics pmetric.ScopeMetrics) {
	for _, metricItem := range mb.metricSlice {
		metrics := scopeMetrics.Metrics().AppendEmpty()
		metrics.SetDataType(pmetric.MetricDataTypeGauge)
		metrics.SetName(metricItem.Name)

		metric := metrics.Gauge().DataPoints().AppendEmpty()
		metric.SetDoubleVal(metricItem.Val)
		metric.SetTimestamp(metricItem.Ts)
		for k, v := range metricItem.Dimensions {
			metric.Attributes().UpsertString(k, v)
		}
	}
}

type JvmGcDataPoint struct {
	countVal uint64
	timeVal  float64
}

var (
	survivorPattern = regexp.MustCompile(`(?i).*Survivor.*`)
	newGenPools     = map[string]bool{
		"PS Eden Space": true, "PS Survivor Space": true,
		"Par Eden Space": true, "Par Survivor Space": true,
		"Eden Space": true, "Survivor Space": true,
		"G1 Eden Space": true, "G1 Survivor Space": true,
	}
	oldGenPools = map[string]bool{
		"PS Old Gen": true, "CMS Old Gen": true,
		"Tenured Gen": true, "G1 Old Gen": true,
	}
)

type memoryMappings struct {
	heap      map[pcommon.Timestamp]int64
	nonHeap   map[pcommon.Timestamp]int64
	metaspace map[pcommon.Timestamp]int64
	codeCache map[pcommon.Timestamp]int64
	newgen    map[pcommon.Timestamp]int64
	oldgen    map[pcommon.Timestamp]int64
	survivor  map[pcommon.Timestamp]int64
}

func newMemoryMappings(includeHeap, includeGen, includeSurvivor bool) *memoryMappings {
	m := &memoryMappings{
		nonHeap:   make(map[pcommon.Timestamp]int64),
		metaspace: make(map[pcommon.Timestamp]int64),
		codeCache: make(map[pcommon.Timestamp]int64),
	}

	if includeHeap {
		m.heap = make(map[pcommon.Timestamp]int64)
	}
	if includeGen {
		m.newgen = make(map[pcommon.Timestamp]int64)
		m.oldgen = make(map[pcommon.Timestamp]int64)
	}
	if includeSurvivor {
		m.survivor = make(map[pcommon.Timestamp]int64)
	}

	return m
}

type threadMappings struct {
	runnable     map[pcommon.Timestamp]int64
	blocked      map[pcommon.Timestamp]int64
	waiting      map[pcommon.Timestamp]int64
	timedWaiting map[pcommon.Timestamp]int64
	daemon       map[pcommon.Timestamp]int64
	live         map[pcommon.Timestamp]int64
}

func newThreadMappings() *threadMappings {
	return &threadMappings{
		runnable:     make(map[pcommon.Timestamp]int64),
		blocked:      make(map[pcommon.Timestamp]int64),
		waiting:      make(map[pcommon.Timestamp]int64),
		timedWaiting: make(map[pcommon.Timestamp]int64),
		daemon:       make(map[pcommon.Timestamp]int64),
		live:         make(map[pcommon.Timestamp]int64),
	}
}
