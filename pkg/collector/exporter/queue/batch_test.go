// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package queue

import (
	"sync"
	"testing"
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/appmonitor"
)

type testMetricsEvent struct {
	define.CommonEvent
}

func (t testMetricsEvent) RecordType() define.RecordType {
	return define.RecordPushGateway
}

type testTracesEvent struct {
	define.CommonEvent
}

func (t testTracesEvent) RecordType() define.RecordType {
	return define.RecordTraces
}

type testLogsEvent struct {
	define.CommonEvent
}

func TestQueueFlushPreservesApplicationCountsAcrossSharedDataID(t *testing.T) {
	q := NewBatchQueue(Config{TracesBatchSize: 2, FlushInterval: time.Minute}, func(string) Config { return Config{} })
	defer q.Close()
	a := define.Token{Original: "queue-monitor-a", TenantID: "tenant-a", BizId: 2, AppID: 74001, AppName: "a"}
	b := define.Token{Original: "queue-monitor-b", TenantID: "tenant-a", BizId: 2, AppID: 74002, AppName: "b"}
	q.Put(
		&testTracesEvent{CommonEvent: define.NewCommonEvent(a, 9001, common.MapStr{"item": "a"})},
		&testTracesEvent{CommonEvent: define.NewCommonEvent(b, 9001, common.MapStr{"item": "b"})},
	)
	select {
	case <-q.Pop():
	case <-time.After(3 * time.Second):
		t.Fatal("the full batch did not flush")
	}
	for _, token := range []define.Token{a, b} {
		id, ok := appmonitor.IdentityFromToken(token)
		assert.True(t, ok)
		assert.Equal(t, float64(1), queueAppCounter(t, id.AppID, id.TokenFingerprint))
	}
}

func TestQueueFlushSkipsApplicationMetricForNonAPMRecord(t *testing.T) {
	q := NewBatchQueue(Config{LogsBatchSize: 1, FlushInterval: time.Minute}, func(string) Config { return Config{} })
	defer q.Close()
	token := define.Token{Original: "queue-monitor-log", TenantID: "tenant-a", BizId: 2, AppID: 98902, AppName: "other"}
	q.Put(&testLogsEvent{CommonEvent: define.NewCommonEvent(token, 9002, common.MapStr{"item": "log"})})
	select {
	case <-q.Pop():
	case <-time.After(3 * time.Second):
		t.Fatal("the full batch did not flush")
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "bk_collector_app_exporter_events_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "app_id" && label.GetValue() == "98902" {
					t.Fatal("log flush must not create an application-level metric series")
				}
			}
		}
	}
}

func queueAppCounter(t *testing.T, appID, fingerprint string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "bk_collector_app_exporter_events_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["app_id"] == appID && labels["token_fingerprint"] == fingerprint && labels["stage"] == "flushed" && labels["record_type"] == "traces" {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func (t testLogsEvent) RecordType() define.RecordType {
	return define.RecordLogs
}

func TestQueueOut(t *testing.T) {
	conf := Config{
		MetricsBatchSize: 100,
		LogsBatchSize:    2000,
		TracesBatchSize:  100,
		FlushInterval:    time.Second,
	}
	queue := NewBatchQueue(conf, func(s string) Config {
		return Config{}
	})
	defer queue.Close()

	dataIDs := []int32{1001, 1002}
	wg := sync.WaitGroup{}
	for _, id := range dataIDs {
		cloned := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				queue.Put(&testMetricsEvent{
					CommonEvent: define.NewCommonEvent(define.Token{}, cloned, common.MapStr{"count": i}),
				})
			}
		}()
	}

	var total int
	for {
		ms := <-queue.Pop()
		v, _ := ms.GetValue("data")
		data := v.([]common.MapStr)
		total += len(data)
		if total == 20000 {
			break
		}
	}
	wg.Wait()
}

func TestQueueOutWithDelta(t *testing.T) {
	conf := Config{
		MetricsBatchSize: 100,
		LogsBatchSize:    100,
		TracesBatchSize:  100,
		FlushInterval:    time.Second,
	}
	queue := NewBatchQueue(conf, func(s string) Config {
		return Config{}
	})
	defer queue.Close()

	dataIDs := []int32{1001, 1002}
	wg := sync.WaitGroup{}
	for _, id := range dataIDs {
		cloned := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				time.Sleep(time.Millisecond)
				queue.Put(&testTracesEvent{
					CommonEvent: define.NewCommonEvent(define.Token{}, cloned, common.MapStr{"count": i}),
				})
			}
		}()
	}

	var total int
	for {
		ms := <-queue.Pop()
		v, _ := ms.GetValue("items")
		data := v.([]common.MapStr)
		total += len(data)
		if total == 2000 {
			break
		}
	}
	wg.Wait()
}

func TestQueueFull(t *testing.T) {
	conf := Config{
		MetricsBatchSize: 100,
		LogsBatchSize:    1,
		TracesBatchSize:  100,
		FlushInterval:    2 * time.Second,
	}
	queue := NewBatchQueue(conf, func(s string) Config {
		return Config{}
	})
	defer queue.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)

	cases := map[int32]int32{
		1001: 1,
		1002: 2,
		1003: 3,
		1004: 4,
	}
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			for k, v := range cases {
				queue.Put(&testMetricsEvent{
					CommonEvent: define.NewCommonEvent(define.Token{}, k, common.MapStr{"count": v}),
				})
			}
		}
		done <- struct{}{}
	}()

	for {
		select {
		case e := <-queue.Pop():
			data, err := e.GetValue("data")
			assert.NoError(t, err)

			dataID, err := e.GetValue("dataid")
			assert.NoError(t, err)

			actual := data.([]common.MapStr)[0]["count"]
			expected := cases[dataID.(int32)]
			assert.Equal(t, expected, actual)

		case <-done:
			return
		}
	}
}

func TestQueueFullBatch(t *testing.T) {
	conf := Config{
		MetricsBatchSize: 100,
		LogsBatchSize:    1,
		TracesBatchSize:  100,
		FlushInterval:    2 * time.Second,
	}
	queue := NewBatchQueue(conf, func(s string) Config {
		return Config{}
	})
	defer queue.Close()

	cases := map[int32]int32{
		1001: 1,
		1002: 2,
		1003: 3,
		1004: 4,
	}
	done := make(chan struct{})
	go func() {
		for k, v := range cases {
			events := make([]define.Event, 0)
			for i := 0; i < 100; i++ {
				evt := &testMetricsEvent{CommonEvent: define.NewCommonEvent(define.Token{}, k, common.MapStr{"count": v})}
				events = append(events, evt)
			}
			queue.Put(events...)
		}
		done <- struct{}{}
	}()

	for {
		select {
		case e := <-queue.Pop():
			data, err := e.GetValue("data")
			assert.NoError(t, err)

			dataID, err := e.GetValue("dataid")
			assert.NoError(t, err)

			actual := data.([]common.MapStr)[0]["count"]
			expected := cases[dataID.(int32)]
			assert.Equal(t, expected, actual)

		case <-done:
			time.Sleep(time.Second)
			return
		}
	}
}

func TestQueueTick(t *testing.T) {
	conf := Config{
		MetricsBatchSize: 100,
		LogsBatchSize:    101,
		TracesBatchSize:  100,
		FlushInterval:    2 * time.Second,
	}
	queue := NewBatchQueue(conf, func(s string) Config {
		return Config{}
	})
	defer queue.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)

	cases := map[int32]int32{
		1001: 1,
		1002: 2,
		1003: 3,
		1004: 4,
	}

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			for k, v := range cases {
				queue.Put(&testMetricsEvent{
					CommonEvent: define.NewCommonEvent(define.Token{}, k, common.MapStr{"count": v}),
				})
			}
		}
	}()

	n := 0
	for {
		select {
		case e := <-queue.Pop():
			data, err := e.GetValue("data")
			assert.NoError(t, err)

			dataID, err := e.GetValue("dataid")
			assert.NoError(t, err)

			actual := data.([]common.MapStr)[0]["count"]
			expected := cases[dataID.(int32)]
			assert.Equal(t, expected, actual)

			n++
			if n == 4 {
				return
			}
		}
	}
}

func TestQueueResize(t *testing.T) {
	conf := Config{
		MetricsBatchSize: 100,
		LogsBatchSize:    100,
		TracesBatchSize:  100,
		FlushInterval:    time.Minute, // 保证测试运行期间不会触发
	}

	assertResize := func(event define.Event, key string) {
		queue := NewBatchQueue(conf, func(s string) Config {
			return Config{
				MetricsBatchSize: 10,
				LogsBatchSize:    10,
				TracesBatchSize:  10,
			}
		})
		defer queue.Close()

		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			defer wg.Done()
			for i := 0; i < 120; i++ {
				queue.Put(event)
			}
		}()

		n := 0
		for {
			select {
			case e := <-queue.Pop():
				_, err := e.GetValue(key)
				assert.NoError(t, err)

				dataID, err := e.GetValue("dataid")
				assert.NoError(t, err)
				assert.Equal(t, int32(1001), dataID.(int32))
				n++
				if n == 3 {
					return
				}
			}
		}
	}

	t.Run("Metrics", func(t *testing.T) {
		assertResize(&testMetricsEvent{
			CommonEvent: define.NewCommonEvent(define.Token{}, 1001, common.MapStr{"count": 1}),
		}, "data")
	})

	t.Run("Traces", func(t *testing.T) {
		assertResize(&testTracesEvent{
			CommonEvent: define.NewCommonEvent(define.Token{}, 1001, common.MapStr{"count": 1}),
		}, "items")
	})

	t.Run("Logs", func(t *testing.T) {
		assertResize(&testLogsEvent{
			CommonEvent: define.NewCommonEvent(define.Token{}, 1001, common.MapStr{"count": 1}),
		}, "items")
	})
}

func TestQueueUniqueKey(t *testing.T) {
	conf := Config{
		FlushInterval: 2 * time.Second,
	}
	queue := NewBatchQueue(conf, func(s string) Config {
		return Config{}
	})
	defer queue.Close()

	queue.Put(testMetricsEvent{
		CommonEvent: define.NewCommonEvent(define.Token{}, 1001, common.MapStr{"count": 10}),
	})
	queue.Put(testTracesEvent{
		CommonEvent: define.NewCommonEvent(define.Token{}, 1001, common.MapStr{"count": 10}),
	})

	n := 0
	for i := 0; i < 2; i++ {
		item := <-queue.Pop()
		t.Logf("pop item: %+v", item)
		n += len(item)
	}
	assert.Equal(t, 8, n)
}
