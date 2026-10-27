// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package receiver

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/confengine"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/appmonitor"
)

func TestReceiver(t *testing.T) {
	const configContent = `
  receiver:
    disabled: false
    # Http Server Config
    http_server:
      # 是否启动 Http 服务
      # default: false
      enabled: true
      # 服务监听端点
      # default: ""
      endpoint: "localhost:0"
      # 服务中间件，目前支持：logging/cors/content_decompressor
      middlewares:
        - "logging"
        - "cors"
        - "content_decompressor"

    # Grpc Server Config
    grpc_server:
      # 是否启动 Grpc 服务
      # default: false
      enabled: true
      # 传输协议，目前支持 tcp
      # default: ""
      transport: "tcp"
      # 服务监听端点
      # default: ""
      endpoint: "localhost:0"

    # Tars Server Config
    tars_server:
      # 是否启动 Tars 服务
      # default: false
      enabled: true
      # 传输协议，目前支持 tcp
      # default: ""
      transport: "tcp"
      # 服务监听端点
      # default: ""
      endpoint: ":4319"
`

	config := confengine.MustLoadConfigContent(configContent)
	r, err := New(config)
	assert.NoError(t, err)

	componentsReady[define.SourceJaeger] = func() {}
	componentsReady[define.SourceOtlp] = func() {}
	componentsReady[define.SourcePushGateway] = func() {}
	componentsReady[define.SourceRemoteWrite] = func() {}
	componentsReady[define.SourceZipkin] = func() {}
	componentsReady[define.SourceSkywalking] = func() {}
	componentsReady[define.SourcePyroscope] = func() {}
	componentsReady[define.SourceFta] = func() {}
	componentsReady[define.SourceBeat] = func() {}
	componentsReady[define.SourceTars] = func() {}
	componentsReady[define.SourceLogPush] = func() {}

	r.ready()
	assert.NoError(t, r.Start())
	r.Reload(config)
	assert.NoError(t, r.Stop())
	RecordHandleMetrics(DefaultMetricMonitor, define.Token{}, define.RequestHttp, define.RecordMetrics, 0, time.Now())
}

func TestPublisher(t *testing.T) {
	pub := Publisher{}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		pub.Publish(&define.Record{})
	}()

	go func() {
		defer wg.Done()
		<-Records()
	}()
	wg.Wait()
}

func TestPublisherStampsTraceAcceptanceOnce(t *testing.T) {
	var published *define.Record
	pub := Publisher{Func: func(record *define.Record) { published = record }}
	record := &define.Record{RecordType: define.RecordTraces}
	before := time.Now().UnixMilli()
	pub.Publish(record)
	after := time.Now().UnixMilli()

	assert.Same(t, record, published)
	assert.GreaterOrEqual(t, record.TraceAcceptedAtMs, before)
	assert.LessOrEqual(t, record.TraceAcceptedAtMs, after)
	assert.NotEmpty(t, record.TraceRequestID)

	acceptedAt, requestID := record.TraceAcceptedAtMs, record.TraceRequestID
	pub.Publish(record)
	assert.Equal(t, acceptedAt, record.TraceAcceptedAtMs)
	assert.Equal(t, requestID, record.TraceRequestID)

	metric := &define.Record{RecordType: define.RecordMetrics}
	pub.Publish(metric)
	assert.Zero(t, metric.TraceAcceptedAtMs)
	assert.Empty(t, metric.TraceRequestID)
}

func TestPublisherCountsAuthenticatedApplicationItems(t *testing.T) {
	token := define.Token{Original: "receiver-app-monitor-test", TenantID: "tenant-a", BizId: 2, AppID: 73, AppName: "orders"}
	id, ok := appmonitor.IdentityFromToken(token)
	assert.True(t, ok)
	traces := ptrace.NewTraces()
	spans := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	spans.AppendEmpty()
	spans.AppendEmpty()
	metrics := pmetric.NewMetrics()
	metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
	pub := Publisher{Func: func(*define.Record) {}}
	pub.Publish(&define.Record{RecordType: define.RecordTraces, Token: token, Data: traces})
	pub.Publish(&define.Record{RecordType: define.RecordMetrics, Token: token, Data: metrics})

	assert.Equal(t, float64(2), gatheredCounter(t, "bk_collector_app_receiver_items_total", id.AppID, "traces"))
	assert.Equal(t, float64(1), gatheredCounter(t, "bk_collector_app_receiver_items_total", id.AppID, "metrics"))
}

func gatheredCounter(t *testing.T, name, appID, rtype string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["app_id"] == appID && labels["record_type"] == rtype {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestRegisterDuplicateRoutes(t *testing.T) {
	assert.Panics(t, func() {
		RegisterRecvHttpRoute("test", []RouteWithFunc{
			{
				Method:       http.MethodGet,
				RelativePath: "/route1",
			},
			{
				Method:       http.MethodGet,
				RelativePath: "/route1",
			},
		})
	})
}

func TestSkywalkingFetcher(t *testing.T) {
	t.Run("Define function", func(t *testing.T) {
		fetcher := SkywalkingConfigFetcher{
			Func: func(s string) SkywalkingConfig {
				return SkywalkingConfig{
					Sn: "sn1",
				}
			},
		}

		config := fetcher.Fetch("token1")
		assert.Equal(t, "sn1", config.Sn)
	})

	t.Run("Default", func(t *testing.T) {
		var fetcher SkywalkingConfigFetcher
		config := fetcher.Fetch("token1")
		assert.Equal(t, "", config.Sn)
	})
}

func TestWriteResponse(t *testing.T) {
	r := httptest.NewRecorder()
	WriteResponse(r, "application/json", 200, nil)
	assert.Equal(t, 200, r.Result().StatusCode)
}
