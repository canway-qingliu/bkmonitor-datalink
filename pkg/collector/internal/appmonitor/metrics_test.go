// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package appmonitor

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
)

func TestIdentityUsesTrustedApplicationAndMasksToken(t *testing.T) {
	token := define.Token{Original: "secret-token-value", TenantID: "tenant-a", BizId: 2, AppID: 17, AppName: "orders"}
	identity, ok := IdentityFromToken(token)
	if !ok {
		t.Fatal("trusted application identity should be available")
	}
	if identity.TenantID != "tenant-a" || identity.BizID != "2" || identity.AppID != "17" {
		t.Fatalf("unexpected application identity: %+v", identity)
	}
	if len(identity.TokenFingerprint) != 16 || strings.Contains(identity.TokenFingerprint, token.Original) {
		t.Fatalf("token fingerprint must be short and must not contain the credential: %+v", identity)
	}
	if again, ok := IdentityFromToken(token); !ok || again != identity {
		t.Fatal("the same token must retain its identity across Collector instances")
	}
	if _, ok := IdentityFromToken(define.Token{Original: "unknown", BizId: 2}); ok {
		t.Fatal("an unbound token must not produce application-labelled metrics")
	}
}

func TestIdentityDoesNotRequireApplicationName(t *testing.T) {
	token := define.Token{Original: "application-name-can-change", TenantID: "tenant-a", BizId: 2, AppID: 19}
	if _, ok := IdentityFromToken(token); !ok {
		t.Fatal("stable application ID should be enough for monitoring identity")
	}
}

func TestFlowCountersKeepTraceAndMetricUnitsSeparate(t *testing.T) {
	token := define.Token{Original: "test-flow-token", TenantID: "tenant-a", BizId: 2, AppID: 18, AppName: "checkout"}
	id, ok := IdentityFromToken(token)
	if !ok {
		t.Fatal("missing identity")
	}
	ObserveReceiver(token, define.RecordTraces, 3)
	ObserveReceiver(token, define.RecordMetrics, 5)
	ObservePipeline(token, define.RecordTraces, "handled")
	ObservePipeline(token, define.RecordMetrics, "handled")
	ObserveExporter(token, define.RecordTraces, "enqueued", 3)
	ObserveExporter(token, define.RecordMetrics, "enqueued", 5)

	base := []string{id.TenantID, id.BizID, id.AppID, id.TokenFingerprint}
	if got := counterValue(t, receiverRecords.WithLabelValues(append(base, "traces")...)); got != 1 {
		t.Fatalf("trace requests: got %v, want 1", got)
	}
	if got := counterValue(t, receiverItems.WithLabelValues(append(base, "traces")...)); got != 3 {
		t.Fatalf("trace spans: got %v, want 3", got)
	}
	if got := counterValue(t, receiverItems.WithLabelValues(append(base, "metrics")...)); got != 5 {
		t.Fatalf("metric data points: got %v, want 5", got)
	}
	if got := counterValue(t, pipelineRecords.WithLabelValues(append(base, "traces", "handled")...)); got != 1 {
		t.Fatalf("handled trace records: got %v, want 1", got)
	}
	if got := counterValue(t, exporterEvents.WithLabelValues(append(base, "metrics", "enqueued")...)); got != 5 {
		t.Fatalf("enqueued metric events: got %v, want 5", got)
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "bk_collector_app_receiver_records_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "token" || label.GetName() == "app_name" || label.GetValue() == token.Original {
					t.Fatal("application self-monitoring must not expose a raw token")
				}
			}
		}
	}
}

func TestNonAPMRecordsDoNotCreateApplicationSeries(t *testing.T) {
	token := define.Token{Original: "non-apm-monitor-token", TenantID: "tenant-a", BizId: 2, AppID: 98901, AppName: "other"}
	ObservePipeline(token, define.RecordLogs, "handled")
	ObservePipeline(token, define.RecordMetricsDerived, "handled")
	ObserveExporter(token, define.RecordTars, "enqueued", 3)
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "bk_collector_app_pipeline_records_total" && family.GetName() != "bk_collector_app_exporter_events_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "app_id" && label.GetValue() == "98901" {
					t.Fatalf("non-APM signal must not create an application series: %s", family.GetName())
				}
			}
		}
	}
}

func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := counter.Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}
