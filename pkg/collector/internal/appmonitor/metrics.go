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
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
)

type Identity struct {
	TenantID         string
	BizID            string
	AppID            string
	TokenFingerprint string
}

var identityLabels = []string{"bk_tenant_id", "bk_biz_id", "app_id", define.TokenFingerprintLabel}

var (
	receiverRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: define.MonitoringNamespace,
		Name:      "app_receiver_records_total",
		Help:      "Authenticated Collector input records by trusted application identity",
	}, append(append([]string{}, identityLabels...), "record_type"))
	receiverItems = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: define.MonitoringNamespace,
		Name:      "app_receiver_items_total",
		Help:      "Authenticated input spans or metric data points by trusted application identity",
	}, append(append([]string{}, identityLabels...), "record_type"))
	pipelineRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: define.MonitoringNamespace,
		Name:      "app_pipeline_records_total",
		Help:      "Collector pipeline records by trusted application identity and outcome",
	}, append(append([]string{}, identityLabels...), "record_type", "outcome"))
	exporterEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: define.MonitoringNamespace,
		Name:      "app_exporter_events_total",
		Help:      "Converted events by trusted application identity and local exporter stage; not a Kafka acknowledgement",
	}, append(append([]string{}, identityLabels...), "record_type", "stage"))
)

func IdentityFromToken(token define.Token) (Identity, bool) {
	if token.AppID <= 0 || token.BizId <= 0 || token.Original == "" {
		return Identity{}, false
	}
	return Identity{
		TenantID:         token.TenantID,
		BizID:            strconv.FormatInt(int64(token.BizId), 10),
		AppID:            strconv.FormatInt(token.AppID, 10),
		TokenFingerprint: define.TokenFingerprint(token.Original),
	}, true
}

func (id Identity) values(extra ...string) []string {
	return append([]string{id.TenantID, id.BizID, id.AppID, id.TokenFingerprint}, extra...)
}

func Tracks(rtype define.RecordType) bool {
	return rtype == define.RecordTraces || rtype == define.RecordMetrics
}

func ObserveReceiver(token define.Token, rtype define.RecordType, items int) {
	id, ok := IdentityFromToken(token)
	if !ok {
		return
	}
	receiverRecords.WithLabelValues(id.values(rtype.S())...).Inc()
	if items > 0 {
		receiverItems.WithLabelValues(id.values(rtype.S())...).Add(float64(items))
	}
}

func ObservePipeline(token define.Token, rtype define.RecordType, outcome string) {
	if !Tracks(rtype) {
		return
	}
	id, ok := IdentityFromToken(token)
	if !ok {
		return
	}
	pipelineRecords.WithLabelValues(id.values(rtype.S(), outcome)...).Inc()
}

func ObserveExporter(token define.Token, rtype define.RecordType, stage string, events int) {
	if events <= 0 || !Tracks(rtype) {
		return
	}
	id, ok := IdentityFromToken(token)
	if !ok {
		return
	}
	exporterEvents.WithLabelValues(id.values(rtype.S(), stage)...).Add(float64(events))
}
