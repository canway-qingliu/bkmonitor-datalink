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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/tokentest"
)

// Every receiver series that names an application names it by its token's
// fingerprint: handled, skipped, prechecks failed, bytes received, their
// sizes and the handling time.
func TestReceiverMetricsNameATokenByItsFingerprint(t *testing.T) {
	const token = "receiver-metrics-token-test"
	monitor := DefaultMetricMonitor.Source("token-test")
	monitor.IncHandledCounter(define.RequestHttp, define.RecordTraces, token)
	monitor.IncSkippedCounter(define.RequestHttp, define.RecordTraces, token)
	monitor.IncPreCheckFailedCounter(define.RequestHttp, define.RecordTraces, "tokenchecker", token, define.StatusCodeUnauthorized)
	monitor.AddReceivedBytesCounter(128, define.RequestHttp, define.RecordTraces, token)
	monitor.ObserveBytesDistribution(128, define.RequestHttp, define.RecordTraces, token)
	monitor.ObserveHandledDuration(time.Now(), define.RequestHttp, define.RecordTraces, token)
	tokentest.CheckFamilies(t, token,
		"bk_collector_receiver_handled_total",
		"bk_collector_receiver_skipped_total",
		"bk_collector_receiver_precheck_failed_total",
		"bk_collector_receiver_received_bytes_total",
		"bk_collector_receiver_received_bytes_size",
		"bk_collector_receiver_handled_duration_seconds",
	)
}
