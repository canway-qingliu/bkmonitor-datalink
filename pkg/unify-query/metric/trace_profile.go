// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// traceProfileSnapshotConfirmedAt 是当前读目标快照最近一次被成功读取确认的时间（UnixNano），
// 0 表示没有已安装的快照。
var traceProfileSnapshotConfirmedAt atomic.Int64

var (
	traceProfileReloadTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "unify_query", Name: "trace_profile_snapshot_reload_total",
		Help: "Trace read target snapshot reloads by source and fixed result.",
	}, []string{"source", "result"})
	traceProfileReloadFailures = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "unify_query", Name: "trace_profile_snapshot_reload_consecutive_failures",
		Help: "Trace read target reloads or watches that failed in a row since the last successful read.",
	})
	_ = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "unify_query", Name: "trace_profile_snapshot_age_seconds",
		Help: "Seconds since the installed Trace read target snapshot was last confirmed by a successful read; -1 without a snapshot.",
	}, func() float64 {
		confirmed := traceProfileSnapshotConfirmedAt.Load()
		if confirmed == 0 {
			return -1
		}
		return time.Since(time.Unix(0, confirmed)).Seconds()
	})
	traceProfileInvalidDocumentsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "unify_query", Name: "trace_profile_invalid_documents_total",
		Help: "Tenant read target documents rejected at load (the tenant is revoked), by fixed reason.",
	}, []string{"reason"})
	traceProfileRejectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "unify_query", Name: "trace_profile_rejections_total",
		Help: "Typed Trace requests rejected, by closed error code and HTTP status.",
	}, []string{"code", "status"})
)

// TraceProfileSnapshotReload 记录一次读目标重载或监听结果。source 与 result 只取固定枚举，
// 不带租户、路径或错误原文。
func TraceProfileSnapshotReload(source, result string, consecutiveFailures int) {
	traceProfileReloadTotal.WithLabelValues(source, result).Inc()
	traceProfileReloadFailures.Set(float64(consecutiveFailures))
}

// TraceProfileInvalidDocument 记录一份在加载时被拒的租户读目标文档，reason 只取固定枚举。
func TraceProfileInvalidDocument(reason string) {
	traceProfileInvalidDocumentsTotal.WithLabelValues(reason).Inc()
}

// TraceProfileSnapshotConfirmed 记录当前快照被成功读取确认的时间；零值表示快照已撤销。
func TraceProfileSnapshotConfirmed(at time.Time) {
	if at.IsZero() {
		traceProfileSnapshotConfirmedAt.Store(0)
		return
	}
	traceProfileSnapshotConfirmedAt.Store(at.UnixNano())
}

// TraceProfileRejection 按封闭错误码与 HTTP 状态计数被拒的 typed Trace 请求。
func TraceProfileRejection(code string, status int) {
	traceProfileRejectionsTotal.WithLabelValues(code, httpStatusLabel(status)).Inc()
}

func httpStatusLabel(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	default:
		return "other"
	}
}
