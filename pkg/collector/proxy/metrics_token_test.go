// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package proxy

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/tokentest"
)

// A proxy precheck that failed names the application by its token's
// fingerprint.
func TestProxyMetricsNameATokenByItsFingerprint(t *testing.T) {
	const token = "proxy-metrics-token-test"
	DefaultMetricMonitor.IncPreCheckFailedCounter("tokenchecker", token, 1001, define.StatusCodeUnauthorized)
	tokentest.CheckFamilies(t, token, "bk_collector_proxy_precheck_failed_total")
}
