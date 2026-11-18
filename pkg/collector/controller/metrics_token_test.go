// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controller

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/tokentest"
)

// The pipeline's skipped and handled records name an application by its
// token's fingerprint.
func TestPipelineMetricsNameATokenByItsFingerprint(t *testing.T) {
	const token = "pipeline-metrics-token-test"
	DefaultMetricMonitor.IncSkippedCounter("traces_pipeline", define.RecordTraces, 1001, "sampler", token)
	DefaultMetricMonitor.IncHandledCounter("traces_pipeline", define.RecordTraces, 1001, token)
	tokentest.CheckFamilies(t, token, "bk_collector_pipeline_skipped_total", "bk_collector_pipeline_handled_total")
}
