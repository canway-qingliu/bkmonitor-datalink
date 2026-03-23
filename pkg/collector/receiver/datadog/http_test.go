// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package datadog

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/pipeline"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/receiver"
)

const localReplayURL = "http://127.0.0.1:4318/api/v2/replay"

var registerDatadogRoutesOnce sync.Once

func registerDatadogRoutes() {
	registerDatadogRoutesOnce.Do(Ready)
}

func TestReplayRoute(t *testing.T) {
	registerDatadogRoutes()

	req := httptest.NewRequest(
		http.MethodPost,
		localReplayURL+"?dd-request-id=replay-request-id",
		bytes.NewBufferString("[]"),
	)
	rw := httptest.NewRecorder()

	receiver.RecvHttpRouter().ServeHTTP(rw, req)

	assert.Equal(t, http.StatusOK, rw.Code)
	assert.JSONEq(t, `{"request_id":"replay-request-id"}`, rw.Body.String())
}

func TestConvertDataToDatadogEventV2SupportsTimestamp(t *testing.T) {
	event, err := convertDataToDatadogEventV2(map[string]interface{}{
		"type":      "view",
		"event_type": "page_view",
		"timestamp": float64(1234567890000),
	})

	assert.NoError(t, err)
	assert.Equal(t, int64(1234567890000), event.Date)
}

func TestPublishConvertedRecordsKeepsLogs(t *testing.T) {
	converter := NewConverter()
	result := converter.ToOTEL(&RUMEventV2{
		Type:      "performance",
		EventType: "resource",
		Date:      1700000000000,
		Data: map[string]interface{}{
			"message": "resource finished",
			"resource": map[string]interface{}{
				"duration": float64(123),
				"size":     float64(456),
			},
		},
	})

	service := HttpService{
		Validator: pipeline.Validator{
			Func: func(r *define.Record) (define.StatusCode, string, error) {
				return define.StatusCodeOK, "", nil
			},
		},
	}

	assert.Equal(t, 1, result.Logs.LogRecordCount())

	service.publishConvertedRecords(result, "127.0.0.1", "token", 128, time.Now())

	assert.Equal(t, 1, result.Logs.LogRecordCount())
	assert.NotEqual(t, plog.NewLogs().LogRecordCount(), result.Logs.LogRecordCount())
}
