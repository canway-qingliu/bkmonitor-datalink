// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

func TestRetiredBackgroundTaskSummaryEnvelopeIsRejected(t *testing.T) {
	metadata.InitMetadata()
	gin.SetMode(gin.TestMode)
	body := `{"background_task_summary":{"version":2}}`
	ctx := metadata.InitHashID(context.Background())
	metadata.SetUser(ctx, &metadata.User{TenantID: "system", SpaceUID: "bkcc__42"})
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/query/ts/raw", bytes.NewBufferString(body)).WithContext(ctx)

	HandlerQueryRaw(ginContext)

	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "background_task_summary") {
		t.Fatalf("retired summary envelope was not rejected: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestUnwiredDirectTraceEnvelopeCannotBecomeLegacyRawSuccess(t *testing.T) {
	metadata.InitMetadata()
	gin.SetMode(gin.TestMode)
	for _, body := range []string{
		`{"trace_profile":"trace_summary","trace_contract_version":"trace_query_v2","trace_request":{}}`,
		`{"trace_profile":null}`,
		`{"trace_request":null}`,
	} {
		t.Run(body, func(t *testing.T) {
			ctx := metadata.InitHashID(context.Background())
			metadata.SetUser(ctx, &metadata.User{TenantID: "system", SpaceUID: "bkcc__42"})
			recorder := httptest.NewRecorder()
			ginContext, _ := gin.CreateTestContext(recorder)
			ginContext.Request = httptest.NewRequest(http.MethodPost, "/query/ts/raw", bytes.NewBufferString(body)).WithContext(ctx)
			HandlerQueryRaw(ginContext)
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "直连存储查询入口尚未完成") {
				t.Fatalf("unwired typed Trace request fell into legacy raw success: status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}
