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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
)

// runTraceProfileHandler 直接调用 typed Trace 处理入口，返回响应与交给 span 的错误。
func runTraceProfileHandler(t *testing.T, body string) (*httptest.ResponseRecorder, error) {
	t.Helper()
	metadata.InitMetadata()
	ctx := metadata.InitHashID(context.Background())
	metadata.SetUser(ctx, &metadata.User{TenantID: "system", SpaceUID: "bkcc__42"})
	ctx, span := trace.NewSpan(ctx, "trace-profile-test")
	defer span.End(new(error))
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/query/ts/raw", bytes.NewBufferString(body)).WithContext(ctx)
	ginContext.Set(verifiedJWTAppCodeContextKey, "kingeye-apm")
	return recorder, handleTraceProfileQuery(ginContext, ctx, span)
}

func traceProfileRejectionCount(t *testing.T, code, status string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "unify_query_trace_profile_rejections_total" {
			continue
		}
		for _, sample := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range sample.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["code"] == code && labels["status"] == status {
				return sample.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// F7：后端失败返回 5xx 并交给 span 记为失败；驱动错误原文（含后端地址）不回给调用方。
func TestTraceBackendFailureIs5xxWithoutDriverText(t *testing.T) {
	withTraceProfileRuntime(t, traceProfileRuntimeConfig())
	withTraceProfileAuth(t, true, "kingeye-apm")
	before := traceProfileRejectionCount(t, traceprofile.CodePartialFailure, "5xx")
	recorder, spanErr := runTraceProfileHandler(t, typedTraceBody("trace_span", "system_kapm"))
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("backend failure returned HTTP %d: %s", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "127.0.0.1") {
		t.Fatalf("driver error text reached the caller: %s", recorder.Body.String())
	}
	if spanErr == nil || strings.Contains(spanErr.Error(), "127.0.0.1") {
		t.Fatalf("span error missing or carries driver text: %v", spanErr)
	}
	if after := traceProfileRejectionCount(t, traceprofile.CodePartialFailure, "5xx"); after != before+1 {
		t.Fatalf("backend failure was not counted by code: %v -> %v", before, after)
	}
}

// F7：请求被拒是 4xx，按错误码计数，不把 span 记为失败。
func TestTraceRejectionIsCountedByCodeAndNotASpanError(t *testing.T) {
	withTraceProfileRuntime(t, traceProfileRuntimeConfig())
	withTraceProfileAuth(t, true, "kingeye-apm")
	before := traceProfileRejectionCount(t, traceprofile.CodeUnsupportedSchemaCapability, "4xx")
	body := strings.Replace(typedTraceBody("trace_span", ""), `"trace_request":{`, `"trace_request":{"observation_domain_id":"other_kapm",`, 1)
	recorder, spanErr := runTraceProfileHandler(t, body)
	if recorder.Code != http.StatusBadRequest || spanErr != nil {
		t.Fatalf("rejection returned HTTP %d span error %v", recorder.Code, spanErr)
	}
	if after := traceProfileRejectionCount(t, traceprofile.CodeUnsupportedSchemaCapability, "4xx"); after != before+1 {
		t.Fatalf("rejection was not counted by code: %v -> %v", before, after)
	}
}

// F7：发布源读取失败且没有快照时返回 503，与"未发布/未配置"的 400 区分。
func TestTraceReadTargetsUnavailableIs503(t *testing.T) {
	publication := withConsulTracePublication(t, map[string]string{"system": tracePublicationFor("system")})
	publication.fail(context.DeadlineExceeded)
	if err := reloadTraceProfilePublishedRuntime(); err == nil {
		t.Fatal("failed read was reported as success")
	}
	recorder, spanErr := runTraceProfileHandler(t, typedTraceBody("trace_span", "system_kapm"))
	if recorder.Code != http.StatusServiceUnavailable || spanErr == nil {
		t.Fatalf("unavailable read targets returned HTTP %d span error %v", recorder.Code, spanErr)
	}
	publication.fail(nil)
	publication.set("system", "")
	_ = reloadTraceProfilePublishedRuntime()
	recorder, spanErr = runTraceProfileHandler(t, typedTraceBody("trace_span", "system_kapm"))
	if recorder.Code != http.StatusBadRequest || spanErr != nil {
		t.Fatalf("unpublished read targets returned HTTP %d span error %v", recorder.Code, spanErr)
	}
}

// L2：响应文案只来自封闭的拒绝文案。
func TestUnclassifiedErrorDoesNotLeakItsText(t *testing.T) {
	recorder, _ := runTraceProfileWriteError(t, errors.New("dial tcp 10.0.0.1:9030: refused"))
	if strings.Contains(recorder.Body.String(), "10.0.0.1") || recorder.Code != http.StatusBadGateway {
		t.Fatalf("an unclassified error leaked its text: %d %s", recorder.Code, recorder.Body.String())
	}
}

// runTraceProfileWriteError 直接写出一个错误，返回响应与交给 span 的错误。
func runTraceProfileWriteError(t *testing.T, err error) (*httptest.ResponseRecorder, error) {
	t.Helper()
	metadata.InitMetadata()
	ctx := metadata.InitHashID(context.Background())
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/query/ts/raw", nil).WithContext(ctx)
	return recorder, writeTraceProfileError(ctx, ginContext, nil, err)
}
