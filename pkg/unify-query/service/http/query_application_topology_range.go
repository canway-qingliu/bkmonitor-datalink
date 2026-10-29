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
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
)

func isApplicationTopologyRangeEnvelope(body []byte) bool {
	var probe struct {
		Version string `json:"application_topology_contract_version"`
	}
	return json.Unmarshal(body, &probe) == nil && probe.Version == "application_topology_range_v2"
}

func handleApplicationTopologyRangeQuery(c *gin.Context, ctx context.Context, span *trace.Span,
	runtime *traceProfileRuntime, verified bool, appCode string, body []byte) error {
	// 1. 使用现有成员级调用方白名单；空间/业务不决定租户级 应用拓扑 的数据范围。
	if err := authorizeTraceProfile(verified, appCode, runtime.memberLookupAllowedAppCodes); err != nil {
		return writeTraceProfileReject(ctx, c, span, traceprofile.CodeUnauthorizedMember, err.Error())
	}
	query, err := traceprofile.ParseApplicationTopologyRangeRequest(body, runtime.maxBodyBytes)
	if err != nil {
		return writeTraceProfileReject(ctx, c, span, traceprofile.CodeUnsupportedSchemaCapability, err.Error())
	}
	user := metadata.GetUser(ctx)
	anchorAllowed := false
	for _, app := range query.Request.Applications {
		if user.SpaceUID == fmt.Sprintf("bkcc__%d", app.BkBizID) {
			anchorAllowed = true
			break
		}
	}
	if !anchorAllowed {
		return writeTraceProfileReject(ctx, c, span, traceprofile.CodeUnauthorizedMember, "应用拓扑 全景路由空间不在受信应用目录")
	}
	tenantID := user.TenantID
	if query.ScopeFingerprint != traceprofile.ApplicationTopologyRangeFingerprint(tenantID, query) {
		return writeTraceProfileReject(ctx, c, span, traceprofile.CodeUnauthorizedMember, "应用拓扑 全景租户范围摘要不匹配")
	}
	query.Request.TenantID = tenantID
	// 2. 只读已发布的稳定应用边，不将空结果解释为完整零调用。
	result, err := runtime.executor.QueryApplicationTopologyRange(ctx, query.Request)
	if err != nil {
		return writeTraceProfileError(ctx, c, span, err)
	}
	if result.PublishedRouteVersion != query.ExpectedRouteVersion {
		return writeTraceProfileReject(ctx, c, span, traceprofile.CodeScopePartial, "应用拓扑 全景发布版本尚未生效")
	}
	span.Set("trace-profile", "application_topology_range")
	span.Set("trace-profile-route-version", runtime.routeVersion)
	metric.APIRequestInc(ctx, c.Request.URL.Path, metric.StatusSuccess, user.SpaceUID, user.Source)
	response := gin.H{
		"application_topology_contract_version": query.ProtocolVersion,
		"scope_fingerprint":                     query.ScopeFingerprint,
		"published_route_version":               result.PublishedRouteVersion,
		"expected_minutes":                      result.ExpectedMinutes,
		"edges":                                 result.Edges,
	}
	c.JSON(http.StatusOK, response)
	return nil
}
