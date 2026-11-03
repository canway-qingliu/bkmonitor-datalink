// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package traceprofile

import (
	"crypto/sha256"
	"fmt"
	"regexp"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

// DorisTarget 是 Metadata 在 Binding 物理 db/table 与集群读回后发布的可信只读目标。
// 连接凭据不在这里，只存在于 UQ 侧只读连接配置（读写契约 §5.2.1）。
type DorisTarget struct {
	ClusterID     string
	Database      string
	Table         string
	SchemaVersion string
}

// ReadRoute 按 (租户, 业务, 应用, profile) 定位一个受信物理读目标。
type ReadRoute struct {
	BkTenantID string
	BkBizID    int64
	AppID      int64 // 必须为正整数；业务身份仅用于授权与行过滤。
	Profile    metadata.TraceProfile
	// ResultTableID 与 DataID 是可选成对发布的来源身份，不用于派生物理目标。
	ResultTableID string
	DataID        int64
	// PublishedVersion 是 Kingeye 单租户文档版本；快照 Version 是所有租户的合成版本。
	PublishedVersion int64
	Doris            *DorisTarget
}

// RouteSnapshot 是一次发布版本的只读快照。版本不匹配、路由缺失或物理坐标
// 与冻结命名漂移时一律 fail closed，不猜、不回退。
type RouteSnapshot struct {
	version int64
	routes  map[string]ReadRoute
}

var physicalIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// typed Trace 只支持 Span；物理表必须由受信配置明确登记，不按业务派生。
var profilePhysicalTable = map[metadata.TraceProfile]string{
	metadata.TraceProfileSpan: "cw_trace_span",
}

func appRouteKey(tenantID string, bizID, appID int64, profile metadata.TraceProfile) string {
	return fmt.Sprintf("%s\x00%d\x00%d\x00%s", tenantID, bizID, appID, profile)
}

func tenantSpanDatabase(tenantID string) string {
	if tenantID == "default" {
		return "default_kapm"
	}
	digest := sha256.Sum256([]byte(tenantID))
	return fmt.Sprintf("tenant_%x_kapm", digest[:8])
}

func matchesApplicationSpanTable(table string, appID int64) bool {
	return appID > 0 && (table == "cw_trace_span" ||
		table == fmt.Sprintf("cw_trace_span_app_%d", appID))
}

// 已发布旧 hash 库保持可读；规范化规则与 Kingeye tenant_shared_span_database 一致。
func matchesTenantSpanDatabase(database, tenantID string) bool {
	if !ValidTenantID(tenantID) {
		return false
	}
	if database == tenantSpanDatabase(tenantID) {
		return true
	}
	readable := strings.Map(func(char rune) rune {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' {
			return char
		}
		return '_'
	}, strings.ToLower(tenantID)) + "_kapm"
	return len(readable) <= 64 && physicalIdentifierPattern.MatchString(readable) && database == readable
}

func matchesPublishedPhysicalTable(profile metadata.TraceProfile, target DorisTarget, scope Scope, request Request) bool {
	return profile == metadata.TraceProfileSpan && matchesApplicationSpanTable(target.Table, exactSpanRouteAppID(request)) &&
		matchesTenantSpanDatabase(target.Database, scope.BkTenantID)
}

// ParseProfile 把配置或线路上的 profile 文本解析成闭合枚举。
func ParseProfile(value string) (metadata.TraceProfile, error) {
	profile := metadata.TraceProfile(value)
	if _, supported := profilePhysicalTable[profile]; !supported {
		return "", fmt.Errorf("profile %s 不受支持", value)
	}
	return profile, nil
}

// NewRouteSnapshot 校验并冻结一份读路由快照。
func NewRouteSnapshot(version int64, routes []ReadRoute) (*RouteSnapshot, error) {
	if version <= 0 {
		return nil, fmt.Errorf("Trace 读路由快照版本必须为正整数")
	}
	snapshot := &RouteSnapshot{
		version: version, routes: make(map[string]ReadRoute, len(routes)),
	}
	for index, route := range routes {
		if err := validateReadRoute(route); err != nil {
			return nil, fmt.Errorf("Trace 读路由第 %d 条非法: %w", index, err)
		}
		key := appRouteKey(route.BkTenantID, route.BkBizID, route.AppID, route.Profile)
		if _, duplicated := snapshot.routes[key]; duplicated {
			return nil, fmt.Errorf("Trace 读路由重复: 租户 %s / 业务 %d / 应用 %d / profile %s", route.BkTenantID, route.BkBizID, route.AppID, route.Profile)
		}
		snapshot.routes[key] = route
	}
	return snapshot, nil
}

func validateReadRoute(route ReadRoute) error {
	if route.PublishedVersion < 0 {
		return fmt.Errorf("Trace 单租户发布版本非法")
	}
	if !ValidTenantID(route.BkTenantID) {
		return fmt.Errorf("租户标识非法")
	}
	if route.AppID <= 0 {
		return fmt.Errorf("Trace 读路由必须提供正整数 app_id，业务路由已退役")
	}
	if (route.ResultTableID != "" || route.DataID != 0) &&
		(strings.TrimSpace(route.ResultTableID) == "" || route.DataID <= 0) {
		return fmt.Errorf("Trace 路由 result_table_id/data_id 必须成对提供非空 RT 和正整数 DataID")
	}
	if route.Doris == nil {
		return fmt.Errorf("缺少 Doris 物理目标")
	}
	if route.Profile != metadata.TraceProfileSpan || route.BkBizID <= 0 {
		return fmt.Errorf("应用级读路由仅支持 Span 和正整数业务 ID")
	}
	if !matchesApplicationSpanTable(route.Doris.Table, route.AppID) {
		return fmt.Errorf("应用 Span 物理表名非法")
	}
	if !matchesTenantSpanDatabase(route.Doris.Database, route.BkTenantID) {
		return fmt.Errorf("应用 Span 物理库与租户不匹配")
	}
	if route.Doris.ClusterID == "" || route.Doris.SchemaVersion == "" {
		return fmt.Errorf("Doris 目标缺少集群或 Schema 版本")
	}
	return nil
}

// Version 返回快照版本。
func (s *RouteSnapshot) Version() int64 {
	if s == nil {
		return 0
	}
	return s.version
}

// ResolveForApplication 仅解析精确应用路由；缺失时明确失败，不回退业务或其它应用。
func (s *RouteSnapshot) ResolveForApplication(
	tenantID string, bizID, appID int64, profile metadata.TraceProfile,
) (ReadRoute, error) {
	if s == nil {
		return ReadRoute{}, reject(CodeRouteNotFound, "Trace 读路由尚未配置")
	}
	if profile != metadata.TraceProfileSpan || appID <= 0 {
		return ReadRoute{}, reject(CodeRouteNotFound, "Trace 读路由要求正整数 app_id 与 Span profile")
	}
	route, found := s.routes[appRouteKey(tenantID, bizID, appID, profile)]
	if !found {
		return ReadRoute{}, reject(
			CodeRouteNotFound, "Trace 读路由缺失: 租户 %s / 业务 %d / 应用 %d / profile %s", tenantID, bizID, appID, profile,
		)
	}
	return route, nil
}
