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

// ReadRoute 按 (租户, profile) 定位一个物理读目标。
type ReadRoute struct {
	BkTenantID string
	BkBizID    int64
	AppID      int64 // 0 保留旧业务路由；正数只用于新 Span 应用目标。
	Profile    metadata.TraceProfile
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

// 物理表名由受信业务 ID 和闭合 profile 派生，不接受请求方指定。
var profilePhysicalTable = map[metadata.TraceProfile]string{
	metadata.TraceProfileSpan: "otel_trace_span",
}

func routeKey(tenantID string, bizID int64, profile metadata.TraceProfile) string {
	return fmt.Sprintf("%s\x00%d\x00%s", tenantID, bizID, profile)
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

func businessPhysicalTable(profile metadata.TraceProfile, bizID int64) (string, error) {
	base, supported := profilePhysicalTable[profile]
	if !supported || bizID <= 0 {
		return "", fmt.Errorf("Trace profile 或业务 ID 不受支持")
	}
	return fmt.Sprintf("%s_%d", base, bizID), nil
}

func matchesPublishedPhysicalTable(profile metadata.TraceProfile, target DorisTarget, scope Scope, request Request) bool {
	expected, err := businessPhysicalTable(profile, scope.BkBizID)
	if err == nil && target.Table == expected && target.Database == scope.BkTenantID+"_kapm" {
		return true
	}
	appID := exactSpanRouteAppID(request)
	return profile == metadata.TraceProfileSpan && appID > 0 &&
		target.Database == tenantSpanDatabase(scope.BkTenantID) &&
		(target.Table == "otel_trace_span" || target.Table == fmt.Sprintf("otel_trace_span_app_%d", appID))
}

// ParseProfile 把配置或线路上的 profile 文本解析成闭合枚举。
func ParseProfile(value string) (metadata.TraceProfile, error) {
	profile := metadata.TraceProfile(value)
	if _, supported := profilePhysicalTable[profile]; !supported {
		return "", fmt.Errorf("profile %s 不受支持", value)
	}
	return profile, nil
}

// NewDorisTarget 从受信业务与 profile 派生物理表；调用方不能覆盖库表。
func NewDorisTarget(clusterID, database string, bizID int64, profile metadata.TraceProfile, schemaVersion string) (DorisTarget, error) {
	table, err := businessPhysicalTable(profile, bizID)
	if err != nil {
		return DorisTarget{}, err
	}
	if !physicalIdentifierPattern.MatchString(database) {
		return DorisTarget{}, fmt.Errorf("物理库名非法: %s", database)
	}
	if clusterID == "" || schemaVersion == "" {
		return DorisTarget{}, fmt.Errorf("Doris 目标缺少集群或 Schema 版本")
	}
	return DorisTarget{
		ClusterID:     clusterID,
		Database:      database,
		Table:         table,
		SchemaVersion: schemaVersion,
	}, nil
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
		key := routeKey(route.BkTenantID, route.BkBizID, route.Profile)
		if route.AppID > 0 {
			key = appRouteKey(route.BkTenantID, route.BkBizID, route.AppID, route.Profile)
		}
		if _, duplicated := snapshot.routes[key]; duplicated {
			return nil, fmt.Errorf("Trace 读路由重复: 租户 %s / 业务 %d / profile %s", route.BkTenantID, route.BkBizID, route.Profile)
		}
		snapshot.routes[key] = route
	}
	return snapshot, nil
}

func validateReadRoute(route ReadRoute) error {
	if route.PublishedVersion < 0 {
		return fmt.Errorf("Trace 单租户发布版本非法")
	}
	if !ValidTenantID(route.BkTenantID) || route.AppID < 0 {
		return fmt.Errorf("租户标识非法")
	}
	if route.Doris == nil {
		return fmt.Errorf("缺少 Doris 物理目标")
	}
	var table string
	if route.AppID > 0 {
		if route.Profile != metadata.TraceProfileSpan || route.BkBizID <= 0 {
			return fmt.Errorf("应用级读路由仅支持 Span")
		}
		sharedTable, exclusiveTable := "otel_trace_span", fmt.Sprintf("otel_trace_span_app_%d", route.AppID)
		if route.Doris.Table != sharedTable && route.Doris.Table != exclusiveTable {
			return fmt.Errorf("应用 Span 物理表名非法")
		}
		table = route.Doris.Table
		if route.Doris.Database != tenantSpanDatabase(route.BkTenantID) {
			return fmt.Errorf("应用 Span 物理库与租户不匹配")
		}
	} else {
		var err error
		table, err = businessPhysicalTable(route.Profile, route.BkBizID)
		if err != nil {
			return err
		}
		if route.Doris.Database != route.BkTenantID+"_kapm" || !physicalIdentifierPattern.MatchString(route.Doris.Database) {
			return fmt.Errorf("物理库与租户不匹配")
		}
	}
	if route.Doris.Table != table {
		return fmt.Errorf("物理表与 profile 不匹配")
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

// Resolve 按可信租户和 profile 解析已发布读目标。
func (s *RouteSnapshot) Resolve(
	tenantID string, bizID int64, profile metadata.TraceProfile,
) (ReadRoute, error) {
	if s == nil {
		return ReadRoute{}, reject(CodeRouteNotFound, "Trace 读路由尚未配置")
	}
	route, ok := s.routes[routeKey(tenantID, bizID, profile)]
	if !ok {
		return ReadRoute{}, reject(
			CodeRouteNotFound, "Trace 读路由缺失: 租户 %s / 业务 %d / profile %s", tenantID, bizID, profile,
		)
	}
	return route, nil
}

// ResolveForApplication 优先取受信应用 Span 目标，再回退到已发布业务目标。
func (s *RouteSnapshot) ResolveForApplication(
	tenantID string, bizID, appID int64, profile metadata.TraceProfile,
) (ReadRoute, error) {
	if s == nil {
		return ReadRoute{}, reject(CodeRouteNotFound, "Trace 读路由尚未配置")
	}
	if profile == metadata.TraceProfileSpan && appID > 0 {
		if route, found := s.routes[appRouteKey(tenantID, bizID, appID, profile)]; found {
			return route, nil
		}
	}
	return s.Resolve(tenantID, bizID, profile)
}
