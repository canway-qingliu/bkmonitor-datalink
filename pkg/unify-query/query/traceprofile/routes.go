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
	BkTenantID          string
	BkBizID             int64
	Profile             metadata.TraceProfile
	ObservationDomainID string
	// PublishedVersion 是 Kingeye 单租户文档版本；快照 Version 是所有租户的合成版本。
	PublishedVersion int64
	Doris            *DorisTarget
}

// ESSummaryTarget 是按应用发布的新 ES Summary 逻辑读 RT；不代表索引已暖机。
type ESSummaryTarget struct {
	BkTenantID          string
	BkBizID             int64
	AppID               int64
	AppName             string
	ObservationDomainID string
	ResultTableID       string
	StorageClusterID    int64
	RetentionDays       int64
	PublishedVersion    int64
}

// RouteSnapshot 是一次发布版本的只读快照。版本不匹配、路由缺失或物理坐标
// 与冻结命名漂移时一律 fail closed，不猜、不回退。
type RouteSnapshot struct {
	version         int64
	routes          map[string]ReadRoute
	esSummaryRoutes map[string]ESSummaryTarget
}

var physicalIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// 物理表名由受信业务 ID 和闭合 profile 派生，不接受请求方指定。
var profilePhysicalTable = map[metadata.TraceProfile]string{
	metadata.TraceProfileSpan:    "otel_trace_span",
	metadata.TraceProfileSummary: "otel_trace_summary",
}

func routeKey(tenantID string, bizID int64, profile metadata.TraceProfile) string {
	return fmt.Sprintf("%s\x00%d\x00%s", tenantID, bizID, profile)
}

func esSummaryRouteKey(tenantID string, bizID, appID int64) string {
	return fmt.Sprintf("%s\x00%d\x00%d", tenantID, bizID, appID)
}

func businessPhysicalTable(profile metadata.TraceProfile, bizID int64) (string, error) {
	base, supported := profilePhysicalTable[profile]
	if !supported || bizID <= 0 {
		return "", fmt.Errorf("Trace profile 或业务 ID 不受支持")
	}
	return fmt.Sprintf("%s_%d", base, bizID), nil
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
func NewDorisTarget(clusterID, observationDomainID string, bizID int64, profile metadata.TraceProfile, schemaVersion string) (DorisTarget, error) {
	table, err := businessPhysicalTable(profile, bizID)
	if err != nil {
		return DorisTarget{}, err
	}
	if !physicalIdentifierPattern.MatchString(observationDomainID) {
		return DorisTarget{}, fmt.Errorf("观测域作为物理库名非法: %s", observationDomainID)
	}
	if clusterID == "" || schemaVersion == "" {
		return DorisTarget{}, fmt.Errorf("Doris 目标缺少集群或 Schema 版本")
	}
	return DorisTarget{
		ClusterID:     clusterID,
		Database:      observationDomainID,
		Table:         table,
		SchemaVersion: schemaVersion,
	}, nil
}

// NewRouteSnapshot 校验并冻结一份读路由快照。
func NewRouteSnapshot(version int64, routes []ReadRoute) (*RouteSnapshot, error) {
	return NewRouteSnapshotWithESTargets(version, routes, nil)
}

// NewRouteSnapshotWithESTargets 冻结 Doris 路由与 ES Summary 应用目标。
func NewRouteSnapshotWithESTargets(version int64, routes []ReadRoute, esTargets []ESSummaryTarget) (*RouteSnapshot, error) {
	if version <= 0 {
		return nil, fmt.Errorf("Trace 读路由快照版本必须为正整数")
	}
	snapshot := &RouteSnapshot{
		version: version, routes: make(map[string]ReadRoute, len(routes)),
		esSummaryRoutes: make(map[string]ESSummaryTarget, len(esTargets)),
	}
	for index, route := range routes {
		if err := validateReadRoute(route); err != nil {
			return nil, fmt.Errorf("Trace 读路由第 %d 条非法: %w", index, err)
		}
		key := routeKey(route.BkTenantID, route.BkBizID, route.Profile)
		if _, duplicated := snapshot.routes[key]; duplicated {
			return nil, fmt.Errorf("Trace 读路由重复: 租户 %s / 业务 %d / profile %s", route.BkTenantID, route.BkBizID, route.Profile)
		}
		snapshot.routes[key] = route
	}
	for index, target := range esTargets {
		if err := validateESSummaryTarget(target); err != nil {
			return nil, fmt.Errorf("ES Summary 读目标第 %d 条非法: %w", index, err)
		}
		key := esSummaryRouteKey(target.BkTenantID, target.BkBizID, target.AppID)
		if _, duplicated := snapshot.esSummaryRoutes[key]; duplicated {
			return nil, fmt.Errorf("ES Summary 应用读目标重复")
		}
		snapshot.esSummaryRoutes[key] = target
	}
	return snapshot, nil
}

var logicalResultTablePattern = regexp.MustCompile(`^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$`)

func validateESSummaryTarget(target ESSummaryTarget) error {
	if !physicalIdentifierPattern.MatchString(target.BkTenantID) || target.BkBizID <= 0 || target.AppID <= 0 ||
		target.AppName == "" || target.ObservationDomainID != target.BkTenantID+"_kapm" ||
		!logicalResultTablePattern.MatchString(target.ResultTableID) || target.StorageClusterID <= 0 ||
		target.RetentionDays < 1 || target.RetentionDays > 30 || target.PublishedVersion < 0 {
		return fmt.Errorf("ES Summary 应用身份、逻辑 RT、集群或保留期非法")
	}
	return nil
}

func validateReadRoute(route ReadRoute) error {
	if route.PublishedVersion < 0 {
		return fmt.Errorf("Trace 单租户发布版本非法")
	}
	if route.BkTenantID == "" || !physicalIdentifierPattern.MatchString(route.BkTenantID) {
		return fmt.Errorf("租户标识非法")
	}
	table, err := businessPhysicalTable(route.Profile, route.BkBizID)
	if err != nil {
		return err
	}
	// 观测域与物理库名同源（裁定 A）：<bk_tenant_id>_kapm，两处漂移即拒绝。
	if route.ObservationDomainID != route.BkTenantID+"_kapm" {
		return fmt.Errorf("观测域与租户不匹配")
	}
	if route.Doris == nil {
		return fmt.Errorf("缺少 Doris 物理目标")
	}
	if route.Doris.Database != route.ObservationDomainID {
		return fmt.Errorf("物理库与观测域不匹配")
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

// Resolve 按 (租户, profile) 解析读目标，并校验请求携带的观测域。
func (s *RouteSnapshot) Resolve(
	tenantID string, bizID int64, profile metadata.TraceProfile, observationDomainID string,
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
	if observationDomainID != route.ObservationDomainID {
		return ReadRoute{}, reject(
			CodeUnauthorizedDomain, "Trace 观测域不在可信路由内: %s", observationDomainID,
		)
	}
	return route, nil
}

// ResolveESSummary 用成对业务/应用身份解析受信新 ES Summary 逻辑 RT。
func (s *RouteSnapshot) ResolveESSummary(tenantID string, bizID, appID int64, observationDomainID string) (ESSummaryTarget, error) {
	if s == nil {
		return ESSummaryTarget{}, reject(CodeRouteNotFound, "ES Summary 读路由尚未配置")
	}
	target, ok := s.esSummaryRoutes[esSummaryRouteKey(tenantID, bizID, appID)]
	if !ok {
		return ESSummaryTarget{}, reject(CodeRouteNotFound, "ES Summary 应用读路由缺失")
	}
	if target.ObservationDomainID != observationDomainID {
		return ESSummaryTarget{}, reject(CodeUnauthorizedDomain, "ES Summary 观测域不在可信路由内")
	}
	return target, nil
}
