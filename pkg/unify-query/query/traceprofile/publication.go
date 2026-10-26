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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

var publishedSchemaVersionPattern = regexp.MustCompile(`^sha256:[0-9a-f]{16}$`)

// Kingeye trace_doris_schema.schema_version_for 的当前 Span/Summary DDL 摘要。
// 读投影不兼容时必须随 DDL 一起升级，不能只接受任意形状的摘要字符串。
var supportedPublishedSchemaVersion = map[string]string{
	"trace_span":    "sha256:e6e9af09d86b396e",
	"trace_summary": "sha256:281052267ec373fc",
}

var supportedApplicationTopologyTargets = map[string]struct{ table, schema string }{
	"application_edge":     {"kapm_application_edge_observed_1m", "sha256:19617e34c53d393e"},
	"application_coverage": {"kapm_application_edge_coverage_1m", "sha256:06c86fab2f251999"},
}

// PublishedDocument 是 Kingeye 在 Consul 发布的单租户、带版本只读目标。
type PublishedDocument struct {
	TenantID                   string
	Version                    int64
	Routes                     []ReadRoute
	ESSummaryTargets           []ESSummaryTarget
	ApplicationTopologyTargets []ApplicationTopologyTarget
}

type publishedDocumentWire struct {
	Version                    int64                                    `json:"route_snapshot_version"`
	TenantID                   string                                   `json:"bk_tenant_id"`
	Routes                     []publishedRouteWire                     `json:"read_routes"`
	LegacyA3Targets            []publishedApplicationTopologyTargetWire `json:"a3_targets"`
	ApplicationTopologyTargets []publishedApplicationTopologyTargetWire `json:"application_topology_targets"`
	ESSummaryTargets           []publishedESSummaryTargetWire           `json:"es_summary_targets,omitempty"`
}

type publishedApplicationTopologyTargetWire struct {
	Label               string          `json:"label"`
	ObservationDomainID string          `json:"observation_domain_id"`
	Database            string          `json:"database"`
	Table               string          `json:"table"`
	ClusterID           json.RawMessage `json:"cluster_id"`
	ClusterName         string          `json:"cluster_name"`
	SchemaVersion       string          `json:"schema_version"`
	RetentionDays       int64           `json:"retention_days"`
}

type publishedESSummaryTargetWire struct {
	TenantID            string `json:"bk_tenant_id"`
	BizID               int64  `json:"bk_biz_id"`
	AppID               int64  `json:"app_id"`
	AppName             string `json:"app_name"`
	ObservationDomainID string `json:"observation_domain_id"`
	ResultTableID       string `json:"read_result_table_id"`
	StorageClusterID    int64  `json:"storage_cluster_id"`
	RetentionDays       int64  `json:"retention_days"`
	EntryIndexVersion   int64  `json:"entry_index_version,omitempty"`
}

type publishedRouteWire struct {
	TenantID            string          `json:"bk_tenant_id"`
	BizID               int64           `json:"bk_biz_id"`
	AppID               int64           `json:"app_id,omitempty"`
	Profile             string          `json:"profile"`
	ObservationDomainID string          `json:"observation_domain_id"`
	Database            string          `json:"database"`
	Table               string          `json:"table"`
	ClusterID           json.RawMessage `json:"cluster_id"`
	ClusterName         string          `json:"cluster_name"`
	SchemaVersion       string          `json:"schema_version"`
}

// ParsePublishedDocument 校验 Consul key、完整业务双 profile 与物理坐标。
// 客户端请求不能通过此入口指定库、表或集群。
func ParsePublishedDocument(body []byte, keyTenant string) (PublishedDocument, error) {
	if !ValidTenantID(keyTenant) {
		return PublishedDocument{}, fmt.Errorf("Trace 发布路径缺少可信租户")
	}
	if err := rejectDuplicateKeys(body); err != nil {
		return PublishedDocument{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var wire publishedDocumentWire
	if err := decoder.Decode(&wire); err != nil {
		return PublishedDocument{}, fmt.Errorf("Trace 发布文档非法: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return PublishedDocument{}, fmt.Errorf("Trace 发布文档有多余内容")
	}
	if wire.TenantID != keyTenant || wire.Version <= 0 || wire.Routes == nil {
		return PublishedDocument{}, fmt.Errorf("Trace 发布文档租户、版本或路由缺失")
	}
	if wire.LegacyA3Targets == nil && wire.ApplicationTopologyTargets == nil {
		return PublishedDocument{}, fmt.Errorf("Trace 发布文档 应用拓扑 段非法")
	}
	routes := make([]ReadRoute, 0, len(wire.Routes))
	profilesByBiz := make(map[int64]uint8)
	for index, item := range wire.Routes {
		knownSchema := item.SchemaVersion == supportedPublishedSchemaVersion[item.Profile] ||
			item.Profile == string(metadata.TraceProfileSummary) && item.SchemaVersion == tenantSummarySchemaVersion
		if item.TenantID != keyTenant || item.ClusterName == "" || !publishedSchemaVersionPattern.MatchString(item.SchemaVersion) ||
			!knownSchema {
			return PublishedDocument{}, fmt.Errorf("Trace 发布路由 %d 的租户、集群或 schema 非法", index)
		}
		profile, err := ParseProfile(item.Profile)
		if err != nil {
			return PublishedDocument{}, err
		}
		var clusterID int64
		if len(item.ClusterID) == 0 || json.Unmarshal(item.ClusterID, &clusterID) != nil || clusterID <= 0 {
			return PublishedDocument{}, fmt.Errorf("Trace 发布路由 %d 的集群 ID 非法", index)
		}
		var target DorisTarget
		if item.AppID > 0 {
			target = DorisTarget{
				ClusterID: strconv.FormatInt(clusterID, 10), Database: item.Database,
				Table: item.Table, SchemaVersion: item.SchemaVersion,
			}
		} else {
			var err error
			target, err = NewDorisTarget(strconv.FormatInt(clusterID, 10), item.ObservationDomainID, item.BizID, profile, item.SchemaVersion)
			if err != nil {
				return PublishedDocument{}, err
			}
			if item.Database != target.Database || item.Table != target.Table {
				return PublishedDocument{}, fmt.Errorf("Trace 发布路由 %d 的物理库表漂移", index)
			}
		}
		routes = append(routes, ReadRoute{
			BkTenantID: keyTenant, BkBizID: item.BizID, AppID: item.AppID, Profile: profile,
			ObservationDomainID: item.ObservationDomainID, PublishedVersion: wire.Version, Doris: &target,
		})
		if item.AppID == 0 {
			if item.Profile == "trace_span" {
				profilesByBiz[item.BizID] |= 1
			} else {
				profilesByBiz[item.BizID] |= 2
			}
		}
	}
	for bizID, profiles := range profilesByBiz {
		if profiles != 3 {
			return PublishedDocument{}, fmt.Errorf("业务 %d 的 Trace Span/Summary 读目标不完整", bizID)
		}
	}
	esTargets := make([]ESSummaryTarget, 0, len(wire.ESSummaryTargets))
	for _, item := range wire.ESSummaryTargets {
		if item.TenantID != keyTenant {
			return PublishedDocument{}, fmt.Errorf("ES Summary 读目标租户不一致")
		}
		esTargets = append(esTargets, ESSummaryTarget{
			BkTenantID: item.TenantID, BkBizID: item.BizID, AppID: item.AppID,
			AppName: item.AppName, ObservationDomainID: item.ObservationDomainID,
			ResultTableID: item.ResultTableID, StorageClusterID: item.StorageClusterID,
			RetentionDays: item.RetentionDays, EntryIndexVersion: item.EntryIndexVersion,
			PublishedVersion: wire.Version,
		})
	}
	topologyTargets, err := parsePublishedApplicationTopologyTargets(wire.LegacyA3Targets, keyTenant, wire.Version, false)
	if err != nil {
		return PublishedDocument{}, err
	}
	if wire.ApplicationTopologyTargets != nil {
		newTargets, parseErr := parsePublishedApplicationTopologyTargets(wire.ApplicationTopologyTargets, keyTenant, wire.Version, true)
		if parseErr != nil {
			return PublishedDocument{}, parseErr
		}
		if len(newTargets) == 0 && len(topologyTargets) != 0 {
			newTargets = topologyTargets
		}
		if len(topologyTargets) != 0 {
			legacyByLabel := make(map[string]ApplicationTopologyTarget, len(topologyTargets))
			for _, target := range topologyTargets {
				legacyByLabel[target.Label] = target
			}
			for _, target := range newTargets {
				if legacyByLabel[target.Label] != target {
					return PublishedDocument{}, fmt.Errorf("应用拓扑新旧读目标不一致")
				}
			}
			// 新字段可以只发布边；旧字段仍保留覆盖读目标供 v1 滚动升级使用。
			if len(newTargets) == 1 {
				newTargets = append(newTargets, legacyByLabel["application_coverage"])
			}
		}
		topologyTargets = newTargets
	}
	if _, err := NewRouteSnapshotWithAllTargets(wire.Version, routes, esTargets, topologyTargets); err != nil {
		return PublishedDocument{}, err
	}
	return PublishedDocument{
		TenantID: keyTenant, Version: wire.Version, Routes: routes,
		ESSummaryTargets: esTargets, ApplicationTopologyTargets: topologyTargets,
	}, nil
}

func parsePublishedApplicationTopologyTargets(wire []publishedApplicationTopologyTargetWire, tenantID string, version int64, descriptiveLabels bool) ([]ApplicationTopologyTarget, error) {
	if len(wire) == 0 {
		return nil, nil
	}
	if len(wire) != 2 && (!descriptiveLabels || len(wire) != 1) {
		return nil, fmt.Errorf("应用拓扑读目标必须包含边表；旧版边/覆盖仍须成对发布")
	}
	targets := make([]ApplicationTopologyTarget, 0, 2)
	seen := make(map[string]bool, 2)
	for _, item := range wire {
		label := item.Label
		if descriptiveLabels {
			if label != "application_edge" && label != "application_coverage" {
				return nil, fmt.Errorf("应用拓扑读目标标签非法")
			}
		} else {
			switch item.Label {
			case "a3_edge":
				label = "application_edge"
			case "a3_coverage":
				label = "application_coverage"
			default:
				return nil, fmt.Errorf("旧版应用拓扑读目标标签非法")
			}
		}
		identity, known := supportedApplicationTopologyTargets[label]
		var clusterID int64
		if !known || seen[label] || item.ObservationDomainID != tenantID+"_kapm" ||
			item.Database != item.ObservationDomainID || item.Table != identity.table ||
			item.SchemaVersion != identity.schema || item.RetentionDays != 30 ||
			item.ClusterName == "" || json.Unmarshal(item.ClusterID, &clusterID) != nil || clusterID <= 0 {
			return nil, fmt.Errorf("应用拓扑 读目标身份、物理表、集群或 Schema 非法")
		}
		seen[label] = true
		targets = append(targets, ApplicationTopologyTarget{
			BkTenantID: tenantID, Label: label, Database: item.Database, Table: item.Table,
			ClusterID: strconv.FormatInt(clusterID, 10), ClusterName: item.ClusterName,
			SchemaVersion: item.SchemaVersion, RetentionDays: item.RetentionDays,
			PublishedVersion: version,
		})
	}
	if !seen["application_edge"] || len(targets) == 2 && !seen["application_coverage"] {
		return nil, fmt.Errorf("应用拓扑读目标缺少边表")
	}
	if len(targets) == 2 && (targets[0].ClusterID != targets[1].ClusterID || targets[0].ClusterName != targets[1].ClusterName) {
		return nil, fmt.Errorf("应用拓扑 边与覆盖读目标集群不一致")
	}
	return targets, nil
}
