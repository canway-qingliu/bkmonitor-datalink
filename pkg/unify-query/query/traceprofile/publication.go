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
)

var publishedSchemaVersionPattern = regexp.MustCompile(`^sha256:[0-9a-f]{16}$`)

// Kingeye trace_doris_schema.schema_version_for 的当前 Span/Summary DDL 摘要。
// 读投影不兼容时必须随 DDL 一起升级，不能只接受任意形状的摘要字符串。
var supportedPublishedSchemaVersion = map[string]string{
	"trace_span":    "sha256:e6e9af09d86b396e",
	"trace_summary": tenantSummarySchemaVersion,
}

var supportedApplicationTopologyTargets = map[string]struct{ table, schema string }{
	"application_edge": {"kapm_application_edge_observed_1m", "sha256:19617e34c53d393e"},
}

// PublishedDocument 是 Kingeye 发布的单租户、带版本 Redis 只读目标。
type PublishedDocument struct {
	TenantID                   string
	Version                    int64
	Routes                     []ReadRoute
	ESSummaryTargets           []ESSummaryTarget
	ApplicationTopologyTargets []ApplicationTopologyTarget
}

type publishedDocumentWire struct {
	Version  int64                `json:"route_snapshot_version"`
	TenantID string               `json:"bk_tenant_id"`
	Routes   []publishedRouteWire `json:"read_routes"`
	// 只为滚动升级读取旧 Redis 文档；旧目标不进入当前应用拓扑查询。
	LegacyApplicationTopologyTargets []publishedApplicationTopologyTargetWire `json:"a3_targets"`
	ApplicationTopologyTargets       []publishedApplicationTopologyTargetWire `json:"application_topology_targets"`
	ESSummaryTargets                 []publishedESSummaryTargetWire           `json:"es_summary_targets,omitempty"`
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

// ParsePublishedDocument 校验租户发布键与物理坐标。
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
	if wire.ApplicationTopologyTargets == nil && wire.LegacyApplicationTopologyTargets == nil {
		return PublishedDocument{}, fmt.Errorf("Trace 发布文档 应用拓扑 段非法")
	}
	routes := make([]ReadRoute, 0, len(wire.Routes))
	for index, item := range wire.Routes {
		knownSchema := item.SchemaVersion == supportedPublishedSchemaVersion[item.Profile]
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
	topologyTargets, err := parsePublishedApplicationTopologyTargets(
		wire.ApplicationTopologyTargets, keyTenant, wire.Version)
	if err != nil {
		return PublishedDocument{}, err
	}
	if _, err := NewRouteSnapshotWithAllTargets(wire.Version, routes, esTargets, topologyTargets); err != nil {
		return PublishedDocument{}, err
	}
	return PublishedDocument{
		TenantID: keyTenant, Version: wire.Version, Routes: routes,
		ESSummaryTargets: esTargets, ApplicationTopologyTargets: topologyTargets,
	}, nil
}

func parsePublishedApplicationTopologyTargets(wire []publishedApplicationTopologyTargetWire, tenantID string, version int64) ([]ApplicationTopologyTarget, error) {
	if len(wire) == 0 {
		return nil, nil
	}
	if len(wire) != 1 {
		return nil, fmt.Errorf("应用拓扑只允许一个已观测边目标")
	}
	item := wire[0]
	identity := supportedApplicationTopologyTargets["application_edge"]
	var clusterID int64
	if item.Label != "application_edge" || item.ObservationDomainID != tenantID+"_kapm" ||
		item.Database != item.ObservationDomainID || item.Table != identity.table ||
		item.SchemaVersion != identity.schema || item.RetentionDays != 30 ||
		item.ClusterName == "" || json.Unmarshal(item.ClusterID, &clusterID) != nil || clusterID <= 0 {
		return nil, fmt.Errorf("应用拓扑读目标身份、物理表、集群或 Schema 非法")
	}
	return []ApplicationTopologyTarget{{
		BkTenantID: tenantID, Label: item.Label, Database: item.Database, Table: item.Table,
		ClusterID: strconv.FormatInt(clusterID, 10), ClusterName: item.ClusterName,
		SchemaVersion: item.SchemaVersion, RetentionDays: item.RetentionDays,
		PublishedVersion: version,
	}}, nil
}
