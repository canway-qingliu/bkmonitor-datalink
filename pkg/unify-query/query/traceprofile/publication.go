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
	"strings"
)

var publishedSchemaVersionPattern = regexp.MustCompile(`^sha256:[0-9a-f]{16}$`)

// Kingeye trace_doris_schema.schema_version_for 的当前 Span DDL 摘要。
// 读投影不兼容时必须随 DDL 一起升级，不能只接受任意形状的摘要字符串。
var supportedPublishedSchemaVersion = map[string]string{
	"trace_span": "sha256:0bf5aac6160a636e",
}

// PublishedDocument 是 Kingeye 发布的单租户、带版本 Redis 只读目标。
type PublishedDocument struct {
	TenantID string
	Version  int64
	Routes   []ReadRoute
}

type publishedDocumentWire struct {
	Version  int64                `json:"route_snapshot_version"`
	TenantID string               `json:"bk_tenant_id"`
	Routes   []publishedRouteWire `json:"read_routes"`
	// 已退役段：仍接受旧发布文档中的这些字段以完成滚动升级，但不解析、不生效。
	LegacyApplicationTopologyTargets json.RawMessage `json:"a3_targets,omitempty"`
	ApplicationTopologyTargets       json.RawMessage `json:"application_topology_targets,omitempty"`
	ESSummaryTargets                 json.RawMessage `json:"es_summary_targets,omitempty"`
}

type publishedRouteWire struct {
	TenantID      string          `json:"bk_tenant_id"`
	BizID         int64           `json:"bk_biz_id"`
	AppID         int64           `json:"app_id"`
	ResultTableID json.RawMessage `json:"result_table_id,omitempty"`
	DataID        json.RawMessage `json:"data_id,omitempty"`
	Profile       string          `json:"profile"`
	Database      string          `json:"database"`
	Table         string          `json:"table"`
	ClusterID     json.RawMessage `json:"cluster_id"`
	ClusterName   string          `json:"cluster_name"`
	SchemaVersion string          `json:"schema_version"`
}

// ParseRouteSourceIdentity 解码可选、成对提供的 RT/DataID；显式 null 或零值不等同于缺省。
func ParseRouteSourceIdentity(resultTableJSON, dataIDJSON json.RawMessage) (string, int64, error) {
	if len(resultTableJSON) == 0 && len(dataIDJSON) == 0 {
		return "", 0, nil
	}
	var resultTableID string
	var dataID int64
	if json.Unmarshal(resultTableJSON, &resultTableID) != nil || strings.TrimSpace(resultTableID) == "" ||
		json.Unmarshal(dataIDJSON, &dataID) != nil || dataID <= 0 {
		return "", 0, fmt.Errorf("Trace 路由 result_table_id/data_id 必须成对提供非空 RT 和正整数 DataID")
	}
	return resultTableID, dataID, nil
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
		resultTableID, dataID, err := ParseRouteSourceIdentity(item.ResultTableID, item.DataID)
		if err != nil {
			return PublishedDocument{}, fmt.Errorf("Trace 发布路由 %d 的来源身份非法: %w", index, err)
		}
		// 1. 保留发布文档明确登记的物理名，不派生或替换。
		target := DorisTarget{
			ClusterID: strconv.FormatInt(clusterID, 10), Database: item.Database,
			Table: item.Table, SchemaVersion: item.SchemaVersion,
		}
		routes = append(routes, ReadRoute{
			BkTenantID: keyTenant, BkBizID: item.BizID, AppID: item.AppID, Profile: profile,
			ResultTableID: resultTableID, DataID: dataID,
			PublishedVersion: wire.Version, Doris: &target,
		})
	}
	// 2. 同一闭合规则核对正整数应用、租户库、应用表名、重复作用域及目标身份。
	if _, err := NewRouteSnapshot(wire.Version, routes); err != nil {
		return PublishedDocument{}, err
	}
	return PublishedDocument{TenantID: keyTenant, Version: wire.Version, Routes: routes}, nil
}
