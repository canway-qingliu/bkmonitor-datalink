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
	"context"
	"fmt"
	"strings"
	"time"
)

// ApplicationTopologyApplication 是 KAPM 当前租户应用目录的一项；业务 ID 与 Namespace ID 必须成对验证。
type ApplicationTopologyApplication struct {
	NamespaceID int64 `json:"namespace_id"`
	BkBizID     int64 `json:"bk_biz_id"`
}

type ApplicationTopologyRangeRequest struct {
	TenantID        string
	ContractVersion string
	ExpectedVersion int64
	StartMS         int64
	EndMS           int64
	Applications    []ApplicationTopologyApplication
	MaxPairs        int64
}

type ApplicationTopologyEdge struct {
	SourceNamespaceID int64  `json:"source_namespace_id"`
	SourceBkBizID     int64  `json:"source_bk_biz_id"`
	TargetNamespaceID int64  `json:"target_namespace_id"`
	TargetBkBizID     int64  `json:"target_bk_biz_id"`
	Resolution        string `json:"resolution"`
	ObservedMinutes   int64  `json:"observed_minutes"`
	AsOfMS            int64  `json:"as_of_ms"`
}

type ApplicationTopologyCoverage struct {
	SourceNamespaceID int64 `json:"source_namespace_id"`
	SourceBkBizID     int64 `json:"source_bk_biz_id"`
	ObservedMinutes   int64 `json:"observed_minutes"`
	CompleteMinutes   int64 `json:"complete_minutes"`
	PartialMinutes    int64 `json:"partial_minutes"`
	PendingMinutes    int64 `json:"pending_minutes"`
	UnresolvedPeers   int64 `json:"unresolved_peers"`
	AsOfMS            int64 `json:"as_of_ms"`
}

type ApplicationTopologyRangeResult struct {
	PublishedRouteVersion int64                         `json:"published_route_version"`
	ExpectedMinutes       int64                         `json:"expected_minutes"`
	Edges                 []ApplicationTopologyEdge     `json:"edges"`
	Coverage              []ApplicationTopologyCoverage `json:"coverage"`
}

// QueryApplicationTopologyRange 从同一租户的已发布两表读取最长 30 天的有向应用对和覆盖汇总。
// 每表只有一次固定形态 SQL；任何行数、业务身份或发布版本漂移均拒绝。
func (e *Executor) QueryApplicationTopologyRange(ctx context.Context, request ApplicationTopologyRangeRequest) (ApplicationTopologyRangeResult, error) {
	if e == nil || e.snapshot == nil || request.TenantID == "" ||
		request.StartMS < 946684800000 || request.StartMS%60000 != 0 || request.EndMS%60000 != 0 ||
		request.EndMS <= request.StartMS || request.EndMS-request.StartMS > 30*24*60*60000 ||
		len(request.Applications) == 0 || len(request.Applications) > 129 ||
		request.MaxPairs < 1 || request.MaxPairs > 10000 {
		return ApplicationTopologyRangeResult{}, reject(CodeUnboundedRange, "应用拓扑 全景查询时间、成员或行预算非法")
	}
	allowed := make(map[int64]int64, len(request.Applications))
	previous := int64(0)
	ids := make([]any, 0, len(request.Applications))
	for _, app := range request.Applications {
		if app.NamespaceID <= previous || app.BkBizID <= 0 {
			return ApplicationTopologyRangeResult{}, reject(CodeUnauthorizedMember, "应用拓扑 应用目录未按身份升序冻结")
		}
		allowed[app.NamespaceID] = app.BkBizID
		ids = append(ids, app.NamespaceID)
		previous = app.NamespaceID
	}
	edgeTarget, err := e.snapshot.ResolveApplicationTopologyTarget(request.TenantID, "application_edge")
	if err != nil {
		return ApplicationTopologyRangeResult{}, err
	}
	if edgeTarget.PublishedVersion != request.ExpectedVersion {
		return ApplicationTopologyRangeResult{}, reject(CodeScopePartial, "应用拓扑 边表发布版本不一致")
	}
	observedOnly := request.ContractVersion == "application_topology_range_v2"
	var coverageTarget ApplicationTopologyTarget
	if !observedOnly {
		coverageTarget, err = e.snapshot.ResolveApplicationTopologyTarget(request.TenantID, "application_coverage")
		if err != nil {
			return ApplicationTopologyRangeResult{}, err
		}
		if edgeTarget.PublishedVersion != coverageTarget.PublishedVersion || edgeTarget.ClusterID != coverageTarget.ClusterID {
			return ApplicationTopologyRangeResult{}, reject(CodeScopePartial, "应用拓扑 边与覆盖发布目标不一致")
		}
	}
	connection := e.connections[edgeTarget.ClusterID]
	if connection == nil {
		return ApplicationTopologyRangeResult{}, reject(CodeRouteNotFound, "应用拓扑 只读连接不可用")
	}
	startDate := time.UnixMilli(request.StartMS).UTC().Format("2006-01-02")
	endDate := time.UnixMilli(request.EndMS - 1).UTC().Format("2006-01-02")
	marks := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := []any{request.TenantID, startDate, endDate, request.StartMS, request.EndMS}
	args = append(args, ids...)
	coverage := make([]ApplicationTopologyCoverage, 0)
	if !observedOnly {
		coverageSQL := fmt.Sprintf(
			"SELECT source_namespace_id, source_bk_biz_id, COUNT(*) AS observed_minutes, "+
				"SUM(CASE WHEN status = 'complete' AND unresolved_peer_count = 0 AND config_revision > 0 "+
				"AND input_fence_hash IS NOT NULL THEN 1 ELSE 0 END) AS complete_minutes, "+
				"SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) AS partial_minutes, "+
				"SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_minutes, "+
				"SUM(unresolved_peer_count) AS unresolved_peers, MAX(as_of_ms) AS as_of_ms "+
				"FROM %s.%s WHERE bk_tenant_id = ? AND coverage_date >= ? AND coverage_date <= ? "+
				"AND minute_start_ms >= ? AND minute_start_ms < ? AND source_namespace_id IN (%s) "+
				"GROUP BY source_namespace_id, source_bk_biz_id ORDER BY source_namespace_id LIMIT 130",
			quoteIdentifier(coverageTarget.Database), quoteIdentifier(coverageTarget.Table), marks,
		)
		coverageRows, queryErr := connection.Query(ctx, coverageSQL, args...)
		if queryErr != nil {
			return ApplicationTopologyRangeResult{}, queryFailure(queryErr)
		}
		if len(coverageRows.Rows) > len(allowed) {
			return ApplicationTopologyRangeResult{}, reject(CodePartialFailure, "应用拓扑 覆盖源集合超出当前应用目录")
		}
		for _, row := range coverageRows.Rows {
			item, decodeErr := decodeApplicationTopologyCoverage(row, allowed, (request.EndMS-request.StartMS)/60000)
			if decodeErr != nil {
				return ApplicationTopologyRangeResult{}, decodeErr
			}
			coverage = append(coverage, item)
		}
	}
	var edgeSQL string
	if !observedOnly {
		edgeSQL = fmt.Sprintf(
			"SELECT e.source_namespace_id, e.source_bk_biz_id, e.target_namespace_id, e.target_bk_biz_id, "+
				"MIN(e.resolution) AS min_resolution, MAX(e.resolution) AS max_resolution, "+
				"COUNT(*) AS observed_minutes, MAX(e.as_of_ms) AS as_of_ms, "+
				"SUM(CASE WHEN e.observed THEN 0 ELSE 1 END) AS unobserved_count, "+
				"SUM(CASE WHEN c.status = 'complete' AND (c.edge_visible_version_ms IS NULL "+
				"OR e.computed_at_ms > c.edge_visible_version_ms) THEN 1 ELSE 0 END) AS beyond_watermark "+
				"FROM %s.%s e LEFT JOIN %s.%s c ON c.bk_tenant_id = e.bk_tenant_id "+
				"AND c.coverage_date = e.edge_date AND c.minute_start_ms = e.minute_start_ms "+
				"AND c.source_namespace_id = e.source_namespace_id AND c.source_bk_biz_id = e.source_bk_biz_id "+
				"WHERE e.bk_tenant_id = ? AND e.edge_date >= ? AND e.edge_date <= ? "+
				"AND e.minute_start_ms >= ? AND e.minute_start_ms < ? "+
				"AND e.source_namespace_id IN (%s) AND e.target_namespace_id IN (%s) "+
				"GROUP BY e.source_namespace_id, e.source_bk_biz_id, e.target_namespace_id, e.target_bk_biz_id "+
				"ORDER BY e.source_namespace_id, e.target_namespace_id LIMIT %d",
			quoteIdentifier(edgeTarget.Database), quoteIdentifier(edgeTarget.Table),
			quoteIdentifier(coverageTarget.Database), quoteIdentifier(coverageTarget.Table),
			marks, marks, request.MaxPairs,
		)
	} else {
		edgeSQL = fmt.Sprintf(
			"SELECT e.source_namespace_id, e.source_bk_biz_id, e.target_namespace_id, e.target_bk_biz_id, "+
				"MIN(e.resolution) AS min_resolution, MAX(e.resolution) AS max_resolution, "+
				"COUNT(*) AS observed_minutes, MAX(e.as_of_ms) AS as_of_ms, "+
				"SUM(CASE WHEN e.observed THEN 0 ELSE 1 END) AS unobserved_count "+
				"FROM %s.%s e "+
				"WHERE e.bk_tenant_id = ? AND e.edge_date >= ? AND e.edge_date <= ? "+
				"AND e.minute_start_ms >= ? AND e.minute_start_ms < ? "+
				"AND e.source_namespace_id IN (%s) AND e.target_namespace_id IN (%s) "+
				"GROUP BY e.source_namespace_id, e.source_bk_biz_id, e.target_namespace_id, e.target_bk_biz_id "+
				"ORDER BY e.source_namespace_id, e.target_namespace_id LIMIT %d",
			quoteIdentifier(edgeTarget.Database), quoteIdentifier(edgeTarget.Table), marks, marks, request.MaxPairs,
		)
	}
	edgeArgs := append(append([]any{}, args...), ids...)
	edgeRows, err := connection.Query(ctx, edgeSQL, edgeArgs...)
	if err != nil {
		return ApplicationTopologyRangeResult{}, queryFailure(err)
	}
	if int64(len(edgeRows.Rows)) >= request.MaxPairs {
		return ApplicationTopologyRangeResult{}, reject(CodeUnboundedRange, "应用拓扑 应用对超过行预算")
	}
	edges := make([]ApplicationTopologyEdge, 0, len(edgeRows.Rows))
	for _, row := range edgeRows.Rows {
		item, decodeErr := decodeApplicationTopologyEdge(row, allowed, (request.EndMS-request.StartMS)/60000, !observedOnly)
		if decodeErr != nil {
			return ApplicationTopologyRangeResult{}, decodeErr
		}
		edges = append(edges, item)
	}
	return ApplicationTopologyRangeResult{
		PublishedRouteVersion: edgeTarget.PublishedVersion,
		ExpectedMinutes:       (request.EndMS - request.StartMS) / 60000,
		Edges:                 edges, Coverage: coverage,
	}, nil
}

func applicationTopologyInt(row map[string]any, name string) (int64, error) {
	value, err := exactTraceInt64(row[name])
	if err != nil {
		return 0, reject(CodePartialFailure, "应用拓扑 聚合行整数非法")
	}
	return value, nil
}

func decodeApplicationTopologyCoverage(row map[string]any, allowed map[int64]int64, expected int64) (ApplicationTopologyCoverage, error) {
	id, err := applicationTopologyInt(row, "source_namespace_id")
	if err != nil {
		return ApplicationTopologyCoverage{}, err
	}
	biz, err := applicationTopologyInt(row, "source_bk_biz_id")
	if err != nil {
		return ApplicationTopologyCoverage{}, err
	}
	expectedBiz, found := allowed[id]
	if !found || biz <= 0 || expectedBiz != biz {
		return ApplicationTopologyCoverage{}, reject(CodeUnauthorizedMember, "应用拓扑 覆盖源身份越界")
	}
	fields := []string{"observed_minutes", "complete_minutes", "partial_minutes", "pending_minutes", "unresolved_peers", "as_of_ms"}
	values := make([]int64, len(fields))
	for i, field := range fields {
		values[i], err = applicationTopologyInt(row, field)
		if err != nil {
			return ApplicationTopologyCoverage{}, err
		}
	}
	if values[0] < 0 || values[0] > expected ||
		values[1] < 0 || values[2] < 0 || values[3] < 0 ||
		values[1]+values[2]+values[3] > values[0] ||
		values[4] < 0 || values[5] <= 0 {
		return ApplicationTopologyCoverage{}, reject(CodePartialFailure, "应用拓扑 覆盖汇总超出查询窗口")
	}
	return ApplicationTopologyCoverage{
		SourceNamespaceID: id, SourceBkBizID: biz, ObservedMinutes: values[0],
		CompleteMinutes: values[1], PartialMinutes: values[2], PendingMinutes: values[3],
		UnresolvedPeers: values[4], AsOfMS: values[5],
	}, nil
}

func decodeApplicationTopologyEdge(row map[string]any, allowed map[int64]int64, expectedMinutes int64, checkWatermark bool) (ApplicationTopologyEdge, error) {
	fields := []string{"source_namespace_id", "source_bk_biz_id", "target_namespace_id", "target_bk_biz_id", "observed_minutes", "as_of_ms", "unobserved_count"}
	if checkWatermark {
		fields = append(fields, "beyond_watermark")
	}
	values := make([]int64, len(fields))
	var err error
	for i, field := range fields {
		values[i], err = applicationTopologyInt(row, field)
		if err != nil {
			return ApplicationTopologyEdge{}, err
		}
	}
	resolution, _ := row["min_resolution"].(string)
	maximum, _ := row["max_resolution"].(string)
	validResolution := func(value string) bool { return value == "stable" || value == "unique_service_name" }
	sourceBiz, sourceFound := allowed[values[0]]
	targetBiz, targetFound := allowed[values[2]]
	if !sourceFound || !targetFound || values[0] == values[2] ||
		values[1] <= 0 || values[3] <= 0 || sourceBiz != values[1] || targetBiz != values[3] ||
		values[4] <= 0 || values[4] > expectedMinutes || values[5] <= 0 || values[6] != 0 ||
		checkWatermark && values[7] != 0 ||
		!validResolution(resolution) || !validResolution(maximum) {
		return ApplicationTopologyEdge{}, reject(CodeUnauthorizedMember, "应用拓扑 有向边身份或解析证据非法")
	}
	if resolution == "unique_service_name" || maximum == "unique_service_name" {
		resolution = "unique_service_name"
	}
	return ApplicationTopologyEdge{
		SourceNamespaceID: values[0], SourceBkBizID: values[1],
		TargetNamespaceID: values[2], TargetBkBizID: values[3],
		Resolution: resolution, ObservedMinutes: values[4], AsOfMS: values[5],
	}, nil
}
