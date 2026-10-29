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

// ApplicationTopologyApplication 是当前租户应用目录的一项，业务与 Namespace 必须成对验证。
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

type ApplicationTopologyRangeResult struct {
	PublishedRouteVersion int64                     `json:"published_route_version"`
	ExpectedMinutes       int64                     `json:"expected_minutes"`
	Edges                 []ApplicationTopologyEdge `json:"edges"`
}

// QueryApplicationTopologyRange 仅从已发布边表读取真实双端稳定应用边。
// 空结果表示暂未观测到，不表示该时间窗的所有应用间调用已完整覆盖。
func (e *Executor) QueryApplicationTopologyRange(ctx context.Context, request ApplicationTopologyRangeRequest) (ApplicationTopologyRangeResult, error) {
	if e == nil || e.snapshot == nil || request.TenantID == "" ||
		request.ContractVersion != observedApplicationTopologyRangeVersion ||
		request.StartMS < 946684800000 || request.StartMS%60000 != 0 || request.EndMS%60000 != 0 ||
		request.EndMS <= request.StartMS || request.EndMS-request.StartMS > 30*24*60*60000 ||
		len(request.Applications) == 0 || len(request.Applications) > 129 ||
		request.MaxPairs < 1 || request.MaxPairs > 10000 {
		return ApplicationTopologyRangeResult{}, reject(CodeUnboundedRange, "应用拓扑查询时间、成员或行预算非法")
	}
	allowed := make(map[int64]int64, len(request.Applications))
	ids := make([]any, 0, len(request.Applications))
	previous := int64(0)
	for _, app := range request.Applications {
		if app.NamespaceID <= previous || app.BkBizID <= 0 {
			return ApplicationTopologyRangeResult{}, reject(CodeUnauthorizedMember, "应用拓扑应用目录未按身份升序冻结")
		}
		allowed[app.NamespaceID] = app.BkBizID
		ids = append(ids, app.NamespaceID)
		previous = app.NamespaceID
	}
	target, err := e.snapshot.ResolveApplicationTopologyTarget(request.TenantID, "application_edge")
	if err != nil {
		return ApplicationTopologyRangeResult{}, err
	}
	if target.PublishedVersion != request.ExpectedVersion {
		return ApplicationTopologyRangeResult{}, reject(CodeScopePartial, "应用拓扑边表发布版本不一致")
	}
	connection := e.connections[target.ClusterID]
	if connection == nil {
		return ApplicationTopologyRangeResult{}, reject(CodeRouteNotFound, "应用拓扑只读连接不可用")
	}
	marks := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	statement := fmt.Sprintf(
		"SELECT e.source_namespace_id, e.source_bk_biz_id, e.target_namespace_id, e.target_bk_biz_id, "+
			"MIN(e.resolution) AS min_resolution, MAX(e.resolution) AS max_resolution, "+
			"COUNT(*) AS observed_minutes, MAX(e.as_of_ms) AS as_of_ms, "+
			"SUM(CASE WHEN e.observed THEN 0 ELSE 1 END) AS unobserved_count "+
			"FROM %s.%s e WHERE e.edge_date >= ? AND e.edge_date <= ? "+
			"AND e.minute_start_ms >= ? AND e.minute_start_ms < ? AND e.resolution = 'stable' "+
			"AND e.source_namespace_id IN (%s) AND e.target_namespace_id IN (%s) "+
			"GROUP BY e.source_namespace_id, e.source_bk_biz_id, e.target_namespace_id, e.target_bk_biz_id "+
			"ORDER BY e.source_namespace_id, e.target_namespace_id LIMIT %d",
		quoteIdentifier(target.Database), quoteIdentifier(target.Table), marks, marks, request.MaxPairs+1,
	)
	startDate := time.UnixMilli(request.StartMS).UTC().Format("2006-01-02")
	endDate := time.UnixMilli(request.EndMS - 1).UTC().Format("2006-01-02")
	args := append([]any{startDate, endDate, request.StartMS, request.EndMS}, ids...)
	args = append(args, ids...)
	rows, err := connection.Query(ctx, statement, args...)
	if err != nil {
		return ApplicationTopologyRangeResult{}, queryFailure(err)
	}
	if int64(len(rows.Rows)) > request.MaxPairs {
		return ApplicationTopologyRangeResult{}, reject(CodeUnboundedRange, "应用拓扑应用对超过行预算")
	}
	edges := make([]ApplicationTopologyEdge, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		edge, decodeErr := decodeObservedApplicationTopologyEdge(row, allowed, (request.EndMS-request.StartMS)/60000)
		if decodeErr != nil {
			return ApplicationTopologyRangeResult{}, decodeErr
		}
		edges = append(edges, edge)
	}
	return ApplicationTopologyRangeResult{
		PublishedRouteVersion: target.PublishedVersion,
		ExpectedMinutes:       (request.EndMS - request.StartMS) / 60000,
		Edges:                 edges,
	}, nil
}

func decodeObservedApplicationTopologyEdge(row map[string]any, allowed map[int64]int64, expectedMinutes int64) (ApplicationTopologyEdge, error) {
	fields := []string{"source_namespace_id", "source_bk_biz_id", "target_namespace_id", "target_bk_biz_id", "observed_minutes", "as_of_ms", "unobserved_count"}
	values := make([]int64, len(fields))
	for index, field := range fields {
		value, err := exactTraceInt64(row[field])
		if err != nil {
			return ApplicationTopologyEdge{}, reject(CodePartialFailure, "应用拓扑聚合行整数非法")
		}
		values[index] = value
	}
	minimum, _ := row["min_resolution"].(string)
	maximum, _ := row["max_resolution"].(string)
	sourceBiz, sourceFound := allowed[values[0]]
	targetBiz, targetFound := allowed[values[2]]
	if !sourceFound || !targetFound || values[0] == values[2] ||
		values[1] <= 0 || values[3] <= 0 || sourceBiz != values[1] || targetBiz != values[3] ||
		values[4] <= 0 || values[4] > expectedMinutes || values[5] <= 0 || values[6] != 0 ||
		minimum != "stable" || maximum != "stable" {
		return ApplicationTopologyEdge{}, reject(CodeUnauthorizedMember, "应用拓扑有向边身份或双端证据非法")
	}
	return ApplicationTopologyEdge{
		SourceNamespaceID: values[0], SourceBkBizID: values[1],
		TargetNamespaceID: values[2], TargetBkBizID: values[3],
		Resolution: "stable", ObservedMinutes: values[4], AsOfMS: values[5],
	}, nil
}
