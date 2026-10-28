// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
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
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb/elasticsearch"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb/prometheus"
)

type taskSummaryResponse struct {
	Total                      int                            `json:"total"`
	List                       []elasticsearch.TaskSummaryRow `json:"list"`
	BackgroundTaskSummaryExact bool                           `json:"background_task_summary_exact"`
}

type taskSummaryReader interface {
	QueryTaskSummary(context.Context, *metadata.Query, time.Time, time.Time) ([]elasticsearch.TaskSummaryRow, error)
}

func validateTaskSummaryQuery(query *structured.QueryTs) error {
	if query == nil || query.BackgroundTaskSummaryEnvelope == nil {
		return fmt.Errorf("background task summary contract is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(query.BackgroundTaskSummaryEnvelope))
	decoder.DisallowUnknownFields()
	var contract struct {
		Version int `json:"version"`
	}
	if decoder.Decode(&contract) != nil || contract.Version != 1 {
		return fmt.Errorf("background task summary version is invalid")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("background task summary contract has trailing content")
	}
	if len(query.QueryList) != 1 || query.QueryList[0] == nil ||
		query.MetricMerge != "" || len(query.OutputList) != 0 || query.ResponseContract != "" ||
		query.IsESBatch || query.IsSearchAfter || query.From != 0 || query.Limit != 0 || len(query.OrderBy) != 0 ||
		query.Start == "" || query.End == "" {
		return fmt.Errorf("background task summary requires one bounded raw query")
	}
	item := query.QueryList[0]
	if item.DataSource != "bkapm" || string(item.TableID) == "" || item.QueryString != "*" ||
		item.ExactTotal || item.SQL != "" || item.Collapse != nil || item.Scroll != "" ||
		len(item.AggregateMethodList) != 0 || item.From != 0 || item.Limit != 0 || len(item.OrderBy) != 0 {
		return fmt.Errorf("background task summary query fields are invalid")
	}
	return nil
}

func queryTaskSummary(ctx context.Context, query *structured.QueryTs) ([]elasticsearch.TaskSummaryRow, error) {
	if err := validateTaskSummaryQuery(query); err != nil {
		return nil, err
	}
	user := metadata.GetUser(ctx)
	if user.SpaceUID == "" || user.TenantID == "" {
		return nil, fmt.Errorf("background task summary requires authenticated tenant and space")
	}
	query.SpaceUid = user.SpaceUID
	queryRef, err := query.ToQueryReference(ctx)
	if err != nil {
		return nil, err
	}
	routes := queryRef.CollectRouteInfo()
	if queryRef.Count() != 1 || len(routes) != 1 || routes[0].StorageType != metadata.ElasticsearchStorageType ||
		routes[0].DataSource != "bkapm" {
		return nil, fmt.Errorf("background task summary requires one authorized Elasticsearch Span route")
	}
	window := metadata.GetQueryParams(ctx)
	// The caller's millisecond window is rounded outward to seconds by /query/ts/raw.
	if !window.Start.Before(window.End) || window.End.Sub(window.Start) > 24*time.Hour+2*time.Second {
		return nil, fmt.Errorf("background task summary time window exceeds 24 hours")
	}
	var target *metadata.Query
	queryRef.Range("", func(item *metadata.Query) { target = item })
	if target == nil {
		return nil, fmt.Errorf("background task summary route is missing")
	}
	instance := prometheus.GetTsDbInstance(ctx, target)
	reader, ok := instance.(taskSummaryReader)
	if !ok {
		return nil, fmt.Errorf("background task summary Elasticsearch reader is unavailable")
	}
	return reader.QueryTaskSummary(ctx, target, window.Start, window.End)
}
