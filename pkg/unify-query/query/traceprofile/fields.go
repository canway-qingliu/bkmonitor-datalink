// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package traceprofile

import "github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"

// Complete physical row contracts stay independent of the storage transport.
var spanProjection = []string{
	"bk_tenant_id", "observation_domain_id", "app_id", "span_date", "trace_id", "span_id",
	"space_uid", "bk_biz_id", "data_id", "app_name", "parent_span_id", "trace_state",
	"span_name", "span_kind", "status_code", "status_message", "start_time_us", "end_time_us",
	"duration_us", "event_time_ms", "service_namespace", "service_name", "service_instance_id",
	"deployment_environment", "resource", "attributes", "events", "links", "ingest_time",
}

var summaryProjection = []string{
	"bk_tenant_id", "observation_domain_id", "trace_date", "app_id", "trace_id", "bk_biz_id",
	"app_name", "trace_start_time_us", "trace_end_time_us", "duration_us", "span_count",
	"error_count", "service_count", "hierarchy_count", "span_min_duration_us", "span_max_duration_us",
	"event_time_ms", "root_service_name", "root_service_span_id", "root_service_span_name",
	"root_service_kind", "root_service_category", "root_service_status_code", "root_span_id",
	"root_span_name", "root_span_service_name", "root_span_kind", "category_statistics",
	"kind_statistics", "partial", "computed_at_ms",
}

func projectionForProfile(profile metadata.TraceProfile) []string {
	if profile == metadata.TraceProfileSpan {
		return spanProjection
	}
	return summaryProjection
}
