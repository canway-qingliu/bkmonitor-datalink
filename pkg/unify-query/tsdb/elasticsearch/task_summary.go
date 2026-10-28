// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	elastic "github.com/olivere/elastic/v7"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

const taskSummaryMaxBuckets = 5000
const taskSummaryCandidates = "(attributes.celery.action:run OR attributes.scheduling.system:* OR attributes.job.system:* OR " +
	"span_name:SpringScheduled* OR span_name:SpringScheduling* OR span_name:quartz-scheduler* OR span_name:Quartz* OR " +
	"span_name:ElasticJob* OR span_name:xxl-job* OR span_name:APScheduler* OR " +
	"attributes.plugind.action_uid:* OR attributes.plugind.resource_uid:*)"

// taskSummaryKeyScript mirrors Kingeye's task-entry classifier over ES doc values.
// The length-prefixed key keeps service, type, and task name unambiguous.
const taskSummaryKeyScript = `
String value(def fields, String name) {
  if (!fields.containsKey(name) || fields[name].size() == 0) return '';
  return fields[name].value.toString();
}
boolean root(String parent) {
  if (parent.length() == 0 || parent == '-1') return true;
  for (int i = 0; i < parent.length(); i++) if (parent.substring(i, i + 1) != '0') return false;
  return true;
}
boolean prefix(String name, String prefix) {
  if (!name.startsWith(prefix)) return false;
  if (name.length() == prefix.length()) return true;
  String delimiter = name.substring(prefix.length(), prefix.length() + 1);
  return delimiter == '/' || delimiter == '.' || delimiter == ':' || delimiter == ' ';
}
int kind = doc.containsKey('kind') && doc['kind'].size() != 0 ? ((Number)doc['kind'].value).intValue() : 0;
if (kind == 2 || kind == 3 || kind == 4) return null;
String name = value(doc, 'span_name');
String type = '';
String task = '';
if (kind == 1 && root(value(doc, 'parent_span_id')) && value(doc, 'attributes.plugind.action_uid').trim().length() > 0) {
  type = 'async'; task = name.trim().length() > 0 ? name.trim() : 'plugind.action';
} else if (kind == 1 && root(value(doc, 'parent_span_id')) && value(doc, 'attributes.plugind.resource_uid').trim().length() > 0) {
  type = 'async'; task = name.trim().length() > 0 ? name.trim() : 'plugind.resource';
} else if ('run'.equalsIgnoreCase(value(doc, 'attributes.celery.action'))) {
  type = 'async'; String t = value(doc, 'attributes.celery.task_name'); task = t.trim().length() > 0 ? t.trim() : name.trim();
} else if (kind == 5) {
  return null;
} else if ('celery' == value(doc, 'attributes.scheduling.system') && 'tick' == value(doc, 'attributes.scheduling.operation')) {
  return null;
} else {
  String system = value(doc, 'attributes.scheduling.system');
  if (system.length() == 0) system = value(doc, 'attributes.job.system');
  system = system.toLowerCase();
  String lower = name.toLowerCase();
  boolean aps = system == 'apscheduler' || prefix(lower, 'apscheduler');
  boolean scheduled = aps;
  if (!scheduled) {
    for (String p : new String[]{'springscheduled', 'springscheduling', 'quartz-scheduler', 'quartz', 'elasticjob', 'xxl-job'}) {
      if (prefix(lower, p)) { scheduled = true; break; }
    }
    if (system == 'spring' || system == 'spring-scheduling' || system == 'spring_scheduling' || system == 'quartz' || system == 'elasticjob' || system == 'xxl-job') scheduled = true;
  }
  if (!scheduled) return null;
  type = 'scheduled'; String t = value(doc, 'attributes.job.name'); task = t.trim().length() > 0 ? t.trim() : name.trim();
}
if (task.length() == 0) return null;
String service = value(doc, 'resource.service.name');
return service.length() + ":" + service + type.length() + ":" + type + task.length() + ":" + task;
`

type TaskSummaryRow struct {
	ServiceName    string  `json:"service_name"`
	TaskType       string  `json:"task_type"`
	TaskName       string  `json:"task_name"`
	ExecutionCount int64   `json:"execution_count"`
	FailureCount   int64   `json:"failure_count"`
	AvgDurationMs  float64 `json:"avg_duration_ms"`
	MaxDurationMs  float64 `json:"max_duration_ms"`
	LastCalledMs   int64   `json:"last_called_ms"`
	LatestTraceID  string  `json:"latest_trace_id"`
}

func taskSummaryAggregation() *elastic.TermsAggregation {
	latest := elastic.NewTopHitsAggregation().Size(1).
		Sort("start_time", false).Sort("trace_id", false).
		FetchSourceContext(elastic.NewFetchSourceContext(true).Include("trace_id", "start_time"))
	return elastic.NewTermsAggregation().
		Script(elastic.NewScript(taskSummaryKeyScript).Lang("painless")).
		Size(taskSummaryMaxBuckets+1).ShardSize(taskSummaryMaxBuckets+1).
		ShowTermDocCountError(true).OrderByCountDesc().OrderByKeyAsc().
		SubAggregation("duration_sum", elastic.NewSumAggregation().Field("elapsed_time")).
		SubAggregation("duration_max", elastic.NewMaxAggregation().Field("elapsed_time")).
		SubAggregation("failed", elastic.NewFilterAggregation().Filter(elastic.NewTermQuery("status.code", 2))).
		SubAggregation("latest", latest)
}

func (i *Instance) QueryTaskSummary(
	ctx context.Context, query *metadata.Query, start, end time.Time,
) ([]TaskSummaryRow, error) {
	if query == nil || query.StorageType != metadata.ElasticsearchStorageType || !start.Before(end) {
		return nil, fmt.Errorf("task summary requires one bounded Elasticsearch route")
	}
	// Keep UQ's resolved route and structured scope/time predicates. The raw
	// query-string translator can omit valid OR matches; ES parses the fixed
	// candidate expression natively before the task-classification script.
	scoped := *query
	scoped.QueryString = ""
	prepared, err := i.PrepareRawQuery(ctx, &scoped, start, end, nil)
	if err != nil {
		return nil, err
	}
	filter := elastic.NewBoolQuery().Filter(prepared.countQuery, elastic.NewQueryStringQuery(taskSummaryCandidates))
	prepared.source.Query(filter)
	prepared.countQuery = filter
	prepared.source.Size(0).Timeout("20s").Aggregation("tasks", taskSummaryAggregation())
	body, err := marshalSearchSource(prepared.source)
	if err != nil {
		return nil, err
	}
	result, err := i.executeESQuery(ctx, prepared.queryOption, prepared.fact, prepared.source, prepared.countQuery, body)
	if err != nil {
		return nil, err
	}
	return parseTaskSummaryResult(result)
}

func parseTaskSummaryResult(result *elastic.SearchResult) ([]TaskSummaryRow, error) {
	if result == nil || result.TimedOut || result.TerminatedEarly || result.Shards == nil || result.Shards.Failed != 0 ||
		result.Shards.Successful != result.Shards.Total {
		return nil, fmt.Errorf("task summary Elasticsearch response is incomplete")
	}
	buckets, ok := result.Aggregations.Terms("tasks")
	if !ok || buckets == nil || buckets.SumOfOtherDocCount != 0 || buckets.DocCountErrorUpperBound != 0 ||
		len(buckets.Buckets) > taskSummaryMaxBuckets {
		return nil, fmt.Errorf("task summary aggregation buckets are incomplete or over budget")
	}
	rows := make([]TaskSummaryRow, 0, len(buckets.Buckets))
	for _, bucket := range buckets.Buckets {
		key, ok := bucket.Key.(string)
		if !ok || bucket.DocCount <= 0 {
			return nil, fmt.Errorf("task summary bucket identity is invalid")
		}
		service, kind, name, err := parseTaskSummaryKey(key)
		if err != nil || service == "" || name == "" || (kind != "async" && kind != "scheduled") {
			return nil, fmt.Errorf("task summary bucket identity is invalid")
		}
		sum, sumOK := bucket.Aggregations.Sum("duration_sum")
		maximum, maxOK := bucket.Aggregations.Max("duration_max")
		failed, failedOK := bucket.Aggregations.Filter("failed")
		latest, latestOK := bucket.Aggregations.TopHits("latest")
		if !sumOK || !maxOK || !failedOK || !latestOK || sum.Value == nil || maximum.Value == nil ||
			latest.Hits == nil || len(latest.Hits.Hits) != 1 || failed.DocCount > bucket.DocCount ||
			!validDuration(*sum.Value) || !validDuration(*maximum.Value) {
			return nil, fmt.Errorf("task summary bucket metrics are invalid")
		}
		var hit struct {
			TraceID   string `json:"trace_id"`
			StartTime int64  `json:"start_time"`
		}
		if err := json.Unmarshal(latest.Hits.Hits[0].Source, &hit); err != nil || hit.TraceID == "" || hit.StartTime < 0 {
			return nil, fmt.Errorf("task summary latest Trace is invalid")
		}
		rows = append(rows, TaskSummaryRow{
			ServiceName: service, TaskType: kind, TaskName: name,
			ExecutionCount: bucket.DocCount, FailureCount: failed.DocCount,
			AvgDurationMs: math.RoundToEven(*sum.Value/float64(bucket.DocCount)/10) / 100,
			MaxDurationMs: math.RoundToEven(*maximum.Value/10) / 100,
			LastCalledMs:  hit.StartTime / 1000, LatestTraceID: hit.TraceID,
		})
	}
	return rows, nil
}

func validDuration(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func parseTaskSummaryKey(key string) (string, string, string, error) {
	service, remaining, err := takeTaskKeySegment(key)
	if err != nil {
		return "", "", "", err
	}
	kind, remaining, err := takeTaskKeySegment(remaining)
	if err != nil {
		return "", "", "", err
	}
	name, remaining, err := takeTaskKeySegment(remaining)
	if err != nil || remaining != "" {
		return "", "", "", fmt.Errorf("task summary bucket key is malformed")
	}
	return service, kind, name, nil
}

func takeTaskKeySegment(key string) (string, string, error) {
	colon := strings.IndexByte(key, ':')
	if colon < 1 || colon > 6 {
		return "", "", fmt.Errorf("task summary bucket segment length is invalid")
	}
	length, err := strconv.Atoi(key[:colon])
	if err != nil || length < 0 || length > 65535 {
		return "", "", fmt.Errorf("task summary bucket segment length is invalid")
	}
	remainder := key[colon+1:]
	if length == 0 {
		return "", remainder, nil
	}
	units := 0
	for offset, char := range remainder {
		units++
		if char > 0xffff {
			units++
		}
		if units > length {
			return "", "", fmt.Errorf("task summary bucket segment length is invalid")
		}
		if units == length {
			end := offset + utf8.RuneLen(char)
			return remainder[:end], remainder[end:], nil
		}
	}
	return "", "", fmt.Errorf("task summary bucket segment is incomplete")
}
