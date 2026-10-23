// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

/*
# TracesDeriver: traces 数据衍生器

processor:
    - name: "traces_deriver/count"
      config:
        operations:
          - type: "count" # count|min|max|delta|bucket|count
            metric_name: "bk_apm_total"
            publish_interval: "10s"
            gc_interval: "1h"
            max_series: 1000
            rules:
              - kind: "SPAN_KIND_SERVER"
                predicate_key: ""
                dimensions:
                  - "resource.bk.instance.id"
                  - "span_name"
                  - "kind"
                  - "status.code"
              - kind: "SPAN_KIND_CLIENT"
                predicate_key: ""
                dimensions:
                  - "resource.bk.instance.id"
                  - "span_name"
                  - "kind"
                  - "status.code"

    - name: "traces_deriver/bucket"
      config:
        operations:
          - type: "bucket"
            metric_name: "bk_apm_duration_bucket"
            publish_interval: "10s"
            gc_interval: "1h"
            buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 5] # buckets 列表
            max_series: 1000
            rules:
              - kind: "SPAN_KIND_SERVER"
                predicate_key: ""
                dimensions:
                  - "resource.bk.instance.id"
                  - "span_name"
                  - "kind"
                  - "status.code"
              - kind: "SPAN_KIND_CLIENT"
                predicate_key: ""
                dimensions:
                  - "resource.bk.instance.id"
                  - "span_name"
                  - "kind"
                  - "status.code"

共享 Metric DataID 的 Span 派生指标在 extractor 和 accumulator 之前，
用已校验 Token 的 bk_tenant_id、bk_biz_id、app_id、bk_app_name 覆盖客户端同名维度。
普通应用仍沿用原有 app_name 维度，不追加租户共享标签。
*/

package tracesderiver
