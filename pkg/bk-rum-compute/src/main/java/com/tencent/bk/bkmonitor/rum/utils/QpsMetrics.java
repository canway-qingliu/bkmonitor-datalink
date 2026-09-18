// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
package com.tencent.bk.bkmonitor.rum.utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

/**
 * 基于 Flink {@link MeterView} 的 QPS 计数器。
 *
 * <p>用法:在算子的 {@code open()} 里调用 {@link #register(RuntimeContext, String)} 完成注册,
 * 之后每条输入调用 {@link #mark(long)}。注册与 mark 均为惰性:未注册时 mark 是空操作,便于单测直接
 * {@code new} 算子而不必搭运行时上下文。
 *
 * <p>指标以 {@code <operatorName>.qps.*} 子组暴露,如 {@code session-precompute.qps.input}。
 * 配合 Prometheus reporter 即得 {@code flink_taskmanager_job_task_operator_<name>_qps_<name>_rate},
 * 配合 log reporter 则在过滤条件中包含 {@code *qps*} 即可。
 */
public class QpsMetrics {
  private final MeterView meter;

  private QpsMetrics(MeterView meter) {
    this.meter = meter;
  }

  /** 在 {@link RuntimeContext} 上注册 QPS 指标;返回的实例在算子内共享,随算子生命周期使用。 */
  public static QpsMetrics register(RuntimeContext context, String metricName) {
    return register(context.getMetricGroup(), metricName);
  }

  /** 在 {@link MetricGroup} 上注册 QPS 指标;供 source 反序列化等只能拿到 MetricGroup 的场景使用。 */
  public static QpsMetrics register(MetricGroup group, String metricName) {
    return new QpsMetrics(group.addGroup("qps").meter(metricName, new MeterView(60)));
  }

  /** 记录 {@code count} 条输入。 */
  public void mark(long count) {
    meter.markEvent(count);
  }

  /** 记录 1 条输入。 */
  public void mark() {
    mark(1L);
  }
}
