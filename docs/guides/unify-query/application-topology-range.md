# 租户应用拓扑已观测边有界查询

## 需求要做什么

Kingeye 全景图按选定时间窗直接展示已有的跨应用有向边。合法空窗返回空边；Doris 连接、读目标或行合同失败必须返回错误，不伪装成空图。普通 Trace 查询保持原合同。

## 怎么改

请求使用 `application_topology_contract_version: "application_topology_range_v2"` 和 `application_topology_request`。范围指纹使用固定的 NUL 分隔字段序列，前缀为 `application-topology-range-v2`。UQ 从当前租户已发布的 `application_topology_targets` 中解析唯一的 `application_edge`，只查 `kapm_application_edge_observed_1m`；按 `[start,end)`、租户及升序 Namespace/业务允许集汇总有向应用对。回包包含指纹、发布版本、`expected_minutes` 和 `edges`，不包含覆盖状态。查询最多 30 天、129 个应用和 10,000 个有向应用对，超预算或身份不符拒绝。

当前应用拓扑发布文档只使用一个 `application_edge` 目标。滚动升级期间，UQ 仍接受旧 Redis 发布字段与旧静态配置字段，但只保留其中的 Span/Summary 读路由；旧应用拓扑目标不会进入 v2 查询。Kingeye 重发单边目标后才开放应用图查询。旧的两表查询请求不再属于当前合同；既有覆盖表及数据不由查询服务删除。

## 与之前的区别

此前产品图等覆盖状态达到 `complete`，因此已写入的边也可能不可见。v2 有边就显示；没有边仅表示该窗口没有观测到调用关系，不证明输入完整。运行健康与 checkpoint 等问题在任务监控观察，不再通过 `pending/partial/complete` 表达。

## 验证与发布关注点

聚焦测试覆盖单边发布、只查已观测边、摘要、版本和身份拒绝。目标环境按 Kingeye、UQ、Flink 作业的协议升级顺序验证真实 Doris 有边/空窗、跨业务与跨租户反例、查询延迟和页面呈现；切换期间先确认 Kingeye 已使用 v2 请求。UQ 的 `trace_profile.allowed_bk_app_codes`、成员查询白名单、JWT 与空间锚点仍须正确配置；发布成功不等于目标环境已完成读回。
