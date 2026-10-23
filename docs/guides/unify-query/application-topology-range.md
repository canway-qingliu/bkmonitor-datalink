# 租户应用拓扑已观测边有界查询

## 需求要做什么

Kingeye 全景图按选定时间窗直接展示已有的跨应用有向边。合法空窗返回空边；Doris 连接、读目标或行合同失败必须返回错误，不伪装成空图。普通 Trace 查询保持原合同。

## 怎么改

新请求使用 `application_topology_contract_version: "application_topology_range_v2"` 和 `application_topology_request`。范围指纹与 v1 同字段、同 NUL 分隔算法，但前缀改为 `application-topology-range-v2`。UQ 从当前租户已发布的 `application_topology_targets` 中解析 `application_edge`，只查 `kapm_application_edge_observed_1m`；按 `[start,end)`、租户及升序 Namespace/业务允许集汇总有向应用对。回包包含指纹、发布版本、`expected_minutes` 和 `edges`，不包含 `coverage`。查询最多 30 天、129 个应用和 10,000 个有向应用对，超预算或身份不符拒绝。

新租户只发布 `application_edge` 目标。存量成对目标可以继续发布；边和覆盖若同时存在，必须指向同一租户、集群和发布版本。旧 `application_topology_range_v1` 与 `a3_panorama_range_v1` 保持原两表查询与响应，缺少覆盖目标时明确拒绝。新旧字段同时发布时，对重叠目标逐项校验；旧覆盖目标可暂留给旧版请求。既有覆盖表及数据不删除。

## 与之前的区别

此前产品图等覆盖状态达到 `complete`，因此已写入的边也可能不可见。v2 有边就显示；没有边仅表示该窗口没有观测到调用关系，不证明输入完整。运行健康与 checkpoint 等问题在任务监控观察，不再通过 `pending/partial/complete` 表达。

## 验证与发布关注点

聚焦测试覆盖 v2 单边发布、跨租户混合发布、只发一条边的查询、v1 旧覆盖兼容、摘要、版本和身份拒绝。目标环境还须按 UQ、Kingeye、Flinkd 的协议升级顺序验证真实 Doris 有边/空窗、跨业务与跨租户反例、查询延迟和页面呈现。UQ 的 `trace_profile.allowed_bk_app_codes`、成员查询白名单、JWT 与空间锚点仍须正确配置；发布成功不等于目标环境已完成读回。
