# 租户应用拓扑边与覆盖有界查询

## 需求与范围

Kingeye KAPM 的跨应用全景图通过 `query_ts_raw` 读取租户级应用边分钟事实及零边覆盖。新功能按业务语义称为“应用拓扑范围查询”，不再把 TR3/T3.3 阶段代号 `A3` 当成功能名。UQ 仅使用当前租户已发布的成对 Doris 目标，最多查询 30 天、129 个升序应用和 10,000 个有向应用对；超预算、目标漂移或版本不一致均拒绝。普通 Trace 查询保持原有合同。

## 新查询协议

请求顶层使用 `application_topology_contract_version: "application_topology_range_v1"` 和 `application_topology_request`。请求体包含 `expected_scope_fingerprint`、`observation_domain_id`、`expected_route_version`、分钟对齐的 `start_time_ms/end_time_ms`、升序 `applications[{namespace_id,bk_biz_id}]` 与 `max_pairs`。租户来自 UQ 已验证身份，不能由请求指定；请求也不能指定物理库、表或集群。

范围摘要将 `application-topology-range-v1`、租户、观测域、发布版本、起止时间、行预算，以及各应用的 `namespace_id:bk_biz_id` 依次用 NUL 分隔，再计算 SHA-256 小写十六进制。成功回包以同名 `application_topology_contract_version` 回显版本，同时返回范围摘要、发布版本、`expected_minutes`、有向 `edges` 和逐源应用 `coverage`。KAPM 对当前租户 Namespace/业务身份再次复核，并按完整分钟数判定 `pending/partial/complete`；有边不代表覆盖完整，无边但覆盖完整才是真实空图。

部署时，`trace_profile.allowed_bk_app_codes` 与 `trace_profile.member_lookup_allowed_bk_app_codes` 都须包含实际已验证的 Kingeye `bk_app_code`，并核对 JWT 公钥、`jwt.bk_app_code_spaces` 空间授权、成对发布目标与 Doris 只读连接。入站 `space_uid` 必须对应请求应用集合中的一个业务；它是既有入口路由锚点，不扩大租户授权。缺少任一条件时按拒绝或未就绪处理，不能解释为空图。

## 发布目标与兼容过渡

新发布字段是 `application_topology_targets`，标签为 `application_edge` 和 `application_coverage`，分别对应 `kapm_application_edge_observed_1m` 与 `kapm_application_edge_coverage_1m`。物理表和 30 天保留期不变。既有 `a3_targets`、`a3_edge`、`a3_coverage` 是历史发布协议，旧分钟查询仍使用它们。UQ 过渡期接受旧字段单独存在、新字段单独存在或两者同时存在；双字段必须指向完全一致的租户、库表、集群、Schema、保留期和发布版本，否则整份发布文档失败。旧 `a3_panorama_range_v1` 查询信封及原摘要算法也暂时可读，并返回旧版响应键。

**部署顺序：**先部署能同时解析新旧字段的 UQ，再在 Kingeye 显式开启新发布字段；开启后保持旧字段双写，以兼容现有分钟查询。旧版 UQ 会拒绝未知的 `application_topology_targets`，因此新字段不能在 UQ 升级前默认发布。Kingeye 新全景调用方发送新查询协议；移除旧发布字段和旧查询别名须在所有消费者迁移且数据面验收后另行执行。

## 查询与验证

覆盖和边汇总各执行一条固定形态的参数化只读 SQL。边汇总与同分钟覆盖按租户、日期、分钟及源应用身份连接，拒绝晚于 complete 可见水位的边。零边分钟的 complete 允许可见水位为空，但要求输入 fence；若该分钟后来出现边且水位为空，则拒绝该边。边查询以 `LIMIT max_pairs` 截止，返回数触及预算时保守拒绝。UQ 对返回行的双端 Namespace/业务身份再次校验。

本地测试覆盖新旧查询协议、摘要、发布目标单字段与双字段一致性、版本及预算拒绝、参数化 SQL、跨业务行拒绝、覆盖与空图区分。仍须在 Test 使用真实 Doris 表验证 JOIN/聚合语法、30 天查询延迟、只读连接行上限、跨租户拒绝、Flink checkpoint 可见性和零边分钟覆盖；本地通过不等于目标数据面可用。
