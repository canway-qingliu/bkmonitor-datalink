# Doris 时间聚合使用事件时间

Doris 时间桶统一使用查询路由的 `TimeField.Name`（缺省 `dtEventTimeStamp`），字段值必须为 epoch 毫秒。固定窗口计算为：

```text
floor((event_ms - offset_ms) / window_ms) * window_ms + offset_ms
```

SQL 使用 `CAST(FLOOR(...) AS BIGINT)`，先向下取整再转整数；保留现有时区偏移与结果补零契约。TSpider 保留无整数 CAST、按完整表达式分组及 `MAX` 投影的兼容路径。

`__shard_key__` 是写入分片键：前缀由 `shard_minutes` 决定，后缀为写入实例随机编号。它不是固定的分钟时间戳，无法恢复较粗分片中的细粒度事件桶。旧整分钟分支将此前缀固定按分钟解码，小时分片会产生错误时间桶；结果转换按请求时间网格补零后表现为全零。

日志窗口计数的调用方应使用 `sum + count_over_time`，对应已有 SQL `COUNT` 下推；本次不扩展下推白名单，不将外层 `count` 的 series 计数语义改为日志条数。无聚合查询、用户 SQL、字段元数据及存储分片键用途保持原契约。

验证分开覆盖 SQL 生成与结果转换：生成测试检查窗口、自定义字段和偏移；自包含真实结果快照由独立逐桶计数校验，再通过生产 Formatter、结构化表达式和 PromQL 引擎回放。回放不执行 SQL，也不替代部署镜像的端到端验证。

回放快照采集于 2026-10-09，预期由独立逐桶 `COUNT` 查询核对；数据已脱敏，不包含环境表名或访问凭据。多表生成测试覆盖传入多 DB 及字段映射时的 QueryFactory 行为，不模拟元数据网络请求。
