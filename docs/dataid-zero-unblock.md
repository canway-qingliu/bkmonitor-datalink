# Collector 输出链路放开 dataid=0 说明文档

## 1. 背景

本次排障中，collector 能正常接收 OTLP 数据，但业务数据未进入 GSE 输出发送阶段。

日志中的典型报错为：

- `publish event failed: dataid 0 <= 0`

这说明 `dataid=0` 的事件在 `ReportCommonData` 之前就被拦截丢弃。

## 2. 根因

collector 的多个输出实现使用了 `dataid <= 0` 作为非法条件，导致 `dataid=0` 被当作无效数据拒绝。

## 3. 改动说明

将校验条件统一从：

- `if dataid <= 0 { ... }`

调整为：

- `if dataid < 0 { ... }`

调整后的行为：

- `dataid=0`：允许发送
- `dataid<0`：继续拒绝

## 4. 涉及文件

- `pkg/libgse/output/gse/gse.go`
  - `PublishEvent`
  - `Report`
  - `ReportRaw`
- `pkg/libgse/output/bkpush/bkpush.go`
  - `publish`
- `pkg/libgse/output/bkpipe_multi/bkpipe_multi.go`
  - `Publish`

## 5. 兼容性与风险评估

- 对现有 `dataid>0` 的链路行为无影响。
- 负值保护仍然保留。
- 若下游系统在业务语义上强依赖 `dataid!=0`，需单独做联调确认。

## 6. 验证清单

1. 启动 mock gse-server 与 collector。
2. 构造并发送一条 `dataid=0` 的事件。
3. 确认 collector 日志不再出现 `dataid 0 <= 0`（或等效拦截）报错。
4. 确认 gse-server 收到业务数据帧，而不只是 type=10 的 agent-info 握手包。
5. 确认发布成功计数增长，且失败/丢弃计数未因 dataid=0 增加。

## 7. 回滚方案

若需要回滚，可将以上三个文件中的条件恢复为 `dataid <= 0`。

## 8. 备注

本次仅放开 `dataid=0`，不改变附加字段逻辑（如 `bizid`、`cloudid`、`ip`、`hostname`、`bk_agent_id`、`bk_host_id`）及发送传输逻辑。
