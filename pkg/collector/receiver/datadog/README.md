# Datadog Browser SDK Receiver

这是一个用于接收 Datadog Browser SDK 数据的 receiver 实现。

## 概述

此 receiver 接收来自 Datadog Browser SDK 的 Real User Monitoring (RUM) 数据，并将其转换为内部日志记录格式进行处理。

## 支持的数据格式

### V1 格式
- 端点: `/api/v2/rum`
- 支持单个 JSON 对象或 JSON 数组
- 包含基本的 RUM 事件数据

### V2 格式 (推荐)
- 端点: `/api/v2/rum/events`
- 支持 JSON Lines 格式 (每行一个 JSON 对象)
- 支持更详细的事件上下文和元数据

## 配置示例

在 `bk-collector` 的配置文件中添加以下配置：

```yaml
receiver:
  http_server:
    enabled: true
    endpoint: "0.0.0.0:4319"
    middlewares:
      - "cors"
      - "accesslog"
```

## Datadog Browser SDK 端点配置

在 Datadog Browser SDK 中配置以下选项：

### JavaScript 配置示例

```javascript
import { datadogRum } from '@datadog/browser-rum';

datadogRum.init({
  applicationId: 'YOUR_APP_ID',
  clientToken: 'YOUR_CLIENT_TOKEN',
  site: 'datadoghq.com',
  service: 'your-service-name',
  env: 'production',
  sessionSampleRate: 100,
  sessionReplaySampleRate: 100,
  rumEndpoint: 'http://your-collector-address:4319',
  defaultPrivacyLevel: 'mask-user-input',
});

datadogRum.startSessionReplayRecording();
```

## 数据流转

1. Datadog Browser SDK 发送 RUM 数据到收集器
2. Receiver 接收并解析数据
3. 数据转换为内部日志记录格式
4. 经过 Pipeline 验证和处理
5. 发送到配置的后端存储

## 支持的事件类型

- **视图事件**: 页面加载和导航
- **用户操作事件**: 点击、输入等交互
- **性能事件**: 资源加载时间、长任务等
- **错误事件**: JavaScript 错误、资源错误等
- **日志事件**: 应用日志消息

## 认证

支持通过以下方式进行认证：

1. **Token 头**: `X-BK-TOKEN` - 在 HTTP 请求头中传递
2. **查询参数**: `token` - 在 URL 查询参数中传递
3. **User-Agent 头**: 提取 token 信息

## 错误处理

- 无效的内容类型返回 `400 Bad Request`
- 缺少认证信息返回相应错误
- 服务器错误返回 `500 Internal Server Error`
- 成功处理返回 `200 OK`

## 性能考虑

1. **批量处理**: 支持一次发送多条事件
2. **异步处理**: 事件异步发送到 Pipeline
3. **背压处理**: 使用带缓冲的队列管理事件

## 故障排除

### 收不到数据
- 检查收集器是否正在运行
- 验证 Datadog SDK 配置中的端点地址是否正确
- 检查防火墙规则是否允许该端口的访问

### 数据被拒绝
- 验证认证 token 是否正确
- 检查日志中的预检查 (pre-check) 失败原因
- 确认数据格式是否正确

### 性能问题
- 检查数据处理管道的配置
- 监控收集器的 CPU 和内存使用情况
- 调整 `max_procs` 配置以允许更多的并发处理

## 相关资源

- [Datadog 官方文档](https://docs.datadoghq.com/)
- [Browser RUM 文档](https://docs.datadoghq.com/real_user_monitoring/browser/)
- [本项目其他 Receiver 实现](../)
