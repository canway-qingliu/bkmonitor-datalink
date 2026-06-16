# AegisV2 Traces 转换说明

本文描述 `receiver/aegisv2/converter.go` 当前实现的输入格式、字段分层、OTLP Traces 映射规则，以及典型事件的转换示例。

## 1. 输入模型

AegisV2 `/collect` 请求整体是三层结构：

1. `bean`
请求级公共上下文，映射为 OTLP `resource.attributes`。

2. `d2[i].fields`
单条事件上下文，映射为 OTLP `span.attributes`。其中 `fields.action` 会额外生成一个独立的 action span。

3. `d2[i].message[j]`
具体观测值。当前实现里，每个 `message[j]` 都会生成一个 span；部分类型会把 `message` 直接提升为 span attributes，其他类型默认写入 span event attributes。

当前解析兼容两种输入形态：

1. 正常 JSON 对象 / 数组
2. 被序列化成字符串的 JSON

也就是 `fields` 和 `message[j]` 都支持对象或字符串两种形态。

## 2. 总体转换规则

### 2.1 Resource attributes

`bean.*` 会映射到 `resource.attributes`：

| AegisV2 字段 | OTLP 字段 |
| --- | --- |
| `topic` | `aegisv2.topic` |
| `scheme` | `aegisv2.scheme` |
| `bean.version` | `resource.version` |
| `bean.aid` | `resource.aid` |
| `bean.env` | `resource.env` |
| `bean.platform` | `resource.platform` |
| `bean.netType` | `resource.netType` |
| `bean.vp` | `resource.vp` |
| `bean.sr` | `resource.sr` |
| `bean.referer` | `resource.referer` |

### 2.2 Span attributes

`fields.*` 会映射到 `span.attributes`：

| AegisV2 字段 | OTLP 字段 |
| --- | --- |
| `fields.from` | `http.url` |
| `fields.type` | `event.type` |
| `fields.level` | `event.level` |
| `fields.plugin` | `event.plugin` |
| `fields.session.id` | `session.id` |
| `fields.view.id` | `view.id` |
| `fields.view.view_name` | `view.name` |
| `fields.view.loading_type` | `view.loading_type` |
| `fields.view.view_url` | `view.url` |
| `fields.view.referrer` | `view.referrer` |
| `fields.action.id` | `action span -> action.id` |
| `fields.action.timestamp` | `action span -> action.timestamp` |
| `fields.action.action_type` | `action span -> action.type` |
| `fields.action.action_name` | `action span -> action.name` |
| `fields.action.action_target_name` | `action span -> action.target_name` |

### 2.3 Trace / Span 生成规则

1. 每个 `d2[i]` 会生成一个新的 TraceID。
2. 同一个 `d2[i]` 下的所有 `message[j]` 共享同一个 TraceID。
3. 若 `fields.action` 存在，会先生成一个 action span。
4. 每个 `message[j]` 都会生成一个新的 SpanID。

也就是：

1. `d2[0].message[0]` 和 `d2[0].message[1]` 属于同一条 trace。
2. `d2[1]` 会开启另一条 trace。

如果 `fields.action` 存在，则 action span 和该 `d2[i]` 下所有 message spans 属于同一条 trace。

### 2.4 时间戳规则

1. `message.timestamp` 作为 span 结束时间。
2. 优先使用 `message.duration` 反推 span 开始时间。
3. 若没有 `duration`，则对 `page_performance` 使用 `firstScreenTiming` 反推开始时间。
4. 若仍然没有时长，则 `StartTimestamp == EndTimestamp`。

## 3. Standalone Span 分类

以下类型会直接提升为独立 span，不再额外生成 event：

| 事件类型 | 条件 | SpanKind |
| --- | --- | --- |
| `assets_speed` | `fields.type == "assets_speed"` 或 `message.msg == "asset_speed"` | `client` |
| `api` | `fields.type == "api"` | `client` |
| `page_performance` | `fields.type == "page_performance"` 或 `message.msg == "page_performance"` | `internal` |
| `session` | `fields.type == "session"` 或 `message.msg == "session"` | `internal` |
| `action` | `fields.action` 存在 | `internal` |
| 错误类事件 | 见下文错误判定规则 | `internal` |

### 3.1 错误类事件判定规则

只要满足以下任一条件，就会被识别为 error span：

1. `fields.plugin == "error"`
2. `fields.level` 包含 `error`
3. `message.isErr == true`

### 3.2 Error span 附加规则

错误类 span 会额外附加：

1. `span.status.code = error`
2. `error.message`
3. `exception.type`
4. `exception.message`

取值规则：

1. `error.message`
优先取 `message.errorMsg`，没有则回退到 `message.msg`。

2. `exception.type`
优先取 `fields.level`，否则取 `fields.plugin`，再否则取 `message.msg`。

3. `exception.message`
与 `error.message` 一致。

此外，若 `fields.type == "normal"` 且属于错误类事件，则 span name 会优先使用 `fields.level`，例如：

1. `promise_error`
2. `image_error`

而不是保留为 `normal`。

## 4. 非 Standalone 事件规则

如果某类事件不属于上面的 standalone span 分类：

1. 仍然生成 span。
2. `message.*` 不直接放到 span attrs。
3. 而是写入一个名为 `aegisv2.message` 的 span event attributes。

当前已经收敛为 standalone 的类型比较多，因此核心前端观测事件大多已不再走 event 附属模式。

## 5. 转换示例

下面的“输出示例”是逻辑视图，便于理解，不是 exporter 最终落库 JSON 的完整结构。

### 示例 1：API 错误 -> Client Error Span

输入：

```text
Action Span:
  name = action.click
  kind = internal
  status = unset
  start = end = 1780994775565

Action Span attrs:
  http.url = http://127.0.0.1:8080/index.html
  session.id = session-1
  view.loading_type = initial_load
  action.id = action-1
  action.type = click
  action.name = 测试 fetch
  action.target_name = button
  action.source_event_type = api
```

```text

```json
{
  "fields": {
    "from": "http://127.0.0.1:8080/index.html",
    "session": {"id": "session-1"},
    "view": {
      "id": "view-1",
      "loading_type": "initial_load",
      "view_name": "VideoFlow",
      "view_url": "http://127.0.0.1:8080/index.html",
      "referrer": ""
    },
    "action": {
      "id": "action-1",
      "timestamp": 1780994775565,
      "action_type": "click",
      "action_name": "测试 fetch",
      "action_target_name": "button"
    },
    "type": "api",
    "level": "error",
    "plugin": "api"
  },
  "message": [
    {
      "duration": 182.3,
      "msg": "url: https://example.com/api",
      "url": "https://example.com/api",
      "status": 200,
      "method": "GET",
      "isErr": true,
      "requestType": "fetch",
      "aegisv2_goto": "goto-1",
      "timestamp": 1780992434065
    }
  ]
}
```

输出：

```text
Span:
  name = api
  kind = client
  status = error
  start = timestamp - 182.3ms
  end = 1780992434065

Span attrs:
  http.url = http://127.0.0.1:8080/index.html
  event.type = api
  event.level = error
  event.plugin = api
  session.id = session-1
  view.loading_type = initial_load

  message.url = https://example.com/api
  message.status = 200
  message.method = GET
  message.duration = 182.3
  message.requestType = fetch
  message.aegisv2_goto = goto-1

  error.message = url: https://example.com/api
  exception.type = error
  exception.message = url: https://example.com/api
```

### 示例 1.1：Action -> Internal Span

输入：

```json
{
  "fields": {
    "from": "http://127.0.0.1:8080/index.html",
    "session": {"id": "session-1"},
    "view": {
      "id": "view-1",
      "loading_type": "initial_load",
      "view_name": "VideoFlow",
      "view_url": "http://127.0.0.1:8080/index.html",
      "referrer": ""
    },
    "action": {
      "id": "action-1",
      "timestamp": 1780994775565,
      "action_type": "click",
      "action_name": "测试 fetch",
      "action_target_name": "button"
    },
    "type": "api",
    "level": "error",
    "plugin": "api"
  }
}
```

输出：

```text
Span:
  name = action.click
  kind = internal
  status = unset
  start = end = 1780994775565

Span attrs:
  http.url = http://127.0.0.1:8080/index.html
  session.id = session-1
  view.loading_type = initial_load

  action.id = action-1
  action.timestamp = 1780994775565
  action.type = click
  action.name = 测试 fetch
  action.target_name = button
  action.source_event_type = api
```

### 示例 2：Assets Speed -> Browser Resource Span

输入：

```json
{
  "fields": {
    "from": "http://127.0.0.1:8080/index.html",
    "type": "assets_speed",
    "level": "info",
    "plugin": "assetSpeed"
  },
  "message": [
    {
      "msg": "asset_speed",
      "url": "http://127.0.0.1:8080/styles.css",
      "status": 200,
      "assetType": "css",
      "duration": 47.3,
      "tlsTime": 139.4,
      "timestamp": 1781593481995
    }
  ]
}
```

输出：

```text
Span:
  name = browser.resource
  kind = client
  status = unset
  start = timestamp - 47.3ms
  end = 1781593481995

Span attrs:
  event.type = assets_speed
  event.plugin = assetSpeed
  span_type = resource
  span_subtype = link
  result = success
  error_type = none
  duration_bucket = <100ms
  event_label = 静态资源
  http.response.status_code = 200
  status_class = 2xx
  url.full = http://127.0.0.1:8080/styles.css
  target_domain = 127.0.0.1:8080
  target_path_template = /styles.css
  target_label = 127.0.0.1:8080/styles.css
  rum.page.host = 127.0.0.1:8080
  rum.page.path = /index.html

  message.url = http://127.0.0.1:8080/styles.css
  message.status = 200
  message.assetType = css
  message.duration = 47.3
  message.tlsTime = 139.4
```

### 示例 3：Page Performance -> documentLoad Span

输入：

```json
{
  "fields": {
    "from": "http://127.0.0.1:8080/index.html",
    "type": "page_performance",
    "level": "info",
    "plugin": "pagePerformance",
    "view": {
      "id": "view-1",
      "loading_type": "initial_load",
      "view_name": "VideoFlow",
      "view_url": "http://127.0.0.1:8080/index.html",
      "referrer": ""
    }
  },
  "message": [
    {
      "msg": "page_performance",
      "dnsLookup": 0,
      "tcp": 0,
      "ssl": 0,
      "ttfb": 2,
      "contentDownload": 1,
      "domParse": 209,
      "resourceDownload": 88,
      "firstScreenTiming": 429,
      "timestamp": 1780992436877
    }
  ]
}
```

输出：

```text
Span:
  name = documentLoad
  kind = internal
  status = unset
  start = timestamp - 429ms
  end = 1780992436877

Span attrs:
  event.type = page_performance
  event.plugin = pagePerformance
  span_type = document
  span_subtype = navigate
  result = success
  error_type = none
  event_label = 文档加载
  trace_scene = page_load
  url.full = http://127.0.0.1:8080/index.html
  rum.page.host = 127.0.0.1:8080
  rum.page.path = /index.html
  target_label = /index.html
  view.loading_type = initial_load

  message.firstScreenTiming = 429
  message.ttfb = 2
  message.domParse = 209
  message.resourceDownload = 88

Span events:
  fetchStart
  domInteractive
  domContentLoadedEventStart
  domContentLoadedEventEnd
  domComplete
  loadEventStart
  loadEventEnd
  firstPaint
  firstContentfulPaint
```

### 示例 4：Session -> Internal Point Span

输入：

```json
{
  "fields": {
    "from": "http://127.0.0.1:8080/index.html",
    "session": {
      "id": "ec763eeb7207bc0b49bd29b4b8a6cab7"
    },
    "type": "session",
    "level": "info",
    "plugin": "session"
  },
  "message": [
    {
      "session_type": "session",
      "is_active": true,
      "session_from": "local_generate",
      "msg": "session",
      "aegisv2_goto": "8bd4a5ea56801604",
      "timestamp": 1780992433875
    }
  ]
}
```

输出：

```text
Span:
  name = session
  kind = internal
  status = unset
  start = end = 1780992433875

Span attrs:
  session.id = ec763eeb7207bc0b49bd29b4b8a6cab7
  event.type = session
  event.plugin = session

  message.session_type = session
  message.is_active = true
  message.session_from = local_generate
  message.aegisv2_goto = 8bd4a5ea56801604
```

### 示例 5：Promise Error -> Internal Error Span

输入：

```json
{
  "fields": {
    "from": "http://127.0.0.1:8080/index.html",
    "type": "normal",
    "level": "promise_error",
    "plugin": "error"
  },
  "message": [
    {
      "msg": "PROMISE_ERROR: Error.message: func sseError not found",
      "errorMsg": "func sseError not found",
      "aegisv2_goto": "8a75c2d530087cfe",
      "timestamp": 1780993960686
    }
  ]
}
```

输出：

```text
Span:
  name = promise_error
  kind = internal
  status = error
  start = end = 1780993960686

Span attrs:
  event.type = normal
  event.level = promise_error
  event.plugin = error

  message.msg = PROMISE_ERROR: Error.message: func sseError not found
  message.aegisv2_goto = 8a75c2d530087cfe

  error.message = func sseError not found
  exception.type = promise_error
  exception.message = func sseError not found
```

## 6. 当前实现边界

当前 traces 转换已经覆盖并稳定处理的类型：

1. `api`
2. `assets_speed`
3. `page_performance`
4. `session`
5. `action`
6. 错误类事件，包括：
   - `promise_error`
   - `image_error`
   - `normal error`
   - `isErr=true` 的 API 类事件

当前 metrics 和 logs 仍未实现，相关入口会返回未实现错误。

## 7. 代码参考

1. 主要转换实现：`receiver/aegisv2/converter.go`
2. 转换示例测试：`receiver/aegisv2/converter_test.go`