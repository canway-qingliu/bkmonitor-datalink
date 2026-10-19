# Profiling 配置说明

profiling 模块负责向客户端下发 CPU / Memory profiling 的采集参数。采集端通过 HTTP 接口按 token 拉取自己生效的配置。

## 1）配置结构

### 主配置

profiling 段写在主配置的 `bk-collector:` 之下，定义所有应用的默认采集参数：

```yaml
bk-collector:
  profiling:
    disabled: false
    cpu:
      enabled: true
      collection_period: "10m"
      collection_window: "30s"
      sampling_frequency: "99Hz"
    memory:
      enabled: true
      collection_period: "15m"
      collection_window: "1m"
      allocation_threshold: "512KB"
```

| 字段                  | 类型            | 说明                                            |
|---------------------|---------------|-------------------------------------------------|
| `disabled`          | bool           | profiling 总开关，缺省为 `true`；`true` 不采集，`false` 允许采集  |
| `cpu.enabled`       | bool           | 是否采集 CPU profiling                             |
| `cpu.collection_period` | string     | 采集周期，如 `"10m"`                               |
| `cpu.collection_window` | string     | 单次采集时长，如 `"30s"`                            |
| `cpu.sampling_frequency` | string    | CPU 采样频率，如 `"99Hz"`、`"1kHz"`                 |
| `memory.enabled` | bool | 是否采集内存 profiling |
| `memory.collection_period` | string | 内存采集周期，如 `"15m"` |
| `memory.collection_window` | string | 单次内存采集时长，如 `"1m"` |
| `memory.allocation_threshold` | string | 内存分配阈值，按分配量触发采集，如 `"512KB"`、`"1MB"` |

CPU / Memory 的 `collection_period`、`collection_window`，以及 CPU 的 `sampling_frequency`、Memory 的 `allocation_threshold` 均按不透明字符串加载并原样下发。YAML 中使用带引号的字符串，HTTP JSON 响应保持相同的字符串值（如 `"5m"`、`"30s"`），不转换为纳秒整数，也不规范化单位写法；参数的解析由客户端负责。CPU 按频率采样，内存按分配量触发，两者分别使用自己的字段。

顶层总开关统一使用 Kingeye 下发的 `disabled`（取值为 `not is_enabled_profiling`），配置和 HTTP JSON 响应使用同一键名及极性。接口始终可用，`disabled` 为 `true` 时照常返回 200 并下发其余参数，客户端据此停止采集。`disabled: false` 时，再由 `cpu.enabled` / `memory.enabled` 分别决定是否采集对应类型。

### 子配置（per-token 覆盖）

针对单个应用（以 token 区分）的个性化参数，写在 subconfig 文件里，命中后**整体替换**主配置的 profiling 段（`disabled` 除外，见下）：

```yaml
type: "subconfig"
token: "Ymtia2JrYmtia2JrYmtiaxUt..."

profiling:
  disabled: false
  cpu:
    enabled: true
    collection_period: "5m"
    collection_window: "10s"
    sampling_frequency: "199Hz"
  memory:
    enabled: false
    collection_period: "30m"
    collection_window: "2m"
    allocation_threshold: "1MB"
```

总开关 `disabled` 是唯一的例外：subconfig 里省略它时继承主配置的取值；显式 `disabled: false` 可覆盖主配置的 `disabled: true`，允许该应用采集。CPU / Memory 参数可以独立下发，例如只有 `disabled: false` 和 `cpu` 参数时，该应用仍可采集 CPU profiling，缺省的 Memory 保持不采集。

subconfig 文件通过主配置的 `apm.patterns` 被发现，与 processor / pipeline 的子配置共用同一套机制：

```yaml
bk-collector:
  apm:
    patterns:
      - "./example/fixtures/report_*.yml"
      - "./example/platform.yml"
      - "./example/subconfig*.yml"
```

## 2）拉取接口

```
GET /profiling/config
```

token 通过 `X-BK-TOKEN` 传递，按 `internal/tokenparser.FromHttpRequest` 的顺序依次读取：

1. query 参数 `X-BK-TOKEN` 或请求头 `X-BK-TOKEN`
2. 请求头 `X-Tps-TenantID` 或 query 参数 `X-Tps-TenantID`

注意 query 参数名是 `X-BK-TOKEN` 这个字面量（`define.KeyToken`），不是 `bk-token`。

未命中 per-token 覆盖时回落到主配置的默认参数。

响应示例：

```json
{
  "disabled": false,
  "cpu": {
    "enabled": true,
    "collection_period": "5m",
    "collection_window": "10s",
    "sampling_frequency": "199Hz"
  },
  "memory": {
    "enabled": false,
    "collection_period": "30m",
    "collection_window": "2m",
    "allocation_threshold": "1MB"
  }
}
```

顶层 `disabled` 即 profiling 总开关。无论它是否为 `true`，接口都返回 200 并下发其余参数，客户端据此判断是否采集。

错误响应（body 为纯文本，非 JSON）：

| 状态码 | body                | 含义                     |
|------|---------------------|------------------------|
| 404  | `no profiling config` | 主配置没有 `profiling` 段，未加载到任何配置   |

## 3）三个容易踩的坑

### 坑一：子配置里的 `profiling` 必须平铺在根层级

主配置和子配置的层级要求**不一样**：

```yaml
# 主配置：profiling 在 bk-collector 段内
bk-collector:
  profiling:
    cpu: ...

# 子配置：profiling 必须在文件根层级
type: subconfig
token: xxx
profiling:
  cpu: ...
```

原因：主配置走 libbeat，libbeat 会把 `bk-collector:` 这一段切出来当 `localConfig` 交给 collector（`libgse/beat/beater.go`），所以 collector 看到的是段内内容；而 subconfig 是 `confengine.LoadConfigPath` 直接读原始 YAML 文件，不做剥壳。

写成 `bk-collector.profiling` 时，`profiling/config.go` 里的 `sc.Has("profiling")` 取不到，该文件会被静默跳过。代码里已加 Warn 日志，命中时会打印：

```
profiling: subconfig for token xxx nests profiling under 'bk-collector',
but it must be at the root level; per-token override skipped
```

### 坑二：`apm.patterns` 的相对路径按进程 CWD 解析

pattern 最终交给 `filepath.Glob`，相对的是 **collector 进程启动时的工作目录**，不是主配置文件所在目录。代码中没有 chdir。

仓库里两个示例主配置引的是同一批文件，写法却不同，正是因为启动目录不同：

```yaml
# example/example.yml —— make dev 启动，CWD = pkg/collector
apm:
  patterns:
    - "./example/fixtures/report_*.yml"

# example/fixtures/main.yml —— 供单测使用，CWD = pkg/collector/pipeline
apm:
  patterns:
    - "../example/fixtures/platform.yml"
```

glob 匹配不到时不报错，只打一条 Warn 后继续：

```
WARN confengine/engine.go:102  no config file found for pattern ./example/subconfig*.yml
```

即 pattern 写错会导致 per-token 覆盖**静默失效**，没有其他错误提示。部署时建议使用绝对路径。

### 坑三：布尔值按 YAML 1.1 解析

配置链路最终走 `gopkg.in/yaml.v2`，即 YAML 1.1 规范。Python 风格的 `True` / `False` 能正常解析为布尔值，但 `yes/no/on/off/y/n` 同样会被解析为布尔值——例如 `enabled: no` 会得到 `false` 而不是报错。

示例配置统一使用小写 `true` / `false`。

## 4）排查清单

per-token 覆盖没生效时，按顺序检查：

1. 主配置有没有 `profiling` 段（整个段缺省时 `Reload` 直接短路，子配置完全不会被读取，接口返回 `no profiling config`）；
2. 启动日志有无 `no config file found for pattern`（坑二）；
3. 启动日志有无 `nests profiling under 'bk-collector'`（坑一）；
4. 启动日志有无 `failed to decode override for token`（配置结构无法解析；三个字符串参数不在 collector 中做格式校验）；
5. 请求的 `X-BK-TOKEN` 是否与子配置的 `token` 字段完全一致。

其中 2-4 均为日志可观测，5 可通过对比 `subconfig` 的 `token` 与请求头确认。

客户端反馈"没在采"但接口能通时，检查响应的顶层 `disabled` 是否为 `true`（主配置未声明时默认为 `true`），以及对应类型的 `cpu.enabled` / `memory.enabled` 是否为 `true`。Kingeye 模板上下文、应用子配置和接口响应的总开关均应使用 `disabled`。
