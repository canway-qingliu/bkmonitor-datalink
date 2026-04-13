---
description: "BK-Collector: Go data pipeline framework with plugin-based architecture. Use this for receiver/processor/exporter/controller development, Go conventions, and config-driven design patterns."
keywords: ["Go", "Go concurrency", "Interface design", "Error handling", "Config-driven architecture"]
applyTo: "**/*.go"
---

# BK-Collector Workspace Instructions

**Project**: BK-Collector — Tencent BlueKing APM data collection and processing framework  
**Language**: Go 1.18+  
**Architecture**: Plugin-based, config-driven data pipeline  

---

## Quick Start Commands

```bash
make install    # Install dev tools (gofumpt, goimports-reviser)
make test       # Run tests (enforces 75% coverage)
make lint       # Auto-fix fmt + gofumpt + import organization
make dev        # Run locally: go run ./cmd/collector -c ./example/example.yml
make build      # Build release package (Linux x86_64)
make bin        # Build binary only to dist/bk-collector
```

**Build Tags**: `JSON_LIB=jsonsonic` (default for fast JSON; also supports stdlib)

**Health Check**: `curl http://localhost:9200/api/v1/ping`

---

## Architecture Overview

```
Input (10+ protocols)
    ↓ [Receiver] (HTTP/gRPC/Tars servers)
    ↓ publishRecord() → global RecordQueue
    ↓ [Pipeline] orchestrates Processors
    ↓ [Exporter] converts → GSE format
    ↓ Output
```

**Key Components**:
- **`receiver/`** — Protocol handlers: OTLP, Jaeger, Skywalking, Prometheus, Pyroscope, Zipkin, etc.
- **`processor/`** — Plugin processors (20+): apdexcalculator, sampler, ratelimiter, tracesderiver, etc.
- **`exporter/`** — Output conversion & batching (GSE target)
- **`pipeline/`** — Pipeline manager + processor orchestration (PreCheck → Sched → Export)
- **`controller/`** — Master orchestrator (start/reload/stop lifecycle)
- **`confengine/`** — Config wrapper over elastic/beats library
- **`define/`** — Shared types (Record, RecordType, Events, Errors)

---

## Go Conventions

### 1. Happy Path Coding
Structure code so the successful path flows straight down. Handle errors immediately.

```go
// Correct: happy path flows down
func (r *Receiver) Start(ctx context.Context) error {
    srv, err := r.createServer()
    if err != nil {
        return fmt.Errorf("create server: %w", err)
    }
    
    if err := srv.Start(); err != nil {
        return fmt.Errorf("start server: %w", err)
    }
    
    return nil
}

// Wrong: main logic nested inside conditions
func (r *Receiver) Start(ctx context.Context) error {
    srv, err := r.createServer()
    if err == nil {
        if err := srv.Start(); err == nil {
            return nil
        }
    }
    return err
}
```

### 2. Error Wrapping & Handling
- **Always wrap** errors with `fmt.Errorf("context: %w", err)` for stack traces
- Use **sentinel errors** from `define/errors.go` for recoverable failures:

```go
var ErrUnknownRecordType = errors.New("bk-collector: unknown record type")

// In code:
if recordType == unknown {
    return nil, ErrUnknownRecordType
}

// Check at call site:
if errors.Is(err, ErrUnknownRecordType) {
    // handle specific case
}
```

- **Never panic** in production code path; return error instead.

### 3. Small, Focused Interfaces
Interfaces should be minimal and cohesive. Avoid large interfaces.

```go
// Good: Receiver interface is simple and focused
type Receiver interface {
    Name() string
    Start(ctx context.Context) error
    Stop() error
    Reload(config map[string]any) error
}

// Good: Processor interface (minimal but extensible)
type Processor interface {
    Name() string
    IsDerived() bool
    IsPreCheck() bool
    Process(originalRecord *define.Record) (derivedRecord *define.Record, err error)
    Reload(config map[string]any, customized []SubConfigProcessor)
    MainConfig() map[string]any
    Clean()
}
```

### 4. Function Receiver Naming
Use short, concise receiver names: `r` for receiver, `p` for processor, `e` for exporter, `c` for controller.

```go
func (r *Receiver) Start() error { ... }
func (p *Processor) Process(record *define.Record) (*define.Record, error) { ... }
func (e *Exporter) Push(record *define.Record) error { ... }
```

### 5. Godoc Style Comments
Public functions/types must have comments starting with the receiver name/type name.

```go
// Processor processes telemetry records through a chain of handlers.
type Processor interface {
    // Name returns the processor's unique identifier.
    Name() string
    
    // Process applies transformations and returns a processed record or error.
    Process(r *define.Record) (*define.Record, error)
}

// Start initializes the receiver and begins accepting connections.
func (r *Receiver) Start(ctx context.Context) error {
    ...
}
```

### 6. Use `any` Instead of `interface{}`
Migrate all `interface{}` to `any` (Go 1.18+).

```sh
gofmt -r 'interface{} -> any' -w .
```

### 7. Concurrency Patterns
- **Channels orchestrate** across goroutines; mutexes serialize access to shared data.
- Don't share memory for communication; share communication channels.
- Always buffer channels appropriately or use `select` with timeouts.

```go
// Correct: channels orchestrate
select {
case record := <-r.recordChan:
    process(record)
case <-ctx.Done():
    return ctx.Err()
}

// Global queues (already thread-safe):
globalRecords.Push(record)  // Publisher
record := <-globalRecords.Get()  // Subscriber
```

### 8. Config-Driven Design
Avoid hardcoded values. Load from config; allow hot-reload.

```go
type ReceiverConfig struct {
    Enabled  bool   `config:"enabled"`
    Endpoint string `config:"endpoint"`  // e.g., "0.0.0.0:4317"
}

// Reload support (tested separately):
func (r *Receiver) Reload(cfg map[string]any) error {
    newCfg := &ReceiverConfig{}
    if err := confengine.Unmarshal(cfg, newCfg); err != nil {
        return fmt.Errorf("unmarshal receiver config: %w", err)
    }
    // Update internal state safely
    return nil
}
```

---

## Project-Specific Patterns

### 1. Plugin Registration (Static, No Dynamic Loading)
All plugins are compiled in via blank imports in `controller/register.go`:

```go
// controller/register.go
package controller

import (
    _ "github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/processor/ratelimiter"
    _ "github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/receiver/otlp"
)
```

When adding a new processor/receiver, add its blank import to register.go.

### 2. Processor Types
Processors are categorized by execution phase:
- **PreCheck** — Runs first (validation, token check, rate limit)
- **Sched** — Main processing (sampling, aggregation, derivation)
- **Derived** — Can create new Records, not just mutate

```go
func (p *Processor) IsPreCheck() bool { return true }  // Phase identifier
func (p *Processor) IsDerived() bool { return false }
```

### 3. Global Queues for Communication
Records flow through global, thread-safe queues:

```go
// In main coordinator:
for record := range globalRecords.Get() {
    for _, proc := range pipeline {
        derived, err := proc.Process(record)
        if err != nil {
            // handle error
        }
        if derived != nil {
            globalRecords.Push(derived)  // FanOut: create new records
        }
    }
}
```

### 4. Config Structure (YAML)
Example config in `example/example.yml`:

```yaml
receiver:
  http_server:
    enabled: true
    endpoint: "0.0.0.0:4317"
    path: "/otel/v1/traces"

processor:
  - name: "tokenchecker"
    config:
      token: "my-token"
  - name: "apdexcalculator"
    config:
      threshold: 100

exporter:
  type: "gse"
  config:
    endpoint: "gse.example.com"
```

---

## Testing Requirements

**Coverage Threshold**: **75% enforced** by CI (`make test`).

```bash
# Run tests with coverage report
make test

# Check coverage for specific file
go tool cover -func=coverage.out | grep processor/
```

**Testing Best Practices**:
- Mock interfaces directly (no mocking library needed):

```go
type mockProcessor struct{}
func (mockProcessor) Name() string { return "mock" }
func (mockProcessor) Process(r *define.Record) (*define.Record, error) {
    return r, nil
}

// In test:
ps.processors = append(ps.processors, mockProcessor{})
```

- Use fixtures in `example/fixtures/` for configs and test data.
- Assertion style: `github.com/stretchr/testify/assert`

```go
assert.NoError(t, err)
assert.Equal(t, expected, actual)
```

- **Test reload paths**: Each component's `Reload()` must have separate tests.
- **Don't skip coverage for generated code**: `/gen/`, `_gen.go`, `/pb/` are auto-excluded.

---

## Directory Reference

| Path | Purpose |
|------|---------|
| `cmd/collector/` | Entry point; uses beat framework |
| `receiver/` | Protocol handlers (OTLP, Jaeger, Prometheus, etc.) |
| `processor/` | Plugin processors (apdex, sampler, ratelimiter, etc.) |
| `exporter/` | Output conversion to GSE format |
| `pipeline/` | Pipeline orchestration & processor chains |
| `controller/` | Master controller + plugin registration |
| `confengine/` | Config management (wrapper over elastic/beats) |
| `define/` | Shared types & sentinel errors |
| `internal/` | Utilities (JSON, labels, TLS middleware, k8s cache, etc.) |
| `example/` | YAML configs, test fixtures |

---

## Common Pitfalls

| Issue | Solution |
|-------|----------|
| **Coverage drops below 75%** | Add tests; use `go tool cover` to identify gaps |
| **Config reload panics** | Test `Reload()` with invalid configs; use safe type assertions |
| **Memory leaks in hot-reload** | Clean up old resources in `Clean()` before replacing |
| **Goroutine leaks** | Always cancel contexts, close channels cleanly |
| **Type assertion fails on JSON unmarshal** | JSON numbers are `float64`, not `int`; check with comma-ok: `if v, ok := m["key"]; ok { ... }` |
| **Plugin not registering** | Forgot blank import in `controller/register.go` |
| **TLS cert not found at runtime** | Verify file path matches `support-files/` structure |
| **Global queue not thread-safe** | Don't create custom queue; use provided `define.RecordQueue` |

---

## Related Resources

- **README**: [./README.md](./README.md) — Architecture diagrams, protocol support matrix
- **Makefile**: [./Makefile](./Makefile) — Build targets and ldflags
- **golang-style skill**: Follow Go proverbs, happy path coding, error wrapping patterns
- **Example configs**: `/example/` — YAML templates for receivers, processors, exporters
- **API docs**: `api.md` — gRPC/HTTP endpoint specs

---

## When to Load the golang-style Skill

This workspace automatically applies Go conventions above. **Load the `golang-style` skill explicitly** if you need detailed guidance on:
- Advanced error wrapping and stack trace handling
- Godoc comment generation for public APIs
- Go proverbs applied to specific scenarios
- Interface design critique

Invoke via slash command: `/golang-style`

---

**Last Updated**: 2026-03-25  
**Questions?** Review architecture in README, check controller/register.go for plugin patterns, or run `make help`.
