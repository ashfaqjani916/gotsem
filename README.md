# gotsem

A Redis-backed distributed semaphore for Go. Enforces per-project concurrency limits across multiple processes/pods using atomic Lua scripts and sorted sets.

> **⚠️ Under Development** — This library is not yet production-ready. APIs may change without notice.

---

## How it works

Each acquired slot is stored as a member in a Redis sorted set, scored by its expiry timestamp (Unix ms). On every acquire/release, expired slots are evicted atomically, so crashed pods or disconnected clients don't leak capacity indefinitely.

- **Acquire** — atomic Lua script: evicts expired slots, checks count vs limit, adds the new slot if capacity allows.
- **Release** — atomic Lua script: removes the slot by ID, cleans up any other expired slots as a side effect.
- **Fail-open** — if Redis is unreachable, `TryAcquire` returns `Acquired: true` with a sentinel slot ID so the caller is never blocked by infrastructure downtime.
- **Per-project limits** — optionally supply a `planFn` to resolve a dynamic limit per project ID, with a 1-minute in-process cache.

## Installation

```bash
go get github.com/ashfaqjani916/gotsem
```

Requires a Redis instance (single node, sentinel, or cluster — anything satisfying `redis.UniversalClient`).

## Usage

### Basic setup

```go
import (
    "context"
    "time"

    "github.com/ashfaqjani916/gotsem"
    "github.com/redis/go-redis/v9"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

sem := gotsem.NewGotsem(
    rdb,
    "sem:",        // key prefix — keys will be like "sem:<projectID>"
    30*time.Second, // slot TTL — how long a slot is held if Release is never called
    10,            // default max concurrent slots per project
    nil,           // planFn — pass nil to always use defaultMax
)
```

### Acquire and release a slot

```go
func handleRequest(ctx context.Context, projectID string) error {
    result := sem.TryAcquire(ctx, projectID)
    if !result.Acquired {
        return fmt.Errorf("concurrency limit reached (%d/%d active)", result.Active, result.Max)
    }
    defer sem.Release(ctx, projectID, result.SlotID)

    // ... do the work ...
    return nil
}
```

`Release` detaches from the request context internally, so it fires correctly even when the client has disconnected or the context has timed out.

### Dynamic per-project limits

Supply a `planFn` to look up the limit for a project at runtime (e.g. from a database or billing tier). Results are cached for 1 minute per project ID.

```go
planFn := func(ctx context.Context, projectID string) int {
    // return 0 or negative to fall back to defaultMax
    return db.GetConcurrencyLimit(ctx, projectID)
}

sem := gotsem.NewGotsem(rdb, "sem:", 30*time.Second, 5, planFn)
```

### Observability

```go
active := sem.ActiveCount(ctx, projectID)
fmt.Printf("project %s has %d active slots\n", projectID, active)
```

## API

| Method | Description |
|---|---|
| `NewGotsem(rdb, keyPrefix, slotTTL, defaultMax, planFn)` | Create a new semaphore instance |
| `TryAcquire(ctx, projectID) AcquireResult` | Non-blocking attempt to acquire a slot |
| `Release(ctx, projectID, slotID)` | Release a previously acquired slot |
| `ActiveCount(ctx, projectID) int` | Current number of active slots for a project |

### `AcquireResult`

```go
type AcquireResult struct {
    Acquired bool   // whether the slot was granted
    SlotID   string // opaque ID to pass to Release
    Active   int    // active slot count after this operation
    Max      int    // limit that was applied
}
```

## Requirements

- Go 1.21+
- Redis 7+
- [`github.com/redis/go-redis/v9`](https://github.com/redis/go-redis)
