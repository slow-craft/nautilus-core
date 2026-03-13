# Multi-Protocol Proxy Node Design

## Overview

New proxy type `multi-protocol` that bundles multiple protocol channels into a single logical node. Unlike proxy groups (fallback/url-test), this is a `ProxyAdapter` — it participates in proxy groups as a single node, not a nested group.

## Motivation

Users have multiple servers in the same region running different protocols (e.g., Host A: trojan + vmess, Host B: hysteria2 + tuic). Current approach requires configuring them as separate nodes and using fallback groups, which:

- Exposes implementation details (multiple nodes for one logical location)
- Relies on periodic URL testing for failure detection (slow)
- Doesn't provide real connection-failure-driven switching

## Configuration

```yaml
proxies:
  - name: "Hong Kong"
    type: multi-protocol
    # Ordered by priority (first = highest)
    protocols:
      - type: hysteria2
        server: hk-b.example.com
        port: 443
        password: "xxx"
      - type: tuic
        server: hk-b.example.com
        port: 8443
        uuid: "xxx"
        password: "xxx"
      - type: trojan
        server: hk-a.example.com
        port: 443
        password: "xxx"
      - type: vmess
        server: hk-a.example.com
        port: 8080
        uuid: "xxx"

    # Optional settings
    max-failures: 3        # Consecutive failures to mark unavailable (default: 3)
    dial-timeout: 5        # Per-protocol dial timeout in seconds (default: 5)
    probe-rate: 0.1        # Shadow dial probability when degraded (default: 10%)
    min-probe-rate: 0.02   # Minimum probe rate after repeated failures (default: 2%)
```

All existing protocol types are supported in the `protocols` list. Each protocol entry uses the same configuration format as a standalone proxy of that type (minus `name`).

Usage in proxy groups — it's just a regular node:

```yaml
proxy-groups:
  - name: "Auto Select"
    type: url-test
    proxies: ["Hong Kong", "Japan", "US"]
```

## Architecture

### Core Struct

```go
// adapter/outbound/multi_protocol.go

type MultiProtocol struct {
    *Base
    protocols    []*protocolState  // ordered by priority (index 0 = highest)
    activeIndex  atomic.Int32      // index of current active protocol
    maxFailures  int
    dialTimeout  time.Duration
    probeRate    float64
    minProbeRate float64
    mu           sync.RWMutex
}

type protocolState struct {
    proxy          C.ProxyAdapter  // actual proxy instance (trojan, vmess, etc.)
    alive          atomic.Bool     // whether this protocol is available
    failCount      atomic.Int32    // consecutive dial failure count
    shadowFails    atomic.Int32    // consecutive shadow dial failures (for adaptive rate)
}
```

### Dial Flow

```
DialContext(ctx, metadata):
  1. active = protocols[activeIndex]
  2. conn, err = active.proxy.DialContext(ctx, metadata)
  3. if err == nil:
       active.failCount.Store(0)
       triggerShadowDial(metadata)   // if degraded
       return conn, nil
  4. if err != nil:
       active.failCount.Add(1)
       if active.failCount >= maxFailures:
           active.alive.Store(false)
           advanceToNextAlive()
       // retry with next available protocol (same request)
       goto step 1 with next protocol
  5. all protocols failed: return nil, error
```

Same logic applies to `ListenPacketContext` for UDP.

### Shadow Dial (Recovery Probing)

Triggered **only when degraded** (active protocol is not the highest priority):

```
triggerShadowDial(metadata):
  if activeIndex == 0:
      return  // already on highest priority, no probing needed

  for i := 0; i < activeIndex; i++:
      if protocols[i].alive:
          continue  // already recovered

      rate = adaptiveRate(protocols[i])
      if rand.Float64() > rate:
          continue  // skip this time

      go func():
          ctx, cancel = context.WithTimeout(5s)
          conn, err = protocols[i].proxy.DialContext(ctx, metadata)
          if err == nil:
              conn.Close()
              protocols[i].alive.Store(true)
              protocols[i].failCount.Store(0)
              protocols[i].shadowFails.Store(0)
              // activeIndex will naturally move up on next DialContext
              updateActiveIndex()
          else:
              protocols[i].shadowFails.Add(1)
      ()
```

**Adaptive probe rate:**

```
adaptiveRate(state):
    fails = state.shadowFails.Load()
    rate = probeRate * (0.5 ^ fails)   // halve rate each failure
    return max(rate, minProbeRate)

Example with defaults (probeRate=0.1, minProbeRate=0.02):
  0 shadow fails → 10%
  1 shadow fail  → 5%
  2 shadow fails → 2.5%
  3+ shadow fails → 2% (clamped to min)
```

### State Machine

```
Protocol States: Active / Unavailable

Node State:
  ┌─ Normal: activeIndex == 0 (highest priority protocol)
  │   → Dial success: stay
  │   → Dial fails max-failures times: mark unavailable, degrade
  │
  └─ Degraded: activeIndex > 0
      → Main path: dial current active protocol (no blocking)
      → Shadow dial: probabilistically probe higher priority protocols
        → Shadow success: mark recovered, updateActiveIndex()
        → Shadow failure: reduce probe rate, silent
      → Current protocol also fails: degrade further
      → All protocols unavailable: return error
```

### Active Index Management

```go
func (m *MultiProtocol) updateActiveIndex() {
    for i, p := range m.protocols {
        if p.alive.Load() {
            m.activeIndex.Store(int32(i))
            return
        }
    }
    // all dead — reset to 0 so next dial attempts from highest priority
    m.activeIndex.Store(0)
}
```

### Connection Handling

- **New connections only**: switching affects new dials; existing connections are untouched
- **Existing connections**: naturally expire or break, then reconnect through current active protocol
- **UDP**: same logic via `ListenPacketContext`, QUIC handshake failure = protocol unavailable

### ProxyAdapter Interface

`MultiProtocol` implements `C.ProxyAdapter`:

| Method | Behavior |
|--------|----------|
| `Name()` | Returns the multi-protocol node name |
| `Type()` | New type constant `C.MultiProtocol` |
| `Addr()` | Returns active protocol's addr |
| `SupportUDP()` | `true` if any protocol supports UDP |
| `DialContext()` | Priority-based dial with inline fallback |
| `ListenPacketContext()` | Same as DialContext for UDP |
| `Unwrap()` | Returns current active protocol's proxy |
| `Close()` | Closes all protocol instances and stops probing |

### Config Parsing

In `adapter/parser.go`, add case `"multi-protocol"`:

1. Parse the `protocols` array
2. For each protocol entry, reuse existing parser logic (`ParseProxy()`) to create proxy instances
3. Wrap them in `protocolState`
4. Create `MultiProtocol` instance

### API / Manual Control

Expose via existing RESTful API:

- `GET /proxies/Hong Kong` — shows current active protocol, all protocol states
- `PUT /proxies/Hong Kong` — force reset all protocols to alive (manual recovery trigger)

## Files to Create/Modify

| File | Action |
|------|--------|
| `adapter/outbound/multi_protocol.go` | **Create** — core implementation |
| `constant/adapters.go` | **Modify** — add `MultiProtocol` adapter type |
| `adapter/parser.go` | **Modify** — add parsing case |
| `docs/config.yaml` | **Modify** — add config example |

## Edge Cases

- **All protocols unavailable**: return error, reset activeIndex to 0 so next attempt tries from top
- **Only one protocol configured**: behaves exactly like a regular proxy, no overhead
- **Context deadline**: if upstream context expires mid-retry, stop trying remaining protocols
- **Concurrent shadow dials**: use `sync.Once`-style guard to prevent multiple shadow dials for the same protocol simultaneously
