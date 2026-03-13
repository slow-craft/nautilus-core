# Multi-Protocol Proxy Node Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** New proxy type `multi-protocol` that bundles multiple protocol channels into a single logical node with priority-based failover and shadow dial recovery.

**Architecture:** Implements `ProxyAdapter` interface (not a proxy group). Internal protocol list ordered by priority. Dial failure triggers inline fallback to next protocol. Recovery uses traffic-driven shadow dials instead of timers.

**Tech Stack:** Go, atomic operations for lock-free state, existing `ParseProxy` for protocol parsing.

---

## Design

### Motivation

Users have multiple servers in the same region running different protocols (e.g., Host A: trojan + vmess, Host B: hysteria2 + tuic). Current approach requires separate nodes + fallback group, which:

- Exposes implementation details (multiple nodes for one logical location)
- Relies on periodic URL testing for failure detection (slow)
- Doesn't provide real connection-failure-driven switching

### Configuration

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

    # Optional (all have defaults)
    max-failures: 3        # Consecutive failures to mark unavailable (default: 3)
    dial-timeout: 5        # Per-protocol dial timeout in seconds (default: 5)
    probe-rate: 0.1        # Shadow dial probability when degraded (default: 10%)
    min-probe-rate: 0.02   # Minimum probe rate after repeated failures (default: 2%)
```

All existing protocol types are supported. Each protocol entry uses the same config format as a standalone proxy of that type (minus `name`).

Usage in proxy groups — it's just a regular node:

```yaml
proxy-groups:
  - name: "Auto Select"
    type: url-test
    proxies: ["Hong Kong", "Japan", "US"]
```

### Core Structs

```go
// adapter/outbound/multi_protocol.go

type MultiProtocolOption struct {
    BasicOption
    Name         string           `proxy:"name"`
    Protocols    []map[string]any `proxy:"protocols"`
    MaxFailures  int              `proxy:"max-failures,omitempty"`  // default: 3
    DialTimeout  int              `proxy:"dial-timeout,omitempty"`  // default: 5 (seconds)
    ProbeRate    float64          `proxy:"probe-rate,omitempty"`    // default: 0.1
    MinProbeRate float64          `proxy:"min-probe-rate,omitempty"` // default: 0.02
}

type MultiProtocol struct {
    *Base
    protocols    []*protocolState
    activeIndex  atomic.Int32
    maxFailures  int32
    dialTimeout  time.Duration
    probeRate    float64
    minProbeRate float64
}

type protocolState struct {
    proxy         C.ProxyAdapter
    alive         atomic.Bool
    failCount     atomic.Int32   // consecutive dial failures
    shadowFails   atomic.Int32   // shadow dial failures (for adaptive rate decay)
    recoveryCount atomic.Int32   // consecutive shadow dial successes (need 2 to recover)
    probing       atomic.Bool    // guard against concurrent shadow dials
}
```

### Failure Counting

**Consecutive failure model** — `failCount` tracks consecutive dial failures per protocol:

- Every dial error: `failCount += 1`
- Every dial success: `failCount = 0` (reset immediately)
- When `failCount >= max-failures`: mark protocol as unavailable

```
Example (max-failures=3):
  Request 1: fail  → failCount=1
  Request 2: ok    → failCount=0  ← reset
  Request 3: fail  → failCount=1  ← restart counting
  Request 4: fail  → failCount=2
  Request 5: fail  → failCount=3  ← marked unavailable, switch to next
```

**All errors count equally** (timeout, connection refused, TLS failure, etc.). The rationale: at the dial/handshake stage, any error means the protocol channel is not working. Upstream target errors happen after a successful proxy connection, so they won't trigger this counter. Future refinement can add error-type filtering if needed.

### Dial Flow (TCP & UDP)

```
DialContext(ctx, metadata) / ListenPacketContext(ctx, metadata):
  1. startIdx = activeIndex.Load()
  2. for i = startIdx; i < len(protocols); i++:
       if !protocols[i].alive.Load():
           continue
       dialCtx = context.WithTimeout(ctx, dialTimeout)
       conn, err = protocols[i].proxy.DialContext(dialCtx, metadata)
       if err == nil:
           protocols[i].failCount.Store(0)   // success resets counter
           if i > 0:  // degraded — trigger shadow dial
               triggerShadowDial(metadata)
           return conn, nil
       else:
           newFails = protocols[i].failCount.Add(1)  // consecutive failure
           if newFails >= maxFailures:
               protocols[i].alive.Store(false)
               updateActiveIndex()
           // continue to next protocol (inline failover, same request)
  3. // all protocols failed — reset all to alive for next attempt
     resetAll()
     return nil, lastError
```

### Shadow Dial (Recovery Probing)

Triggered **only when degraded** (active protocol is not the highest priority). Driven by actual traffic, not timers.

**Recovery requires consecutive successes (N=2):** A single shadow dial success is not enough to mark a protocol as recovered. The protocol must succeed **2 consecutive** shadow dials before being marked alive. This filters out unstable protocols that intermittently succeed.

- Shadow dial success: `recoveryCount += 1`
- Shadow dial failure: `recoveryCount = 0`, `shadowFails += 1` (probe rate decays)
- `recoveryCount >= 2`: mark alive, reset all counters, update activeIndex

```
Example — unstable protocol:
  Shadow 1: ok    → recoveryCount=1
  Shadow 2: fail  → recoveryCount=0, shadowFails=1 (probe rate drops)
  Shadow 3: ok    → recoveryCount=1
  Shadow 4: fail  → recoveryCount=0, shadowFails=2 (probe rate drops more)
  → Never reaches 2 consecutive, never cuts back. Probe rate decays naturally.

Example — truly recovered protocol:
  Shadow 1: ok    → recoveryCount=1
  Shadow 2: ok    → recoveryCount=2 → marked alive! Cut back to this protocol.
```

```
triggerShadowDial(metadata):
  activeIdx = activeIndex.Load()
  if activeIdx == 0:
      return  // already on highest priority

  for i = 0; i < activeIdx; i++:
      if protocols[i].alive.Load():
          continue  // already recovered
      if protocols[i].probing.Load():
          continue  // already probing
      rate = adaptiveRate(protocols[i])
      if rand.Float64() > rate:
          continue  // skip this time

      protocols[i].probing.Store(true)
      go func(idx int):
          defer protocols[idx].probing.Store(false)
          ctx = context.WithTimeout(context.Background(), dialTimeout)
          conn, err = protocols[idx].proxy.DialContext(ctx, probeMetadata)
          if err == nil:
              conn.Close()
              newCount = protocols[idx].recoveryCount.Add(1)
              if newCount >= recoveryThreshold:  // default: 2
                  protocols[idx].alive.Store(true)
                  protocols[idx].failCount.Store(0)
                  protocols[idx].shadowFails.Store(0)
                  protocols[idx].recoveryCount.Store(0)
                  updateActiveIndex()
          else:
              protocols[idx].recoveryCount.Store(0)
              protocols[idx].shadowFails.Add(1)
      (i)
```

**Adaptive probe rate:**

```
adaptiveRate(state):
    fails = state.shadowFails.Load()
    rate = probeRate * (0.5 ^ fails)   // halve rate each shadow failure
    return max(rate, minProbeRate)

Example (probeRate=0.1, minProbeRate=0.02):
  0 shadow fails → 10%
  1 shadow fail  → 5%
  2 shadow fails → 2.5%
  3+ shadow fails → 2% (clamped to min)
```

### State Machine

```
Protocol States: Alive / Unavailable

Node State:
  ┌─ Normal: activeIndex == 0 (highest priority)
  │   → Dial success: stay
  │   → Dial fails max-failures times: mark unavailable, degrade to next
  │
  └─ Degraded: activeIndex > 0
      → Main path: dial current active protocol (no blocking)
      → Shadow dial: probabilistically probe higher priority protocols
        → Success: mark recovered, updateActiveIndex()
        → Failure: reduce probe rate, silent
      → Current protocol also fails: degrade further
      → All protocols fail: reset all, return error
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
    m.activeIndex.Store(0)
}

func (m *MultiProtocol) resetAll() {
    for _, p := range m.protocols {
        p.alive.Store(true)
        p.failCount.Store(0)
    }
    m.activeIndex.Store(0)
}
```

### ProxyAdapter Interface Implementation

| Method | Behavior |
|--------|----------|
| `Name()` | Via embedded `*Base` — returns node name |
| `Type()` | Via embedded `*Base` — returns `C.MultiProtocol` |
| `Addr()` | Returns active protocol's `Addr()` |
| `SupportUDP()` | `true` if any protocol supports UDP |
| `SupportUOT()` | `true` if active protocol supports UOT |
| `IsL3Protocol()` | Delegates to active protocol |
| `DialContext()` | Priority-based dial with inline fallback |
| `ListenPacketContext()` | Same logic as DialContext for UDP |
| `Unwrap()` | Returns nil (opaque node, not a group) |
| `MarshalJSON()` | Include active protocol name + all protocol states |
| `Close()` | Close all protocol instances |

### Shadow Dial Probe Target

Shadow dials use a hardcoded lightweight probe address instead of the user's actual request metadata:

- **TCP**: `1.1.1.1:443` — verifies proxy channel reachability
- **UDP**: `1.1.1.1:53` — verifies UDP channel reachability

This avoids wasting a real connection to the user's target. The probe only validates that the proxy channel (handshake + tunnel) works. Not configurable — `1.1.1.1` (Cloudflare) is globally reachable and reliable.

### Config Parsing

In `adapter/parser.go`, add case `"multi-protocol"`:

```go
case "multi-protocol":
    mpOption := &outbound.MultiProtocolOption{BasicOption: basicOption}
    err = decoder.Decode(mapping, mpOption)
    if err != nil {
        break
    }
    proxy, err = outbound.NewMultiProtocol(*mpOption)
```

Inside `NewMultiProtocol`, parse each protocol entry by calling a new internal
`parseProtocolProxy(mapping map[string]any, basicOption BasicOption) (ProxyAdapter, error)`
that reuses the same switch/decode logic as `ParseProxy` but returns `ProxyAdapter`
instead of `C.Proxy` (avoids the autoclose/wrapper layers for internal sub-proxies).

### Connection Handling

- **New connections only**: switching affects new dials; existing connections untouched
- **Existing connections**: naturally expire/break, then reconnect through current active
- **UDP**: same logic via `ListenPacketContext`; QUIC handshake failure = protocol unavailable
- **smux**: each sub-protocol handles its own smux config; multi-protocol layer is transparent

### Edge Cases

- **All protocols unavailable**: reset all to alive, return error; next request retries from top
- **Only one protocol**: behaves exactly like a regular proxy, zero overhead
- **Context deadline**: if upstream context expires mid-retry, stop trying remaining protocols
- **Concurrent shadow dials**: `probing` atomic bool prevents duplicate shadow dials per protocol

---

## Implementation Tasks

### Task 1: Add MultiProtocol adapter type constant

**Files:**
- Modify: `constant/adapters.go`

**Step 1: Add constant**

In the adapter type `iota` block, after `TrustTunnel`, add:

```go
	TrustTunnel
	MultiProtocol
```

**Step 2: Add String() case**

In the `String()` method, before `case Relay:`, add:

```go
	case MultiProtocol:
		return "MultiProtocol"
```

**Step 3: Verify it compiles**

Run: `go build ./constant/...`
Expected: success

**Step 4: Commit**

```bash
git add constant/adapters.go
git commit -m "feat(multi-protocol): add MultiProtocol adapter type constant"
```

---

### Task 2: Implement core MultiProtocol outbound

**Files:**
- Create: `adapter/outbound/multi_protocol.go`

**Step 1: Create the file with full implementation**

```go
package outbound

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/structure"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

const (
	defaultMaxFailures  = 3
	defaultDialTimeout  = 5
	defaultProbeRate    = 0.1
	defaultMinProbeRate = 0.02
)

type MultiProtocolOption struct {
	BasicOption
	Name         string           `proxy:"name"`
	Protocols    []map[string]any `proxy:"protocols"`
	MaxFailures  int              `proxy:"max-failures,omitempty"`
	DialTimeout  int              `proxy:"dial-timeout,omitempty"`
	ProbeRate    float64          `proxy:"probe-rate,omitempty"`
	MinProbeRate float64          `proxy:"min-probe-rate,omitempty"`
}

type protocolState struct {
	proxy         ProxyAdapter
	alive         atomic.Bool
	failCount     atomic.Int32   // consecutive dial failures
	shadowFails   atomic.Int32   // shadow dial failures (for adaptive rate decay)
	recoveryCount atomic.Int32   // consecutive shadow dial successes (need 2 to recover)
	probing       atomic.Bool    // guard against concurrent shadow dials
}

const recoveryThreshold int32 = 2 // consecutive shadow successes needed to recover

type MultiProtocol struct {
	*Base
	protocols    []*protocolState
	activeIndex  atomic.Int32
	maxFailures  int32
	dialTimeout  time.Duration
	probeRate    float64
	minProbeRate float64
}

func NewMultiProtocol(option MultiProtocolOption) (*MultiProtocol, error) {
	if len(option.Protocols) == 0 {
		return nil, fmt.Errorf("multi-protocol: at least one protocol required")
	}

	maxFailures := option.MaxFailures
	if maxFailures <= 0 {
		maxFailures = defaultMaxFailures
	}
	dialTimeout := option.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultDialTimeout
	}
	probeRate := option.ProbeRate
	if probeRate <= 0 {
		probeRate = defaultProbeRate
	}
	minProbeRate := option.MinProbeRate
	if minProbeRate <= 0 {
		minProbeRate = defaultMinProbeRate
	}

	decoder := structure.NewDecoder(structure.Option{
		TagName: "proxy", WeaklyTypedInput: true,
		KeyReplacer: structure.DefaultKeyReplacer,
	})

	protocols := make([]*protocolState, 0, len(option.Protocols))
	hasUDP := false

	for i, protoMap := range option.Protocols {
		proxy, err := parseProtocolProxy(decoder, protoMap, option.BasicOption)
		if err != nil {
			return nil, fmt.Errorf("multi-protocol: protocol[%d]: %w", i, err)
		}
		state := &protocolState{proxy: proxy}
		state.alive.Store(true)
		protocols = append(protocols, state)
		if proxy.SupportUDP() {
			hasUDP = true
		}
	}

	mp := &MultiProtocol{
		Base: NewBase(BaseOption{
			Name:        option.Name,
			Addr:        protocols[0].proxy.Addr(),
			Type:        C.MultiProtocol,
			UDP:         hasUDP,
			Interface:   option.Interface,
			RoutingMark: option.RoutingMark,
			Prefer:      option.IPVersion,
		}),
		protocols:    protocols,
		maxFailures:  int32(maxFailures),
		dialTimeout:  time.Duration(dialTimeout) * time.Second,
		probeRate:    probeRate,
		minProbeRate: minProbeRate,
	}
	return mp, nil
}

func (m *MultiProtocol) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	var lastErr error
	startIdx := int(m.activeIndex.Load())

	for i := startIdx; i < len(m.protocols); i++ {
		p := m.protocols[i]
		if !p.alive.Load() {
			continue
		}

		dialCtx, cancel := context.WithTimeout(ctx, m.dialTimeout)
		conn, err := p.proxy.DialContext(dialCtx, metadata)
		cancel()

		if err == nil {
			p.failCount.Store(0)
			if i > 0 {
				m.triggerShadowDial(false)
			}
			return conn, nil
		}

		lastErr = err
		newFails := p.failCount.Add(1)
		if newFails >= m.maxFailures {
			p.alive.Store(false)
			log.Warnln("[MultiProtocol] %s: protocol %s marked unavailable after %d failures",
				m.Name(), p.proxy.Name(), newFails)
			m.updateActiveIndex()
		}

		if ctx.Err() != nil {
			break
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("multi-protocol %s: all protocols unavailable", m.Name())
	}
	m.resetAllIfAllDead()
	return nil, lastErr
}

func (m *MultiProtocol) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	var lastErr error
	startIdx := int(m.activeIndex.Load())

	for i := startIdx; i < len(m.protocols); i++ {
		p := m.protocols[i]
		if !p.alive.Load() || !p.proxy.SupportUDP() {
			continue
		}

		dialCtx, cancel := context.WithTimeout(ctx, m.dialTimeout)
		pc, err := p.proxy.ListenPacketContext(dialCtx, metadata)
		cancel()

		if err == nil {
			p.failCount.Store(0)
			if i > 0 {
				m.triggerShadowDial(true)
			}
			return pc, nil
		}

		lastErr = err
		newFails := p.failCount.Add(1)
		if newFails >= m.maxFailures {
			p.alive.Store(false)
			log.Warnln("[MultiProtocol] %s: protocol %s marked unavailable after %d failures",
				m.Name(), p.proxy.Name(), newFails)
			m.updateActiveIndex()
		}

		if ctx.Err() != nil {
			break
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("multi-protocol %s: no UDP-capable protocol available", m.Name())
	}
	m.resetAllIfAllDead()
	return nil, lastErr
}

func (m *MultiProtocol) Addr() string {
	idx := m.activeIndex.Load()
	if int(idx) < len(m.protocols) {
		return m.protocols[idx].proxy.Addr()
	}
	return m.protocols[0].proxy.Addr()
}

func (m *MultiProtocol) SupportUOT() bool {
	idx := m.activeIndex.Load()
	if int(idx) < len(m.protocols) {
		return m.protocols[idx].proxy.SupportUOT()
	}
	return false
}

func (m *MultiProtocol) IsL3Protocol(metadata *C.Metadata) bool {
	idx := m.activeIndex.Load()
	if int(idx) < len(m.protocols) {
		return m.protocols[idx].proxy.IsL3Protocol(metadata)
	}
	return false
}

func (m *MultiProtocol) MarshalJSON() ([]byte, error) {
	activeIdx := m.activeIndex.Load()
	protoStates := make([]map[string]any, 0, len(m.protocols))
	for i, p := range m.protocols {
		protoStates = append(protoStates, map[string]any{
			"type":        p.proxy.Type().String(),
			"addr":        p.proxy.Addr(),
			"alive":       p.alive.Load(),
			"fail_count":  p.failCount.Load(),
			"active":      int32(i) == activeIdx,
		})
	}
	return json.Marshal(map[string]any{
		"type":            m.Type().String(),
		"id":              m.Id(),
		"active_protocol": m.protocols[activeIdx].proxy.Type().String(),
		"protocols":       protoStates,
	})
}

func (m *MultiProtocol) Close() error {
	for _, p := range m.protocols {
		_ = p.proxy.Close()
	}
	return nil
}

// --- internal ---

func (m *MultiProtocol) updateActiveIndex() {
	for i, p := range m.protocols {
		if p.alive.Load() {
			m.activeIndex.Store(int32(i))
			return
		}
	}
	m.activeIndex.Store(0)
}

func (m *MultiProtocol) resetAllIfAllDead() {
	for _, p := range m.protocols {
		if p.alive.Load() {
			return
		}
	}
	log.Warnln("[MultiProtocol] %s: all protocols dead, resetting", m.Name())
	for _, p := range m.protocols {
		p.alive.Store(true)
		p.failCount.Store(0)
	}
	m.activeIndex.Store(0)
}

// newProbeMetadata creates a lightweight metadata for shadow dial probing.
// Uses 1.1.1.1:443 (TCP) or 1.1.1.1:53 (UDP) — only verifies the proxy
// channel is reachable, not the actual target.
func newProbeMetadata(udp bool) *C.Metadata {
	meta := &C.Metadata{
		DstIP:   netip.MustParseAddr("1.1.1.1"),
	}
	if udp {
		meta.DstPort = 53
		meta.NetWork = C.UDP
	} else {
		meta.DstPort = 443
		meta.NetWork = C.TCP
	}
	return meta
}

func (m *MultiProtocol) triggerShadowDial(udp bool) {
	activeIdx := int(m.activeIndex.Load())
	if activeIdx == 0 {
		return
	}

	probeMeta := newProbeMetadata(udp)

	for i := 0; i < activeIdx; i++ {
		p := m.protocols[i]
		if p.alive.Load() || p.probing.Load() {
			continue
		}
		if udp && !p.proxy.SupportUDP() {
			continue
		}

		rate := m.adaptiveRate(p)
		if rand.Float64() > rate {
			continue
		}

		if !p.probing.CompareAndSwap(false, true) {
			continue
		}

		go func(idx int, ps *protocolState) {
			defer ps.probing.Store(false)

			ctx, cancel := context.WithTimeout(context.Background(), m.dialTimeout)
			defer cancel()

			var err error
			if udp {
				pc, e := ps.proxy.ListenPacketContext(ctx, probeMeta)
				if e == nil {
					_ = pc.Close()
				}
				err = e
			} else {
				conn, e := ps.proxy.DialContext(ctx, probeMeta)
				if e == nil {
					_ = conn.Close()
				}
				err = e
			}

			if err == nil {
				newCount := ps.recoveryCount.Add(1)
				if newCount >= recoveryThreshold {
					ps.alive.Store(true)
					ps.failCount.Store(0)
					ps.shadowFails.Store(0)
					ps.recoveryCount.Store(0)
					m.updateActiveIndex()
					log.Infoln("[MultiProtocol] %s: protocol %s recovered via shadow dial",
						m.Name(), ps.proxy.Name())
				}
			} else {
				ps.recoveryCount.Store(0)
				ps.shadowFails.Add(1)
			}
		}(i, p)
	}
}

func (m *MultiProtocol) adaptiveRate(p *protocolState) float64 {
	fails := float64(p.shadowFails.Load())
	rate := m.probeRate * math.Pow(0.5, fails)
	if rate < m.minProbeRate {
		return m.minProbeRate
	}
	return rate
}

// parseProtocolProxy creates a ProxyAdapter from a protocol config map.
// Reuses the same decode logic as ParseProxy but returns the raw ProxyAdapter.
func parseProtocolProxy(decoder *structure.Decoder, mapping map[string]any, basicOption BasicOption) (ProxyAdapter, error) {
	proxyType, ok := mapping["type"].(string)
	if !ok {
		return nil, fmt.Errorf("missing type")
	}

	var (
		proxy ProxyAdapter
		err   error
	)

	switch proxyType {
	case "ss":
		opt := &ShadowSocksOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewShadowSocks(*opt)
		}
	case "ssr":
		opt := &ShadowSocksROption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewShadowSocksR(*opt)
		}
	case "socks5":
		opt := &Socks5Option{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewSocks5(*opt)
		}
	case "http":
		opt := &HttpOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewHttp(*opt)
		}
	case "vmess":
		opt := &VmessOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewVmess(*opt)
		}
	case "vless":
		opt := &VlessOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewVless(*opt)
		}
	case "snell":
		opt := &SnellOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewSnell(*opt)
		}
	case "trojan":
		opt := &TrojanOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewTrojan(*opt)
		}
	case "hysteria":
		opt := &HysteriaOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewHysteria(*opt)
		}
	case "hysteria2":
		opt := &Hysteria2Option{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewHysteria2(*opt)
		}
	case "wireguard":
		opt := &WireGuardOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewWireGuard(*opt)
		}
	case "tuic":
		opt := &TuicOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewTuic(*opt)
		}
	case "ssh":
		opt := &SshOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewSsh(*opt)
		}
	case "mieru":
		opt := &MieruOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewMieru(*opt)
		}
	case "anytls":
		opt := &AnyTLSOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewAnyTLS(*opt)
		}
	case "sudoku":
		opt := &SudokuOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewSudoku(*opt)
		}
	case "masque":
		opt := &MasqueOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewMasque(*opt)
		}
	case "trusttunnel":
		opt := &TrustTunnelOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewTrustTunnel(*opt)
		}
	default:
		return nil, fmt.Errorf("unsupported protocol type in multi-protocol: %s", proxyType)
	}

	return proxy, err
}
```

**Step 2: Verify it compiles**

Run: `go build ./adapter/outbound/...`
Expected: success

**Step 3: Commit**

```bash
git add adapter/outbound/multi_protocol.go
git commit -m "feat(multi-protocol): implement core MultiProtocol outbound adapter"
```

---

### Task 3: Add parser case in adapter/parser.go

**Files:**
- Modify: `adapter/parser.go`

**Step 1: Add multi-protocol case**

Before the `default:` case, add:

```go
	case "multi-protocol":
		mpOption := &outbound.MultiProtocolOption{BasicOption: basicOption}
		err = decoder.Decode(mapping, mpOption)
		if err != nil {
			break
		}
		proxy, err = outbound.NewMultiProtocol(*mpOption)
```

**Step 2: Verify it compiles**

Run: `go build ./adapter/...`
Expected: success

**Step 3: Commit**

```bash
git add adapter/parser.go
git commit -m "feat(multi-protocol): add config parsing support"
```

---

### Task 4: Add config example in docs

**Files:**
- Modify: `docs/config.yaml`

**Step 1: Add config example**

After the last proxy example (before `# dns`), add:

```yaml
  # multi-protocol
  # Bundles multiple protocols into a single logical node with priority-based failover.
  # Protocols are ordered by priority (first = highest). On dial failure, automatically
  # falls back to the next protocol. Uses shadow dials to recover higher-priority protocols.
  - name: multi-protocol-example
    type: multi-protocol
    protocols:
      - type: hysteria2
        server: 1.2.3.4
        port: 443
        password: password
      - type: trojan
        server: 1.2.3.4
        port: 8443
        password: password
        sni: example.com
    # max-failures: 3       # consecutive failures to mark protocol unavailable (default: 3)
    # dial-timeout: 5       # per-protocol dial timeout in seconds (default: 5)
    # probe-rate: 0.1       # shadow dial probability when degraded (default: 0.1)
    # min-probe-rate: 0.02  # minimum probe rate (default: 0.02)
```

**Step 2: Commit**

```bash
git add docs/config.yaml
git commit -m "docs: add multi-protocol config example"
```

---

### Task 5: Write unit tests

**Files:**
- Create: `adapter/outbound/multi_protocol_test.go`

**Step 1: Write tests**

Test cases to cover:
1. **Normal dial**: highest priority protocol works → returns connection
2. **Inline failover**: first protocol fails → falls back to second
3. **Max failures**: protocol marked unavailable after N failures
4. **Active index update**: after marking protocol dead, activeIndex advances
5. **Reset all on all dead**: when all protocols fail, reset to alive
6. **Shadow dial trigger**: only triggered when degraded (activeIndex > 0)
7. **Adaptive probe rate**: rate decreases with shadow failures
8. **UDP dial**: ListenPacketContext follows same fallback logic
9. **Single protocol**: behaves like regular proxy
10. **Context cancellation**: stops retrying when context done

Use mock ProxyAdapter implementations for testing.

**Step 2: Run tests**

Run: `go test ./adapter/outbound/ -run TestMultiProtocol -v`
Expected: all pass

**Step 3: Commit**

```bash
git add adapter/outbound/multi_protocol_test.go
git commit -m "test(multi-protocol): add unit tests"
```

---

### Task 6: Integration verification

**Step 1: Build full binary**

Run: `make darwin-arm64` (or appropriate target)
Expected: success

**Step 2: Test with config file**

Create a test config with `multi-protocol` node and verify it parses correctly.

**Step 3: Commit any fixes**

```bash
git commit -m "fix(multi-protocol): integration fixes"
```
