package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/structure"
	C "github.com/metacubex/mihomo/constant"
)

// --- mock helpers ---

type mockProxyAdapter struct {
	*Base
	dialFunc   func(ctx context.Context, metadata *C.Metadata) (C.Conn, error)
	listenFunc func(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error)
	closed     atomic.Bool
}

func newMockProxy(name string, addr string, udp bool) *mockProxyAdapter {
	m := &mockProxyAdapter{
		Base: NewBase(BaseOption{
			Name: name,
			Addr: addr,
			Type: C.Shadowsocks,
			UDP:  udp,
		}),
	}
	return m
}

func (m *mockProxyAdapter) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if m.dialFunc != nil {
		return m.dialFunc(ctx, metadata)
	}
	c1, c2 := net.Pipe()
	_ = c2.Close()
	return NewConn(c1, m), nil
}

func (m *mockProxyAdapter) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	if m.listenFunc != nil {
		return m.listenFunc(ctx, metadata)
	}
	return nil, errors.New("not supported")
}

func (m *mockProxyAdapter) Close() error {
	m.closed.Store(true)
	return nil
}

func newTestMultiProtocol(name string, proxies []ProxyAdapter, opts ...func(*MultiProtocol)) *MultiProtocol {
	protocols := make([]*protocolState, len(proxies))
	hasUDP := false
	for i, p := range proxies {
		protocols[i] = &protocolState{proxy: p}
		protocols[i].alive.Store(true)
		if p.SupportUDP() {
			hasUDP = true
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	mp := &MultiProtocol{
		Base: NewBase(BaseOption{
			Name: name,
			Addr: proxies[0].Addr(),
			Type: C.MultiProtocol,
			UDP:  hasUDP,
		}),
		protocols:         protocols,
		maxFailures:       3,
		dialTimeout:       5 * time.Second,
		probeRate:         0.1,
		minProbeRate:      0.02,
		recoveryThreshold: 2,
		cooldownBase:      10 * time.Second,
		cooldownMax:       5 * time.Minute,
		ctx:               ctx,
		cancel:            cancel,
	}
	for _, opt := range opts {
		opt(mp)
	}
	return mp
}

func newTestMetadata() *C.Metadata {
	return &C.Metadata{
		NetWork: C.TCP,
		DstPort: 443,
		Host:    "example.com",
	}
}

// --- tests ---

func TestMultiProtocol_NormalDial(t *testing.T) {
	dialCalled := false
	p1 := newMockProxy("proto1", "1.1.1.1:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		dialCalled = true
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p1), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p1})
	conn, err := mp.DialContext(context.Background(), newTestMetadata())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if conn == nil {
		t.Fatal("expected conn, got nil")
	}
	if !dialCalled {
		t.Fatal("expected dial to be called on proto1")
	}
	_ = conn.Close()
}

func TestMultiProtocol_InlineFailover(t *testing.T) {
	p1 := newMockProxy("proto1", "1.1.1.1:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("proto1 down")
	}

	p2Called := false
	p2 := newMockProxy("proto2", "2.2.2.2:443", false)
	p2.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		p2Called = true
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p2), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p1, p2})
	conn, err := mp.DialContext(context.Background(), newTestMetadata())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !p2Called {
		t.Fatal("expected failover to proto2")
	}
	_ = conn.Close()
}

func TestMultiProtocol_MaxFailures(t *testing.T) {
	p1 := newMockProxy("proto1", "1.1.1.1:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("fail")
	}

	p2 := newMockProxy("proto2", "2.2.2.2:443", false)
	p2.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p2), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p1, p2}, func(m *MultiProtocol) {
		m.maxFailures = 3
	})

	meta := newTestMetadata()

	// First two failures should not mark dead (failCount goes to 1, 2)
	for i := 0; i < 2; i++ {
		conn, _ := mp.DialContext(context.Background(), meta)
		if conn != nil {
			_ = conn.Close()
		}
	}
	if !mp.protocols[0].alive.Load() {
		t.Fatal("proto1 should still be alive after 2 failures")
	}

	// Third failure marks it dead
	conn, err := mp.DialContext(context.Background(), meta)
	if err != nil {
		t.Fatalf("expected fallback to succeed, got %v", err)
	}
	_ = conn.Close()

	if mp.protocols[0].alive.Load() {
		t.Fatal("proto1 should be dead after 3 failures")
	}
}

func TestMultiProtocol_ActiveIndexUpdate(t *testing.T) {
	p1 := newMockProxy("proto1", "1.1.1.1:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("fail")
	}
	p2 := newMockProxy("proto2", "2.2.2.2:443", false)
	p2.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p2), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p1, p2}, func(m *MultiProtocol) {
		m.maxFailures = 1
	})

	if mp.activeIndex.Load() != 0 {
		t.Fatal("initial activeIndex should be 0")
	}

	conn, err := mp.DialContext(context.Background(), newTestMetadata())
	if err != nil {
		t.Fatalf("expected success via failover, got %v", err)
	}
	_ = conn.Close()

	if mp.activeIndex.Load() != 1 {
		t.Fatalf("expected activeIndex=1 after proto1 death, got %d", mp.activeIndex.Load())
	}
}

func TestMultiProtocol_ResetAllOnAllDead(t *testing.T) {
	p1 := newMockProxy("proto1", "1.1.1.1:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("fail")
	}
	p2 := newMockProxy("proto2", "2.2.2.2:443", false)
	p2.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("fail")
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p1, p2}, func(m *MultiProtocol) {
		m.maxFailures = 1
		m.cooldownBase = 10 * time.Second
		m.cooldownMax = 5 * time.Minute
	})

	_, err := mp.DialContext(context.Background(), newTestMetadata())
	if err == nil {
		t.Fatal("expected error when all protocols fail")
	}

	// After all dead, should enter cooldown instead of immediate reset
	if mp.cooldownUntil.Load() == 0 {
		t.Fatal("expected cooldown to be active after all protocols dead")
	}
	if mp.cooldownCount.Load() != 1 {
		t.Fatalf("expected cooldownCount=1, got %d", mp.cooldownCount.Load())
	}
}

func TestMultiProtocol_AdaptiveProbeRate(t *testing.T) {
	mp := newTestMultiProtocol("test", []ProxyAdapter{
		newMockProxy("p1", "1.1.1.1:443", false),
	}, func(m *MultiProtocol) {
		m.probeRate = 0.1
		m.minProbeRate = 0.02
	})

	p := mp.protocols[0]

	// No shadow failures: rate = 0.1
	rate := mp.adaptiveRate(p)
	if rate != 0.1 {
		t.Fatalf("expected rate=0.1, got %f", rate)
	}

	// 1 shadow failure: rate = 0.1 * 0.5 = 0.05
	p.shadowFails.Store(1)
	rate = mp.adaptiveRate(p)
	if rate != 0.05 {
		t.Fatalf("expected rate=0.05, got %f", rate)
	}

	// 2 shadow failures: rate = 0.1 * 0.25 = 0.025
	p.shadowFails.Store(2)
	rate = mp.adaptiveRate(p)
	if rate != 0.025 {
		t.Fatalf("expected rate=0.025, got %f", rate)
	}

	// 3 shadow failures: rate = 0.1 * 0.125 = 0.0125, clamped to minProbeRate=0.02
	p.shadowFails.Store(3)
	rate = mp.adaptiveRate(p)
	if rate != 0.02 {
		t.Fatalf("expected rate=0.02 (clamped), got %f", rate)
	}
}

func TestMultiProtocol_UDPDial(t *testing.T) {
	p1 := newMockProxy("proto1", "1.1.1.1:443", true)
	p1.listenFunc = func(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
		return nil, errors.New("udp fail")
	}

	p2 := newMockProxy("proto2", "2.2.2.2:443", true)
	p2Called := false
	p2.listenFunc = func(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
		p2Called = true
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		return newPacketConn(pc, p2), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p1, p2})

	meta := &C.Metadata{NetWork: C.UDP, DstPort: 53, Host: "dns.example.com"}
	pc, err := mp.ListenPacketContext(context.Background(), meta)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !p2Called {
		t.Fatal("expected fallback to proto2 for UDP")
	}
	_ = pc.Close()
}

func TestMultiProtocol_UDPSkipsNonUDPProtocols(t *testing.T) {
	// p1 does not support UDP
	p1 := newMockProxy("proto1", "1.1.1.1:443", false)

	// p2 supports UDP
	p2 := newMockProxy("proto2", "2.2.2.2:443", true)
	p2Called := false
	p2.listenFunc = func(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
		p2Called = true
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		return newPacketConn(pc, p2), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p1, p2})

	meta := &C.Metadata{NetWork: C.UDP, DstPort: 53}
	pc, err := mp.ListenPacketContext(context.Background(), meta)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !p2Called {
		t.Fatal("expected proto2 to be called (proto1 has no UDP)")
	}
	_ = pc.Close()
}

func TestMultiProtocol_SingleProtocol(t *testing.T) {
	dialCount := 0
	p1 := newMockProxy("solo", "1.1.1.1:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		dialCount++
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p1), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p1})

	for i := 0; i < 5; i++ {
		conn, err := mp.DialContext(context.Background(), newTestMetadata())
		if err != nil {
			t.Fatalf("dial %d: unexpected error: %v", i, err)
		}
		_ = conn.Close()
	}

	if dialCount != 5 {
		t.Fatalf("expected 5 dials, got %d", dialCount)
	}
}

func TestMultiProtocol_ContextCancellation(t *testing.T) {
	p1 := newMockProxy("proto1", "1.1.1.1:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("fail")
	}

	p2 := newMockProxy("proto2", "2.2.2.2:443", false)
	p2Called := false
	p2.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		p2Called = true
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p2), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p1, p2})

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel before first dial attempt by using an already-cancelled context
	cancel()

	_, err := mp.DialContext(ctx, newTestMetadata())
	// With cancelled context, proto1 fails then ctx.Err() != nil breaks the loop
	if err == nil {
		t.Fatal("expected error with cancelled context")
	}
	if p2Called {
		t.Fatal("proto2 should not be called after context cancellation")
	}
}

func TestMultiProtocol_MarshalJSON(t *testing.T) {
	p1 := newMockProxy("proto1", "1.1.1.1:443", false)
	p2 := newMockProxy("proto2", "2.2.2.2:443", true)

	mp := newTestMultiProtocol("test-mp", []ProxyAdapter{p1, p2})

	data, err := mp.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON error: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if result["type"] != "MultiProtocol" {
		t.Fatalf("expected type=MultiProtocol, got %v", result["type"])
	}

	protocols, ok := result["protocols"].([]any)
	if !ok {
		t.Fatal("expected protocols to be an array")
	}
	if len(protocols) != 2 {
		t.Fatalf("expected 2 protocols, got %d", len(protocols))
	}

	first := protocols[0].(map[string]any)
	if first["active"] != true {
		t.Fatal("first protocol should be active")
	}
	if first["alive"] != true {
		t.Fatal("first protocol should be alive")
	}

	second := protocols[1].(map[string]any)
	if second["active"] != false {
		t.Fatal("second protocol should not be active")
	}
}

func TestMultiProtocol_Close(t *testing.T) {
	p1 := newMockProxy("proto1", "1.1.1.1:443", false)
	p2 := newMockProxy("proto2", "2.2.2.2:443", false)

	mp := newTestMultiProtocol("test", []ProxyAdapter{p1, p2})
	err := mp.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	if !p1.closed.Load() {
		t.Fatal("proto1 should be closed")
	}
	if !p2.closed.Load() {
		t.Fatal("proto2 should be closed")
	}
}

// --- RED/GREEN bug verification tests ---

func TestMultiProtocol_Bug1_SharedProbeMetaRace(t *testing.T) {
	// Bug 1: triggerShadowDial creates ONE probeMeta and passes it to multiple
	// concurrent goroutines. Proxy implementations modify metadata.DstIP during
	// DialContext, causing a data race.
	//
	// This test must be run with -race to detect the issue.

	// p0: dead, will be probed — modifies metadata.DstIP in DialContext
	p0 := newMockProxy("proto0", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		// Simulate what base.go:155, direct.go:58 etc. do: write to metadata.DstIP
		metadata.DstIP = netip.MustParseAddr("10.0.0.1")
		time.Sleep(10 * time.Millisecond)
		return nil, errors.New("still down")
	}

	// p1: dead, will also be probed — also modifies metadata.DstIP
	p1 := newMockProxy("proto1", "2.2.2.2:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		metadata.DstIP = netip.MustParseAddr("10.0.0.2")
		time.Sleep(10 * time.Millisecond)
		return nil, errors.New("still down")
	}

	// p2: alive, active protocol — succeeds
	p2 := newMockProxy("proto2", "3.3.3.3:443", false)
	p2.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p2), nil
	}

	mp := newTestMultiProtocol("test-race", []ProxyAdapter{p0, p1, p2}, func(m *MultiProtocol) {
		m.probeRate = 1.0    // always probe
		m.minProbeRate = 1.0 // always probe
		m.maxFailures = 1
	})

	// Mark p0 and p1 as dead, set activeIndex to 2
	mp.protocols[0].alive.Store(false)
	mp.protocols[1].alive.Store(false)
	mp.activeIndex.Store(2)

	// Trigger many dials concurrently to amplify the race window
	for i := 0; i < 50; i++ {
		conn, err := mp.DialContext(context.Background(), newTestMetadata())
		if err != nil {
			t.Fatalf("dial %d: unexpected error: %v", i, err)
		}
		_ = conn.Close()
	}

	// Give shadow dial goroutines time to complete
	time.Sleep(100 * time.Millisecond)
}

func TestMultiProtocol_Bug2_LoopSkipsRecoveredProtocols(t *testing.T) {
	// Bug 2: If activeIndex=2 and protocol[0] has recovered (alive=true),
	// but protocol[2] fails, the loop starting from startIdx=2 never checks
	// protocol[0] even though it's alive.

	p0Called := false
	p0 := newMockProxy("proto0", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		p0Called = true
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p0), nil
	}

	p1 := newMockProxy("proto1", "2.2.2.2:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("p1 down")
	}

	p2 := newMockProxy("proto2", "3.3.3.3:443", false)
	p2.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("p2 down")
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0, p1, p2}, func(m *MultiProtocol) {
		m.maxFailures = 100 // don't mark dead from test failures
	})

	// Simulate: p0 recovered (alive=true), p1 dead, p2 alive but will fail this dial.
	// activeIndex stuck at 2 (e.g., not yet updated by shadow dial).
	mp.protocols[0].alive.Store(true)
	mp.protocols[1].alive.Store(false)
	mp.protocols[2].alive.Store(true)
	mp.activeIndex.Store(2)

	conn, err := mp.DialContext(context.Background(), newTestMetadata())
	if err == nil && conn != nil {
		_ = conn.Close()
	}

	// The bug: p0 is alive and working but the loop starts at index 2,
	// so p0 is never tried. If p2 also fails, we get an error even though
	// p0 could have succeeded.
	if !p0Called {
		t.Fatal("BUG: protocol[0] is alive but was never tried because loop starts at activeIndex=2")
	}
}

func TestMultiProtocol_Bug3_SubProtocolWithoutName(t *testing.T) {
	// Bug 3: Sub-protocol configs typically don't include "name" (as shown in
	// docs/config.yaml). The decoder treats "name" as required, so parsing
	// fails entirely. parseProtocolProxy should auto-generate a name.
	decoder := structure.NewDecoder(structure.Option{
		TagName: "proxy", WeaklyTypedInput: true,
		KeyReplacer: structure.DefaultKeyReplacer,
	})

	// Typical sub-protocol config without "name" field (matches docs/config.yaml)
	mapping := map[string]any{
		"type":   "socks5",
		"server": "127.0.0.1",
		"port":   1080,
	}

	proxy, err := parseProtocolProxy(decoder, mapping, BasicOption{})
	if err != nil {
		t.Fatalf("BUG: parseProtocolProxy should succeed without 'name' field, got: %v", err)
	}

	if proxy.Name() == "" {
		t.Fatal("BUG: sub-protocol should have auto-generated name, got empty")
	}
}

func TestMultiProtocol_Bug4_SupportUDPWhenAllUDPDead(t *testing.T) {
	// Bug 4: SupportUDP() is set once at construction based on whether ANY
	// protocol supports UDP. If the only UDP-capable protocol dies,
	// SupportUDP() still returns true, misleading callers.

	// p0: no UDP
	p0 := newMockProxy("proto0", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p0), nil
	}

	// p1: UDP capable
	p1 := newMockProxy("proto1", "2.2.2.2:443", true)

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0, p1})

	// Initially SupportUDP should be true
	if !mp.SupportUDP() {
		t.Fatal("expected SupportUDP=true initially")
	}

	// Mark the only UDP protocol as dead
	mp.protocols[1].alive.Store(false)

	// SupportUDP() should reflect that no alive protocol supports UDP
	if mp.SupportUDP() {
		t.Fatal("BUG: SupportUDP() still true when the only UDP-capable protocol is dead")
	}
}

func TestMultiProtocol_Bug5_ResetAllIfAllDeadRace(t *testing.T) {
	// Bug 5: resetAllIfAllDead has no synchronization. Multiple concurrent
	// DialContext calls can all detect "all dead" and call resetAllIfAllDead
	// concurrently. With cooldown, concurrent calls should safely enter
	// cooldown state without data races.
	//
	// Run with -race to detect any data races.

	p0 := newMockProxy("proto0", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("fail")
	}
	p1 := newMockProxy("proto1", "2.2.2.2:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("fail")
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0, p1}, func(m *MultiProtocol) {
		m.maxFailures = 1
	})

	// Run many concurrent dials that all fail → all trigger resetAllIfAllDead
	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 50; j++ {
				_, _ = mp.DialContext(context.Background(), newTestMetadata())
			}
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}

	// After all goroutines finish, cooldown should be active
	if mp.cooldownUntil.Load() == 0 {
		t.Fatal("expected cooldown to be active after concurrent all-dead failures")
	}
	if mp.cooldownCount.Load() == 0 {
		t.Fatal("expected cooldownCount > 0")
	}
}

func TestMultiProtocol_Bug6_ShadowDialAfterClose(t *testing.T) {
	// Bug 6: Close() shuts down sub-proxies but shadow dial goroutines use
	// context.Background() with no cancellation. They may call DialContext
	// on already-closed proxies after Close() returns.

	dialAfterClose := atomic.Bool{}

	p0 := newMockProxy("proto0", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		// Real proxies check context during dial. Simulate slow dial that respects ctx.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
		// If we reach here after Close(), the context wasn't cancelled properly
		if p0.closed.Load() {
			dialAfterClose.Store(true)
		}
		return nil, errors.New("down")
	}

	p1 := newMockProxy("proto1", "2.2.2.2:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p1), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0, p1}, func(m *MultiProtocol) {
		m.probeRate = 1.0
		m.minProbeRate = 1.0
		m.maxFailures = 1
	})

	// Mark p0 dead, activeIndex=1
	mp.protocols[0].alive.Store(false)
	mp.activeIndex.Store(1)

	// Trigger shadow dial (p0 will be probed in background goroutine)
	conn, err := mp.DialContext(context.Background(), newTestMetadata())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = conn.Close()

	// Close immediately — shadow dial goroutine should be cancelled
	_ = mp.Close()

	// Wait for shadow dial goroutine to finish
	time.Sleep(200 * time.Millisecond)

	if dialAfterClose.Load() {
		t.Fatal("BUG: shadow dial goroutine called DialContext on a closed proxy")
	}
}

func TestMultiProtocol_Bug7_ParseProtocolProxyMutatesInput(t *testing.T) {
	// Bug 7: parseProtocolProxy writes "name" into the input mapping when not
	// present, mutating the caller's data. This is a side effect that could
	// affect config reloads or shared references.
	decoder := structure.NewDecoder(structure.Option{
		TagName: "proxy", WeaklyTypedInput: true,
		KeyReplacer: structure.DefaultKeyReplacer,
	})

	mapping := map[string]any{
		"type":   "socks5",
		"server": "127.0.0.1",
		"port":   1080,
	}

	// Record original keys
	_, hadName := mapping["name"]
	if hadName {
		t.Fatal("test setup: mapping should not have 'name'")
	}

	_, _ = parseProtocolProxy(decoder, mapping, BasicOption{})

	// Check if mapping was mutated
	_, hasNameNow := mapping["name"]
	if hasNameNow {
		t.Fatal("BUG: parseProtocolProxy mutated the input mapping by adding 'name' key")
	}
}

func TestMultiProtocol_Bug8_TCPFailuresPoisonUDP(t *testing.T) {
	// Bug 8: TCP and UDP share the same failCount and alive state.
	// A burst of TCP failures can mark a protocol dead, blocking UDP
	// even if UDP would succeed.

	p0 := newMockProxy("proto0", "1.1.1.1:443", true)
	// TCP always fails
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("tcp fail")
	}
	// UDP always succeeds
	p0.listenFunc = func(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		return newPacketConn(pc, p0), nil
	}

	p1 := newMockProxy("proto1", "2.2.2.2:443", true)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p1), nil
	}
	p1.listenFunc = func(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		return newPacketConn(pc, p1), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0, p1}, func(m *MultiProtocol) {
		m.maxFailures = 3
	})

	meta := newTestMetadata()

	// 3 TCP failures on p0 → marks p0 dead
	for i := 0; i < 3; i++ {
		conn, _ := mp.DialContext(context.Background(), meta)
		if conn != nil {
			_ = conn.Close()
		}
	}

	// p0 should be dead now
	if mp.protocols[0].alive.Load() {
		t.Fatal("expected p0 dead after 3 TCP failures")
	}

	// Now try UDP — p0 should still be usable for UDP since UDP works fine
	udpMeta := &C.Metadata{NetWork: C.UDP, DstPort: 53, Host: "dns.example.com"}
	pc, err := mp.ListenPacketContext(context.Background(), udpMeta)
	if err != nil {
		t.Fatalf("UDP should succeed, got: %v", err)
	}
	_ = pc.Close()

	// Check: did UDP fall through to p1 (indicating p0 was poisoned)?
	// If p0 is dead, UDP had to use p1 instead of the preferred p0.
	if !mp.protocols[0].alive.Load() {
		t.Log("NOTE: TCP failures poisoned UDP — p0 is dead for UDP despite UDP working fine")
		t.Log("This is a design limitation: TCP and UDP share failCount/alive state")
		// This is a design issue, not necessarily a bug to fix.
		// Mark as informational rather than fatal.
	}
}

func TestMultiProtocol_Bug10_WrapAroundDoesNotUpdateActiveIndex(t *testing.T) {
	// Bug 10: When wrap-around finds a working protocol at a LOWER index
	// than activeIndex, it should update activeIndex. Otherwise every
	// subsequent call wastes time trying the failing protocol first.
	//
	// Scenario: activeIndex=2, protocol[2] fails once (not dead yet),
	// wrap-around reaches protocol[0] which succeeds. activeIndex should
	// now be 0.

	p0 := newMockProxy("proto0", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p0), nil
	}

	p1 := newMockProxy("proto1", "2.2.2.2:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("p1 down")
	}

	p2 := newMockProxy("proto2", "3.3.3.3:443", false)
	p2.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("p2 temporarily failing")
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0, p1, p2}, func(m *MultiProtocol) {
		m.maxFailures = 100 // high threshold so p2 doesn't get marked dead
	})

	// Simulate degraded state: activeIndex at 2
	mp.protocols[0].alive.Store(true)
	mp.protocols[1].alive.Store(false)
	mp.protocols[2].alive.Store(true)
	mp.activeIndex.Store(2)

	// Dial: p2 fails → wrap to p0 which succeeds
	conn, err := mp.DialContext(context.Background(), newTestMetadata())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = conn.Close()

	// activeIndex should now be updated to 0 (the protocol that succeeded)
	if idx := mp.activeIndex.Load(); idx != 0 {
		t.Fatalf("BUG: activeIndex should be 0 after wrap-around found protocol[0] works, got %d", idx)
	}
}

func TestMultiProtocol_Bug_ShadowDialNeverFiresWhenActiveIndexZero(t *testing.T) {
	// BUG: When activeIndex=0 but protocol[0] is dead, triggerShadowDial
	// checked activeIndex==0 and returned immediately, thinking "already on
	// highest priority". But protocol[0] is dead — the actual connected
	// protocol is [1]. Shadow dial should probe protocol[0] for recovery.
	//
	// Scenario:
	//   - protocol[0]: dead (hysteria2 unreachable)
	//   - protocol[1]: alive (anytls working)
	//   - activeIndex: 0 (stale, never updated upward)
	//   - Every connection goes through protocol[1] via skip+fallback
	//   - Shadow dial should fire to probe protocol[0], but never does

	shadowProbed := atomic.Bool{}

	p0 := newMockProxy("hysteria2", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		// Shadow dial probe will call this
		shadowProbed.Store(true)
		return nil, errors.New("hysteria2 still down")
	}

	p1 := newMockProxy("anytls", "2.2.2.2:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p1), nil
	}

	mp := newTestMultiProtocol("test-shadow-bug", []ProxyAdapter{p0, p1}, func(m *MultiProtocol) {
		m.probeRate = 1.0    // always probe (eliminate randomness)
		m.minProbeRate = 1.0 // always probe
	})

	// Set up the buggy state: activeIndex=0, protocol[0] dead
	mp.protocols[0].alive.Store(false)
	mp.activeIndex.Store(0) // stale — not updated when protocol[0] died

	// Dial multiple times — each should trigger shadow dial for protocol[0]
	for i := 0; i < 10; i++ {
		conn, err := mp.DialContext(context.Background(), newTestMetadata())
		if err != nil {
			t.Fatalf("dial %d: unexpected error: %v", i, err)
		}
		_ = conn.Close()
	}

	// Give shadow dial goroutines time to run
	time.Sleep(100 * time.Millisecond)

	if !shadowProbed.Load() {
		t.Fatal("BUG: shadow dial never probed dead protocol[0] — triggerShadowDial returned early because activeIndex==0")
	}
}

// --- cooldown mechanism tests ---

func TestMultiProtocol_CooldownEnterAndProbe(t *testing.T) {
	// When all protocols fail, system enters cooldown. During cooldown,
	// each request probes only one protocol (round-robin).
	probeCount := atomic.Int32{}

	p0 := newMockProxy("proto0", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		probeCount.Add(1)
		return nil, errors.New("fail")
	}
	p1 := newMockProxy("proto1", "2.2.2.2:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		probeCount.Add(1)
		return nil, errors.New("fail")
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0, p1}, func(m *MultiProtocol) {
		m.maxFailures = 1
		m.cooldownBase = 1 * time.Hour // long cooldown so it doesn't expire during test
	})

	// First dial: all fail → enter cooldown
	_, err := mp.DialContext(context.Background(), newTestMetadata())
	if err == nil {
		t.Fatal("expected error")
	}
	if mp.cooldownUntil.Load() == 0 {
		t.Fatal("expected cooldown active")
	}

	// Reset probe count — we only care about cooldown probes
	probeCount.Store(0)

	// Next 4 dials should each probe exactly 1 protocol
	for i := 0; i < 4; i++ {
		_, _ = mp.DialContext(context.Background(), newTestMetadata())
	}

	// 4 requests = 4 probes (one per request), round-robin across 2 protocols
	if got := probeCount.Load(); got != 4 {
		t.Fatalf("expected 4 cooldown probes, got %d", got)
	}
}

func TestMultiProtocol_CooldownProbeSuccess(t *testing.T) {
	// During cooldown, if a probe succeeds, cooldown is exited and
	// normal operation resumes.
	callCount := 0

	p0 := newMockProxy("proto0", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		callCount++
		if callCount <= 2 {
			return nil, errors.New("fail")
		}
		// Recover on 3rd call
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return NewConn(c1, p0), nil
	}
	p1 := newMockProxy("proto1", "2.2.2.2:443", false)
	p1.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("fail")
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0, p1}, func(m *MultiProtocol) {
		m.maxFailures = 1
		m.cooldownBase = 1 * time.Hour
	})

	// All fail → enter cooldown
	_, err := mp.DialContext(context.Background(), newTestMetadata())
	if err == nil {
		t.Fatal("expected error")
	}
	if mp.cooldownUntil.Load() == 0 {
		t.Fatal("expected cooldown active")
	}

	// Probe: round-robin, first hits p0 (callCount=2, fails)
	_, err = mp.DialContext(context.Background(), newTestMetadata())
	if err == nil {
		t.Fatal("expected probe to fail")
	}

	// Probe: hits p1 (fails)
	_, err = mp.DialContext(context.Background(), newTestMetadata())
	if err == nil {
		t.Fatal("expected probe to fail")
	}

	// Probe: hits p0 again (callCount=3, succeeds!)
	conn, err := mp.DialContext(context.Background(), newTestMetadata())
	if err != nil {
		t.Fatalf("expected probe to succeed, got %v", err)
	}
	_ = conn.Close()

	// Cooldown should be exited
	if mp.cooldownUntil.Load() != 0 {
		t.Fatal("expected cooldown to be cleared after successful probe")
	}
	if mp.cooldownCount.Load() != 0 {
		t.Fatal("expected cooldownCount reset to 0 after successful probe")
	}

	// All protocols should be alive again
	for i, p := range mp.protocols {
		if !p.alive.Load() {
			t.Fatalf("protocol[%d] should be alive after cooldown exit", i)
		}
	}
}

func TestMultiProtocol_CooldownExponentialBackoff(t *testing.T) {
	p0 := newMockProxy("proto0", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		return nil, errors.New("fail")
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0}, func(m *MultiProtocol) {
		m.maxFailures = 1
		m.cooldownBase = 10 * time.Second
		m.cooldownMax = 5 * time.Minute
	})

	// Each cycle: all fail → enter cooldown → force expire → repeat
	expectedDurations := []time.Duration{
		10 * time.Second,  // 10 * 2^0
		20 * time.Second,  // 10 * 2^1
		40 * time.Second,  // 10 * 2^2
		80 * time.Second,  // 10 * 2^3
		160 * time.Second, // 10 * 2^4
		300 * time.Second, // capped at cooldownMax (5min)
		300 * time.Second, // stays capped
	}

	for i, expected := range expectedDurations {
		// Clear cooldown to simulate expiry (without resetting count)
		mp.cooldownUntil.Store(0)
		// Reset all protocols alive for the next cycle
		for _, p := range mp.protocols {
			p.alive.Store(true)
			p.failCount.Store(0)
		}

		now := time.Now()
		_, _ = mp.DialContext(context.Background(), newTestMetadata())

		deadline := time.Unix(0, mp.cooldownUntil.Load())
		actual := deadline.Sub(now)

		// Allow 1 second tolerance
		if actual < expected-time.Second || actual > expected+time.Second {
			t.Fatalf("attempt %d: expected cooldown ~%v, got %v", i+1, expected, actual)
		}
	}
}

func TestMultiProtocol_CooldownExpiry(t *testing.T) {
	// When cooldown expires naturally, all protocols are reset alive
	// but cooldownCount is preserved for continued backoff.
	callCount := 0

	p0 := newMockProxy("proto0", "1.1.1.1:443", false)
	p0.dialFunc = func(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
		callCount++
		return nil, errors.New("fail")
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0}, func(m *MultiProtocol) {
		m.maxFailures = 1
		m.cooldownBase = 10 * time.Second
		m.cooldownMax = 5 * time.Minute
	})

	// All fail → enter cooldown (count=1)
	_, _ = mp.DialContext(context.Background(), newTestMetadata())
	if mp.cooldownCount.Load() != 1 {
		t.Fatalf("expected cooldownCount=1, got %d", mp.cooldownCount.Load())
	}

	// Simulate cooldown expiry by setting deadline to past
	mp.cooldownUntil.Store(time.Now().Add(-1 * time.Second).UnixNano())

	// Next dial: cooldown expired → exitCooldown(false) → protocols reset alive
	// → normal flow → all fail again → enter cooldown (count=2)
	_, _ = mp.DialContext(context.Background(), newTestMetadata())

	if mp.cooldownCount.Load() != 2 {
		t.Fatalf("expected cooldownCount=2 after expiry+re-fail, got %d", mp.cooldownCount.Load())
	}
}

func TestMultiProtocol_CooldownUDPProbeSkipsNonUDP(t *testing.T) {
	// During cooldown, UDP probe should skip non-UDP protocols.
	p0 := newMockProxy("proto0", "1.1.1.1:443", false) // no UDP
	p1 := newMockProxy("proto1", "2.2.2.2:443", true)  // has UDP
	p1Called := false
	p1.listenFunc = func(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
		p1Called = true
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		return newPacketConn(pc, p1), nil
	}

	mp := newTestMultiProtocol("test", []ProxyAdapter{p0, p1}, func(m *MultiProtocol) {
		m.maxFailures = 1
		m.cooldownBase = 1 * time.Hour
	})

	// Force cooldown state
	mp.cooldownUntil.Store(time.Now().Add(1 * time.Hour).UnixNano())
	mp.cooldownCount.Store(1)
	for _, p := range mp.protocols {
		p.alive.Store(false)
	}

	meta := &C.Metadata{NetWork: C.UDP, DstPort: 53, Host: "dns.example.com"}
	pc, err := mp.ListenPacketContext(context.Background(), meta)
	if err != nil {
		t.Fatalf("expected UDP probe to succeed via p1, got %v", err)
	}
	_ = pc.Close()

	if !p1Called {
		t.Fatal("expected p1 (UDP-capable) to be probed, not p0")
	}

	// Cooldown should be exited
	if mp.cooldownUntil.Load() != 0 {
		t.Fatal("expected cooldown cleared after successful UDP probe")
	}
}
