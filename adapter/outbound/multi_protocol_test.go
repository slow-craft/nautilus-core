package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

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
	})

	_, err := mp.DialContext(context.Background(), newTestMetadata())
	if err == nil {
		t.Fatal("expected error when all protocols fail")
	}

	// After all dead, resetAllIfAllDead should have been called
	for i, p := range mp.protocols {
		if !p.alive.Load() {
			t.Fatalf("protocol[%d] should be alive after reset", i)
		}
		if p.failCount.Load() != 0 {
			t.Fatalf("protocol[%d] failCount should be 0 after reset", i)
		}
	}
	if mp.activeIndex.Load() != 0 {
		t.Fatal("activeIndex should be 0 after reset")
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
