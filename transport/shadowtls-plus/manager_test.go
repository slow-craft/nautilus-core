package shadowtlsplus

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// mockTransport implements Transport interface for testing
type mockTransport struct {
	protocol       Protocol
	connected      atomic.Bool
	health         atomic.Pointer[HealthStatus]
	connectErr     error
	openStreamErr  error
	closeErr       error
	connectCalled  atomic.Int32
	streamsCalled  atomic.Int32
	closeCalled    atomic.Int32
	connectDelay   time.Duration
}

func newMockTransport(proto Protocol) *mockTransport {
	m := &mockTransport{protocol: proto}
	m.health.Store(&HealthStatus{Available: false})
	return m
}

func (m *mockTransport) Connect(ctx context.Context) error {
	m.connectCalled.Add(1)
	if m.connectDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.connectDelay):
		}
	}
	if m.connectErr != nil {
		return m.connectErr
	}
	m.connected.Store(true)
	m.health.Store(&HealthStatus{Available: true, RTT: 100 * time.Millisecond})
	return nil
}

func (m *mockTransport) OpenStream(network, address string) (net.Conn, error) {
	m.streamsCalled.Add(1)
	if m.openStreamErr != nil {
		return nil, m.openStreamErr
	}
	// Return a mock connection
	return &mockConn{}, nil
}

func (m *mockTransport) Close() error {
	m.closeCalled.Add(1)
	m.connected.Store(false)
	m.health.Store(&HealthStatus{Available: false})
	if m.closeErr != nil {
		return m.closeErr
	}
	return nil
}

func (m *mockTransport) Health() *HealthStatus {
	return m.health.Load()
}

func (m *mockTransport) Protocol() Protocol {
	return m.protocol
}

func (m *mockTransport) IsConnected() bool {
	return m.connected.Load()
}

func (m *mockTransport) setHealth(available bool, rtt time.Duration) {
	m.health.Store(&HealthStatus{Available: available, RTT: rtt, LastCheck: time.Now()})
}

// mockConn implements net.Conn for testing
type mockConn struct{}

func (c *mockConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (c *mockConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (c *mockConn) Close() error                       { return nil }
func (c *mockConn) LocalAddr() net.Addr                { return nil }
func (c *mockConn) RemoteAddr() net.Addr               { return nil }
func (c *mockConn) SetDeadline(t time.Time) error      { return nil }
func (c *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestSelectionModeConstants(t *testing.T) {
	if SelectionModeAuto != 0 {
		t.Errorf("SelectionModeAuto = %d, want 0", SelectionModeAuto)
	}
	if SelectionModeTCPFirst != 1 {
		t.Errorf("SelectionModeTCPFirst = %d, want 1", SelectionModeTCPFirst)
	}
	if SelectionModeUDPFirst != 2 {
		t.Errorf("SelectionModeUDPFirst = %d, want 2", SelectionModeUDPFirst)
	}
}

func TestDefaultManagerConfig(t *testing.T) {
	config := DefaultManagerConfig()

	if config.SelectionMode != SelectionModeAuto {
		t.Errorf("SelectionMode = %d, want %d", config.SelectionMode, SelectionModeAuto)
	}
	if config.RTTThreshold != 500*time.Millisecond {
		t.Errorf("RTTThreshold = %v, want %v", config.RTTThreshold, 500*time.Millisecond)
	}
	if config.FailureThreshold != 3 {
		t.Errorf("FailureThreshold = %d, want 3", config.FailureThreshold)
	}
	if config.RecoveryInterval != 30*time.Second {
		t.Errorf("RecoveryInterval = %v, want %v", config.RecoveryInterval, 30*time.Second)
	}
	if !config.WarmupBoth {
		t.Error("WarmupBoth should be true by default")
	}
	if config.HealthCheckInterval != 5*time.Second {
		t.Errorf("HealthCheckInterval = %v, want %v", config.HealthCheckInterval, 5*time.Second)
	}
}

func TestNewTransportManager_WithBothTransports(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	udp := newMockTransport(ProtocolUDP)

	manager, err := NewTransportManager(tcp, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	if manager == nil {
		t.Fatal("NewTransportManager() returned nil")
	}
	if manager.tcp != tcp {
		t.Error("TCP transport not set correctly")
	}
	if manager.udp != udp {
		t.Error("UDP transport not set correctly")
	}
}

func TestNewTransportManager_TCPOnly(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)

	manager, err := NewTransportManager(tcp, nil, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	if manager.tcp != tcp {
		t.Error("TCP transport not set correctly")
	}
	if manager.udp != nil {
		t.Error("UDP transport should be nil")
	}
}

func TestNewTransportManager_UDPOnly(t *testing.T) {
	udp := newMockTransport(ProtocolUDP)

	manager, err := NewTransportManager(nil, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	if manager.tcp != nil {
		t.Error("TCP transport should be nil")
	}
	if manager.udp != udp {
		t.Error("UDP transport not set correctly")
	}
}

func TestNewTransportManager_NoTransports(t *testing.T) {
	_, err := NewTransportManager(nil, nil, nil)
	if err == nil {
		t.Fatal("NewTransportManager() should return error when no transports provided")
	}
	expectedErr := "transport: at least one transport required"
	if err.Error() != expectedErr {
		t.Errorf("error = %q, want %q", err.Error(), expectedErr)
	}
}

func TestNewTransportManager_DefaultConfig(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)

	manager, err := NewTransportManager(tcp, nil, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	if manager.config == nil {
		t.Fatal("config should not be nil")
	}
	if manager.config.SelectionMode != SelectionModeAuto {
		t.Error("Default config should use SelectionModeAuto")
	}
}

func TestNewTransportManager_HealthMonitorInitialized(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	config := &ManagerConfig{
		FailureThreshold: 5,
		RecoveryInterval: 60 * time.Second,
	}

	manager, err := NewTransportManager(tcp, nil, config)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	if manager.healthMon == nil {
		t.Fatal("healthMon should be initialized")
	}
	if manager.healthMon.failureThreshold != 5 {
		t.Errorf("failureThreshold = %d, want 5", manager.healthMon.failureThreshold)
	}
	if manager.healthMon.recoveryInterval != 60*time.Second {
		t.Errorf("recoveryInterval = %v, want %v", manager.healthMon.recoveryInterval, 60*time.Second)
	}
}

func TestTransportManager_Start_BothSuccess(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	udp := newMockTransport(ProtocolUDP)

	manager, err := NewTransportManager(tcp, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = manager.Start(ctx)
	if err != nil {
		t.Errorf("Start() error = %v", err)
	}

	// Both should have been connected
	if tcp.connectCalled.Load() != 1 {
		t.Errorf("TCP connect called %d times, want 1", tcp.connectCalled.Load())
	}
	if udp.connectCalled.Load() != 1 {
		t.Errorf("UDP connect called %d times, want 1", udp.connectCalled.Load())
	}

	manager.Close()
}

func TestTransportManager_Start_TCPOnlySuccess(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	udp := newMockTransport(ProtocolUDP)
	udp.connectErr = errors.New("connection refused")

	manager, err := NewTransportManager(tcp, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = manager.Start(ctx)
	if err != nil {
		t.Errorf("Start() should succeed if at least one transport connects, error = %v", err)
	}

	manager.Close()
}

func TestTransportManager_Start_BothFail(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	tcp.connectErr = errors.New("TCP connection refused")
	udp := newMockTransport(ProtocolUDP)
	udp.connectErr = errors.New("UDP connection refused")

	manager, err := NewTransportManager(tcp, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = manager.Start(ctx)
	if err == nil {
		t.Error("Start() should fail when all transports fail to connect")
	}

	manager.Close()
}

func TestTransportManager_OpenStream_TCPAvailable(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	tcp.setHealth(true, 100*time.Millisecond)
	tcp.connected.Store(true)

	manager, err := NewTransportManager(tcp, nil, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	defer manager.Close()

	conn, err := manager.OpenStream("tcp", "example.com:80")
	if err != nil {
		t.Errorf("OpenStream() error = %v", err)
	}
	if conn == nil {
		t.Error("OpenStream() returned nil connection")
	}

	if tcp.streamsCalled.Load() != 1 {
		t.Errorf("TCP streams called %d times, want 1", tcp.streamsCalled.Load())
	}
}

func TestTransportManager_OpenStream_Failover(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	tcp.setHealth(true, 50*time.Millisecond) // TCP faster, will be selected first
	tcp.connected.Store(true)
	tcp.openStreamErr = errors.New("stream error")

	udp := newMockTransport(ProtocolUDP)
	udp.setHealth(true, 100*time.Millisecond)
	udp.connected.Store(true)

	manager, err := NewTransportManager(tcp, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	defer manager.Close()

	conn, err := manager.OpenStream("tcp", "example.com:80")
	if err != nil {
		t.Errorf("OpenStream() error = %v, want nil (should failover)", err)
	}
	if conn == nil {
		t.Error("OpenStream() returned nil connection after failover")
	}

	// TCP should have been tried first (faster RTT), then UDP (failover)
	if tcp.streamsCalled.Load() != 1 {
		t.Errorf("TCP streams called %d times, want 1", tcp.streamsCalled.Load())
	}
	if udp.streamsCalled.Load() != 1 {
		t.Errorf("UDP streams called %d times, want 1", udp.streamsCalled.Load())
	}
}

func TestTransportManager_OpenStream_Closed(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)

	manager, err := NewTransportManager(tcp, nil, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}

	manager.Close()

	_, err = manager.OpenStream("tcp", "example.com:80")
	if err == nil {
		t.Error("OpenStream() should fail when manager is closed")
	}
	expectedErr := "transport: manager closed"
	if err.Error() != expectedErr {
		t.Errorf("error = %q, want %q", err.Error(), expectedErr)
	}
}

func TestTransportManager_SelectBestTransport_Auto_TCPFaster(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	tcp.setHealth(true, 50*time.Millisecond) // Faster
	udp := newMockTransport(ProtocolUDP)
	udp.setHealth(true, 100*time.Millisecond)

	config := &ManagerConfig{
		SelectionMode:    SelectionModeAuto,
		FailureThreshold: 3,
		RecoveryInterval: 30 * time.Second,
	}
	manager, err := NewTransportManager(tcp, udp, config)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	defer manager.Close()

	selected := manager.selectBestTransport()
	if selected != tcp {
		t.Error("Should select TCP when it's significantly faster")
	}
}

func TestTransportManager_SelectBestTransport_Auto_UDPFaster(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	tcp.setHealth(true, 200*time.Millisecond)
	udp := newMockTransport(ProtocolUDP)
	udp.setHealth(true, 50*time.Millisecond) // Significantly faster (>20%)

	config := &ManagerConfig{
		SelectionMode:    SelectionModeAuto,
		FailureThreshold: 3,
		RecoveryInterval: 30 * time.Second,
	}
	manager, err := NewTransportManager(tcp, udp, config)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	defer manager.Close()

	selected := manager.selectBestTransport()
	if selected != udp {
		t.Error("Should select UDP when it's significantly faster")
	}
}

func TestTransportManager_SelectBestTransport_TCPFirst(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	tcp.setHealth(true, 100*time.Millisecond)
	udp := newMockTransport(ProtocolUDP)
	udp.setHealth(true, 50*time.Millisecond) // Even though UDP is faster

	config := &ManagerConfig{
		SelectionMode:    SelectionModeTCPFirst,
		FailureThreshold: 3,
		RecoveryInterval: 30 * time.Second,
	}
	manager, err := NewTransportManager(tcp, udp, config)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	defer manager.Close()

	selected := manager.selectBestTransport()
	if selected != tcp {
		t.Error("TCPFirst mode should always select TCP when available")
	}
}

func TestTransportManager_SelectBestTransport_UDPFirst(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	tcp.setHealth(true, 50*time.Millisecond) // Even though TCP is faster
	udp := newMockTransport(ProtocolUDP)
	udp.setHealth(true, 100*time.Millisecond)

	config := &ManagerConfig{
		SelectionMode:    SelectionModeUDPFirst,
		FailureThreshold: 3,
		RecoveryInterval: 30 * time.Second,
	}
	manager, err := NewTransportManager(tcp, udp, config)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	defer manager.Close()

	selected := manager.selectBestTransport()
	if selected != udp {
		t.Error("UDPFirst mode should always select UDP when available")
	}
}

func TestTransportManager_SelectBestTransport_OnlyOneAvailable(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	tcp.setHealth(true, 100*time.Millisecond)
	udp := newMockTransport(ProtocolUDP)
	udp.setHealth(false, 0) // UDP unavailable

	manager, err := NewTransportManager(tcp, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	defer manager.Close()

	selected := manager.selectBestTransport()
	if selected != tcp {
		t.Error("Should select the only available transport")
	}
}

func TestTransportManager_GetAlternative(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	udp := newMockTransport(ProtocolUDP)

	manager, err := NewTransportManager(tcp, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	defer manager.Close()

	// TCP's alternative should be UDP
	alt := manager.getAlternative(tcp)
	if alt != udp {
		t.Error("TCP's alternative should be UDP")
	}

	// UDP's alternative should be TCP
	alt = manager.getAlternative(udp)
	if alt != tcp {
		t.Error("UDP's alternative should be TCP")
	}

	// nil should return nil
	alt = manager.getAlternative(nil)
	if alt != nil {
		t.Error("nil's alternative should be nil")
	}
}

func TestTransportManager_Close(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	udp := newMockTransport(ProtocolUDP)

	manager, err := NewTransportManager(tcp, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}

	err = manager.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Both should have been closed
	if tcp.closeCalled.Load() != 1 {
		t.Errorf("TCP close called %d times, want 1", tcp.closeCalled.Load())
	}
	if udp.closeCalled.Load() != 1 {
		t.Errorf("UDP close called %d times, want 1", udp.closeCalled.Load())
	}

	// Double close should be safe
	err = manager.Close()
	if err != nil {
		t.Errorf("Double Close() error = %v", err)
	}
}

func TestTransportManager_Close_WithErrors(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	tcp.closeErr = errors.New("TCP close error")
	udp := newMockTransport(ProtocolUDP)
	udp.closeErr = errors.New("UDP close error")

	manager, err := NewTransportManager(tcp, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}

	err = manager.Close()
	if err == nil {
		t.Error("Close() should return error when transports fail to close")
	}
}

func TestTransportManager_Stats(t *testing.T) {
	tcp := newMockTransport(ProtocolTCP)
	tcp.setHealth(true, 100*time.Millisecond)
	udp := newMockTransport(ProtocolUDP)
	udp.setHealth(true, 50*time.Millisecond)

	manager, err := NewTransportManager(tcp, udp, nil)
	if err != nil {
		t.Fatalf("NewTransportManager() error = %v", err)
	}
	defer manager.Close()

	stats := manager.Stats()
	if stats == nil {
		t.Fatal("Stats() returned nil")
	}
	if stats.TCPHealth == nil {
		t.Error("TCPHealth should not be nil")
	}
	if stats.UDPHealth == nil {
		t.Error("UDPHealth should not be nil")
	}
	if stats.TCPHealth.RTT != 100*time.Millisecond {
		t.Errorf("TCPHealth.RTT = %v, want %v", stats.TCPHealth.RTT, 100*time.Millisecond)
	}
	if stats.UDPHealth.RTT != 50*time.Millisecond {
		t.Errorf("UDPHealth.RTT = %v, want %v", stats.UDPHealth.RTT, 50*time.Millisecond)
	}
}

// HealthMonitor tests

func TestHealthMonitor_RecordSuccess(t *testing.T) {
	h := &HealthMonitor{
		failureThreshold: 3,
		recoveryInterval: 30 * time.Second,
		tcpFailures:      5,
		udpFailures:      3,
	}

	h.RecordSuccess(ProtocolTCP)
	if h.tcpFailures != 0 {
		t.Errorf("tcpFailures = %d, want 0 after success", h.tcpFailures)
	}

	h.RecordSuccess(ProtocolUDP)
	if h.udpFailures != 0 {
		t.Errorf("udpFailures = %d, want 0 after success", h.udpFailures)
	}
}

func TestHealthMonitor_RecordFailure(t *testing.T) {
	h := &HealthMonitor{
		failureThreshold: 3,
		recoveryInterval: 30 * time.Second,
	}

	// Record failures below threshold
	h.RecordFailure(ProtocolTCP)
	h.RecordFailure(ProtocolTCP)
	if h.tcpFailures != 2 {
		t.Errorf("tcpFailures = %d, want 2", h.tcpFailures)
	}
	if !h.tcpBlockedAt.IsZero() {
		t.Error("TCP should not be blocked yet")
	}

	// Reach threshold
	h.RecordFailure(ProtocolTCP)
	if h.tcpBlockedAt.IsZero() {
		t.Error("TCP should be blocked after reaching threshold")
	}
}

func TestHealthMonitor_IsBlocked(t *testing.T) {
	h := &HealthMonitor{
		failureThreshold: 3,
		recoveryInterval: 100 * time.Millisecond,
	}

	// Not blocked initially
	if h.IsBlocked(ProtocolTCP) {
		t.Error("TCP should not be blocked initially")
	}

	// Block TCP
	h.tcpBlockedAt = time.Now()
	if !h.IsBlocked(ProtocolTCP) {
		t.Error("TCP should be blocked")
	}

	// Wait for recovery
	time.Sleep(150 * time.Millisecond)
	if h.IsBlocked(ProtocolTCP) {
		t.Error("TCP should not be blocked after recovery interval")
	}
}

func TestHealthMonitor_TryRecover(t *testing.T) {
	h := &HealthMonitor{
		failureThreshold: 3,
		recoveryInterval: 50 * time.Millisecond,
		tcpBlockedAt:     time.Now().Add(-100 * time.Millisecond), // Blocked 100ms ago
		tcpFailures:      5,
		udpBlockedAt:     time.Now(), // Just blocked
		udpFailures:      3,
	}

	h.TryRecover()

	// TCP should be recovered (blocked more than recovery interval ago)
	if !h.tcpBlockedAt.IsZero() {
		t.Error("TCP block should be cleared after recovery")
	}
	if h.tcpFailures != 0 {
		t.Errorf("tcpFailures = %d, want 0 after recovery", h.tcpFailures)
	}

	// UDP should still be blocked
	if h.udpBlockedAt.IsZero() {
		t.Error("UDP block should not be cleared yet")
	}
}

func TestManagerStats_Fields(t *testing.T) {
	stats := &ManagerStats{
		TCPHealth:  &HealthStatus{Available: true, RTT: 100 * time.Millisecond},
		TCPBlocked: false,
		UDPHealth:  &HealthStatus{Available: false},
		UDPBlocked: true,
	}

	if !stats.TCPHealth.Available {
		t.Error("TCPHealth.Available should be true")
	}
	if stats.TCPBlocked {
		t.Error("TCPBlocked should be false")
	}
	if stats.UDPHealth.Available {
		t.Error("UDPHealth.Available should be false")
	}
	if !stats.UDPBlocked {
		t.Error("UDPBlocked should be true")
	}
}
