package shadowtlsplus

import (
	"context"
	"testing"
	"time"
)

func TestDefaultTCPConfig(t *testing.T) {
	serverAddr := "example.com:443"
	uuid := "test-uuid"

	config := DefaultTCPConfig(serverAddr, uuid)

	if config.ServerAddr != serverAddr {
		t.Errorf("ServerAddr = %q, want %q", config.ServerAddr, serverAddr)
	}
	if config.UUID != uuid {
		t.Errorf("UUID = %q, want %q", config.UUID, uuid)
	}
	if config.TransportConfig == nil {
		t.Fatal("TransportConfig should not be nil")
	}
	if config.SessionConfig == nil {
		t.Fatal("SessionConfig should not be nil")
	}
}

func TestNewTCPTransport_ValidConfig(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)

	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v, want nil", err)
	}
	if transport == nil {
		t.Fatal("NewTCPTransport() returned nil")
	}
	if transport.config != config {
		t.Error("Transport config not set correctly")
	}
	if transport.maxRTTSamples != 10 {
		t.Errorf("maxRTTSamples = %d, want 10", transport.maxRTTSamples)
	}
}

func TestNewTCPTransport_MissingServerAddr(t *testing.T) {
	config := &TCPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr: "",
			UUID:       "test-uuid",
		},
	}
	_, err := NewTCPTransport(config)

	if err == nil {
		t.Fatal("NewTCPTransport() should return error for empty server address")
	}
	expectedErr := "transport: server address required"
	if err.Error() != expectedErr {
		t.Errorf("error = %q, want %q", err.Error(), expectedErr)
	}
}

func TestNewTCPTransport_MissingUUID(t *testing.T) {
	config := &TCPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr: "example.com:443",
			UUID:       "",
		},
	}
	_, err := NewTCPTransport(config)

	if err == nil {
		t.Fatal("NewTCPTransport() should return error for empty UUID")
	}
	expectedErr := "transport: UUID required"
	if err.Error() != expectedErr {
		t.Errorf("error = %q, want %q", err.Error(), expectedErr)
	}
}

func TestTCPTransport_Protocol(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	if transport.Protocol() != ProtocolTCP {
		t.Errorf("Protocol() = %v, want %v", transport.Protocol(), ProtocolTCP)
	}
}

func TestTCPTransport_IsConnected_Initial(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	if transport.IsConnected() {
		t.Error("IsConnected() should be false initially")
	}
}

func TestTCPTransport_Health_Initial(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	health := transport.Health()
	if health == nil {
		t.Fatal("Health() returned nil")
	}
	if health.Available {
		t.Error("Health.Available should be false initially")
	}
	if health.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0", health.ConsecutiveFailures)
	}
}

func TestTCPTransport_UpdateHealth(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	// Simulate failures
	transport.failureCount.Store(3)
	transport.updateHealth(true, 100*time.Millisecond)

	health := transport.Health()
	if !health.Available {
		t.Error("Health.Available should be true")
	}
	if health.RTT != 100*time.Millisecond {
		t.Errorf("RTT = %v, want %v", health.RTT, 100*time.Millisecond)
	}
	if health.ConsecutiveFailures != 3 {
		t.Errorf("ConsecutiveFailures = %d, want 3", health.ConsecutiveFailures)
	}
}

func TestTCPTransport_RecordRTT(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	// Record single RTT
	transport.recordRTT(100 * time.Millisecond)

	if transport.lastRTT.Load() != int64(100*time.Millisecond) {
		t.Errorf("lastRTT = %v, want %v", time.Duration(transport.lastRTT.Load()), 100*time.Millisecond)
	}

	if len(transport.rttSamples) != 1 {
		t.Errorf("rttSamples length = %d, want 1", len(transport.rttSamples))
	}
}

func TestTCPTransport_RecordRTT_MaxSamples(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	// Record more samples than maxRTTSamples
	for i := 0; i < 15; i++ {
		transport.recordRTT(time.Duration(i+1) * time.Millisecond)
	}

	if len(transport.rttSamples) != transport.maxRTTSamples {
		t.Errorf("rttSamples length = %d, want %d", len(transport.rttSamples), transport.maxRTTSamples)
	}

	// Should contain last 10 samples (6-15 ms)
	if transport.rttSamples[0] != 6*time.Millisecond {
		t.Errorf("first sample = %v, want %v", transport.rttSamples[0], 6*time.Millisecond)
	}
}

func TestTCPTransport_AverageRTT_Empty(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	avg := transport.averageRTT()
	if avg != 0 {
		t.Errorf("averageRTT() = %v, want 0 for empty samples", avg)
	}
}

func TestTCPTransport_AverageRTT(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	// Record samples: 100ms, 200ms, 300ms
	transport.recordRTT(100 * time.Millisecond)
	transport.recordRTT(200 * time.Millisecond)
	transport.recordRTT(300 * time.Millisecond)

	avg := transport.averageRTT()
	expected := 200 * time.Millisecond
	if avg != expected {
		t.Errorf("averageRTT() = %v, want %v", avg, expected)
	}
}

func TestTCPTransport_Close(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	// Manually set connected state
	transport.connected.Store(true)

	err = transport.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}

	if transport.IsConnected() {
		t.Error("IsConnected() should be false after Close()")
	}

	health := transport.Health()
	if health.Available {
		t.Error("Health.Available should be false after Close()")
	}
}

func TestTCPTransport_Stats_NoSession(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	stats := transport.Stats()
	// Should return zero-value SessionStats when no session
	if stats.ActiveCount != 0 {
		t.Errorf("ActiveCount = %d, want 0", stats.ActiveCount)
	}
}

func TestTCPTransport_Session_NoSession(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	session := transport.Session()
	if session != nil {
		t.Error("Session() should return nil when not connected")
	}
}

func TestTCPTransport_Connect_CanceledContext(t *testing.T) {
	config := DefaultTCPConfig("example.com:443", "test-uuid")
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err = transport.Connect(ctx)
	if err != context.Canceled {
		t.Errorf("Connect() error = %v, want %v", err, context.Canceled)
	}
}

func TestTCPTransport_Connect_Timeout(t *testing.T) {
	config := &TCPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr:       "192.0.2.1:443", // TEST-NET-1, should timeout
			UUID:             "test-uuid",
			DialTimeout:      100 * time.Millisecond,
			HandshakeTimeout: 100 * time.Millisecond,
			RetryTimes:       1,
		},
		SessionConfig: DefaultSessionConfig(true),
	}
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = transport.Connect(ctx)
	if err == nil {
		t.Error("Connect() should return error for unreachable host")
	}

	// Should have recorded failure
	if transport.failureCount.Load() == 0 {
		t.Error("failureCount should be > 0 after failed connection")
	}
}

func TestTCPTransport_CreateHandshaker(t *testing.T) {
	config := &TCPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr:       "example.com:443",
			UUID:             "test-uuid",
			SNI:              "custom.sni.com",
			HandshakeTimeout: 10 * time.Second,
		},
		SessionConfig: DefaultSessionConfig(true),
	}
	transport, err := NewTCPTransport(config)
	if err != nil {
		t.Fatalf("NewTCPTransport() error = %v", err)
	}

	handshaker, err := transport.createHandshaker()
	if err != nil {
		t.Fatalf("createHandshaker() error = %v", err)
	}
	if handshaker == nil {
		t.Fatal("createHandshaker() returned nil")
	}
}
