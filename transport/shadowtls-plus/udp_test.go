package shadowtlsplus

import (
	"context"
	"testing"
	"time"
)

func TestKCPModeConstants(t *testing.T) {
	if KCPModeFast != "fast" {
		t.Errorf("KCPModeFast = %q, want %q", KCPModeFast, "fast")
	}
	if KCPModeNormal != "normal" {
		t.Errorf("KCPModeNormal = %q, want %q", KCPModeNormal, "normal")
	}
	if KCPModeDefault != "default" {
		t.Errorf("KCPModeDefault = %q, want %q", KCPModeDefault, "default")
	}
}

func TestDefaultKCPConfig(t *testing.T) {
	config := DefaultKCPConfig()

	if config.Mode != KCPModeFast {
		t.Errorf("Mode = %q, want %q", config.Mode, KCPModeFast)
	}
	if config.MTU != 1400 {
		t.Errorf("MTU = %d, want 1400", config.MTU)
	}
	if config.SndWnd != 1024 {
		t.Errorf("SndWnd = %d, want 1024", config.SndWnd)
	}
	if config.RcvWnd != 1024 {
		t.Errorf("RcvWnd = %d, want 1024", config.RcvWnd)
	}
	if config.DataShard != 0 {
		t.Errorf("DataShard = %d, want 0", config.DataShard)
	}
	if config.ParityShard != 0 {
		t.Errorf("ParityShard = %d, want 0", config.ParityShard)
	}
}

func TestKCPConfig_ApplyMode_Fast(t *testing.T) {
	config := &KCPConfig{Mode: KCPModeFast}
	config.applyMode()

	if config.NoDelay != 1 {
		t.Errorf("NoDelay = %d, want 1", config.NoDelay)
	}
	if config.Interval != 10 {
		t.Errorf("Interval = %d, want 10", config.Interval)
	}
	if config.Resend != 2 {
		t.Errorf("Resend = %d, want 2", config.Resend)
	}
	if config.NC != 1 {
		t.Errorf("NC = %d, want 1", config.NC)
	}
}

func TestKCPConfig_ApplyMode_Normal(t *testing.T) {
	config := &KCPConfig{Mode: KCPModeNormal}
	config.applyMode()

	if config.NoDelay != 0 {
		t.Errorf("NoDelay = %d, want 0", config.NoDelay)
	}
	if config.Interval != 30 {
		t.Errorf("Interval = %d, want 30", config.Interval)
	}
	if config.Resend != 2 {
		t.Errorf("Resend = %d, want 2", config.Resend)
	}
	if config.NC != 1 {
		t.Errorf("NC = %d, want 1", config.NC)
	}
}

func TestKCPConfig_ApplyMode_Default(t *testing.T) {
	config := &KCPConfig{Mode: KCPModeDefault}
	config.applyMode()

	if config.NoDelay != 0 {
		t.Errorf("NoDelay = %d, want 0", config.NoDelay)
	}
	if config.Interval != 40 {
		t.Errorf("Interval = %d, want 40", config.Interval)
	}
	if config.Resend != 0 {
		t.Errorf("Resend = %d, want 0", config.Resend)
	}
	if config.NC != 0 {
		t.Errorf("NC = %d, want 0", config.NC)
	}
}

func TestKCPConfig_ApplyMode_Unknown(t *testing.T) {
	config := &KCPConfig{Mode: "unknown"}
	config.applyMode()

	// Should fall through to default mode
	if config.NoDelay != 0 {
		t.Errorf("NoDelay = %d, want 0", config.NoDelay)
	}
	if config.Interval != 40 {
		t.Errorf("Interval = %d, want 40", config.Interval)
	}
}

func TestDefaultUDPConfig(t *testing.T) {
	serverAddr := "example.com:443"
	uuid := "test-uuid"

	config := DefaultUDPConfig(serverAddr, uuid)

	if config.ServerAddr != serverAddr {
		t.Errorf("ServerAddr = %q, want %q", config.ServerAddr, serverAddr)
	}
	if config.UUID != uuid {
		t.Errorf("UUID = %q, want %q", config.UUID, uuid)
	}
	if config.TransportConfig == nil {
		t.Fatal("TransportConfig should not be nil")
	}
	if config.KCP == nil {
		t.Fatal("KCP config should not be nil")
	}
	if config.SessionConfig == nil {
		t.Fatal("SessionConfig should not be nil")
	}

	// UDP has shorter timeouts for fast failover
	if config.HandshakeTimeout != 5*time.Second {
		t.Errorf("HandshakeTimeout = %v, want %v", config.HandshakeTimeout, 5*time.Second)
	}
	if config.DialTimeout != 5*time.Second {
		t.Errorf("DialTimeout = %v, want %v", config.DialTimeout, 5*time.Second)
	}
	if config.RetryTimes != 1 {
		t.Errorf("RetryTimes = %d, want 1", config.RetryTimes)
	}
}

func TestNewUDPTransport_ValidConfig(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)

	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v, want nil", err)
	}
	if transport == nil {
		t.Fatal("NewUDPTransport() returned nil")
	}
	if transport.config != config {
		t.Error("Transport config not set correctly")
	}
	if transport.maxRTTSamples != 10 {
		t.Errorf("maxRTTSamples = %d, want 10", transport.maxRTTSamples)
	}
}

func TestNewUDPTransport_MissingServerAddr(t *testing.T) {
	config := &UDPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr: "",
			UUID:       "test-uuid",
		},
	}
	_, err := NewUDPTransport(config)

	if err == nil {
		t.Fatal("NewUDPTransport() should return error for empty server address")
	}
	expectedErr := "transport: server address required"
	if err.Error() != expectedErr {
		t.Errorf("error = %q, want %q", err.Error(), expectedErr)
	}
}

func TestNewUDPTransport_MissingUUID(t *testing.T) {
	config := &UDPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr: "example.com:443",
			UUID:       "",
		},
	}
	_, err := NewUDPTransport(config)

	if err == nil {
		t.Fatal("NewUDPTransport() should return error for empty UUID")
	}
	expectedErr := "transport: UUID required"
	if err.Error() != expectedErr {
		t.Errorf("error = %q, want %q", err.Error(), expectedErr)
	}
}

func TestNewUDPTransport_AppliesKCPMode(t *testing.T) {
	config := &UDPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr: "example.com:443",
			UUID:       "test-uuid",
		},
		KCP: &KCPConfig{
			Mode: KCPModeFast,
		},
	}

	_, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	// KCP mode should have been applied
	if config.KCP.NoDelay != 1 {
		t.Errorf("KCP.NoDelay = %d, want 1 (fast mode)", config.KCP.NoDelay)
	}
}

func TestUDPTransport_Protocol(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	if transport.Protocol() != ProtocolUDP {
		t.Errorf("Protocol() = %v, want %v", transport.Protocol(), ProtocolUDP)
	}
}

func TestUDPTransport_IsConnected_Initial(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	if transport.IsConnected() {
		t.Error("IsConnected() should be false initially")
	}
}

func TestUDPTransport_Health_Initial(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
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

func TestUDPTransport_UpdateHealth(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	// Simulate failures
	transport.failureCount.Store(5)
	transport.updateHealth(true, 50*time.Millisecond)

	health := transport.Health()
	if !health.Available {
		t.Error("Health.Available should be true")
	}
	if health.RTT != 50*time.Millisecond {
		t.Errorf("RTT = %v, want %v", health.RTT, 50*time.Millisecond)
	}
	if health.ConsecutiveFailures != 5 {
		t.Errorf("ConsecutiveFailures = %d, want 5", health.ConsecutiveFailures)
	}
}

func TestUDPTransport_RecordRTT(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	transport.recordRTT(50 * time.Millisecond)

	if transport.lastRTT.Load() != int64(50*time.Millisecond) {
		t.Errorf("lastRTT = %v, want %v", time.Duration(transport.lastRTT.Load()), 50*time.Millisecond)
	}
	if len(transport.rttSamples) != 1 {
		t.Errorf("rttSamples length = %d, want 1", len(transport.rttSamples))
	}
}

func TestUDPTransport_RecordRTT_MaxSamples(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	// Record more samples than maxRTTSamples
	for i := 0; i < 15; i++ {
		transport.recordRTT(time.Duration(i+1) * time.Millisecond)
	}

	if len(transport.rttSamples) != transport.maxRTTSamples {
		t.Errorf("rttSamples length = %d, want %d", len(transport.rttSamples), transport.maxRTTSamples)
	}
}

func TestUDPTransport_AverageRTT_Empty(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	avg := transport.averageRTT()
	if avg != 0 {
		t.Errorf("averageRTT() = %v, want 0 for empty samples", avg)
	}
}

func TestUDPTransport_AverageRTT(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	// Record samples: 50ms, 100ms, 150ms
	transport.recordRTT(50 * time.Millisecond)
	transport.recordRTT(100 * time.Millisecond)
	transport.recordRTT(150 * time.Millisecond)

	avg := transport.averageRTT()
	expected := 100 * time.Millisecond
	if avg != expected {
		t.Errorf("averageRTT() = %v, want %v", avg, expected)
	}
}

func TestUDPTransport_Close(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
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

func TestUDPTransport_Stats_NoSession(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	stats := transport.Stats()
	// Should return zero-value SessionStats when no session
	if stats.ActiveCount != 0 {
		t.Errorf("ActiveCount = %d, want 0", stats.ActiveCount)
	}
}

func TestUDPTransport_Session_NoSession(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	session := transport.Session()
	if session != nil {
		t.Error("Session() should return nil when not connected")
	}
}

func TestUDPTransport_Connect_CanceledContext(t *testing.T) {
	config := DefaultUDPConfig("example.com:443", "test-uuid")
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err = transport.Connect(ctx)
	if err != context.Canceled {
		t.Errorf("Connect() error = %v, want %v", err, context.Canceled)
	}
}

func TestUDPTransport_CreateHandshaker(t *testing.T) {
	config := &UDPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr:       "example.com:443",
			UUID:             "test-uuid",
			SNI:              "custom.sni.com",
			HandshakeTimeout: 10 * time.Second,
		},
		KCP:           DefaultKCPConfig(),
		SessionConfig: DefaultSessionConfig(true),
	}
	transport, err := NewUDPTransport(config)
	if err != nil {
		t.Fatalf("NewUDPTransport() error = %v", err)
	}

	handshaker, err := transport.createHandshaker()
	if err != nil {
		t.Fatalf("createHandshaker() error = %v", err)
	}
	if handshaker == nil {
		t.Fatal("createHandshaker() returned nil")
	}
}

func TestKCPConfig_WithFEC(t *testing.T) {
	config := &KCPConfig{
		Mode:        KCPModeFast,
		MTU:         1400,
		SndWnd:      2048,
		RcvWnd:      2048,
		DataShard:   10,
		ParityShard: 3,
	}

	// Verify FEC configuration
	if config.DataShard != 10 {
		t.Errorf("DataShard = %d, want 10", config.DataShard)
	}
	if config.ParityShard != 3 {
		t.Errorf("ParityShard = %d, want 3", config.ParityShard)
	}
}
