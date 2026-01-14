package shadowtlsplus

import (
	"testing"
	"time"
)

func TestProtocolString(t *testing.T) {
	tests := []struct {
		protocol Protocol
		expected string
	}{
		{ProtocolTCP, "TCP"},
		{ProtocolUDP, "UDP"},
		{Protocol(0), "UNKNOWN"},
		{Protocol(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.protocol.String()
			if result != tt.expected {
				t.Errorf("Protocol(%d).String() = %q, want %q", tt.protocol, result, tt.expected)
			}
		})
	}
}

func TestProtocolConstants(t *testing.T) {
	// Ensure protocol constants have expected values
	if ProtocolTCP != 1 {
		t.Errorf("ProtocolTCP = %d, want 1", ProtocolTCP)
	}
	if ProtocolUDP != 2 {
		t.Errorf("ProtocolUDP = %d, want 2", ProtocolUDP)
	}
}

func TestDefaultTransportConfig(t *testing.T) {
	serverAddr := "example.com:443"
	uuid := "test-uuid-12345"

	config := DefaultTransportConfig(serverAddr, uuid)

	// Verify required fields
	if config.ServerAddr != serverAddr {
		t.Errorf("ServerAddr = %q, want %q", config.ServerAddr, serverAddr)
	}
	if config.UUID != uuid {
		t.Errorf("UUID = %q, want %q", config.UUID, uuid)
	}

	// Verify default values
	if config.SNI != "www.cloudflare.com" {
		t.Errorf("SNI = %q, want %q", config.SNI, "www.cloudflare.com")
	}
	if config.HandshakeTimeout != 30*time.Second {
		t.Errorf("HandshakeTimeout = %v, want %v", config.HandshakeTimeout, 30*time.Second)
	}
	if config.DialTimeout != 30*time.Second {
		t.Errorf("DialTimeout = %v, want %v", config.DialTimeout, 30*time.Second)
	}
	if config.RetryTimes != 3 {
		t.Errorf("RetryTimes = %d, want 3", config.RetryTimes)
	}
	if config.IdleTimeout != 60*time.Second {
		t.Errorf("IdleTimeout = %v, want %v", config.IdleTimeout, 60*time.Second)
	}
	if config.PingInterval != 30*time.Second {
		t.Errorf("PingInterval = %v, want %v", config.PingInterval, 30*time.Second)
	}
	if config.Dialer != nil {
		t.Error("Dialer should be nil by default")
	}
}

func TestHealthStatusFields(t *testing.T) {
	now := time.Now()
	status := &HealthStatus{
		Available:           true,
		RTT:                 100 * time.Millisecond,
		LastCheck:           now,
		ConsecutiveFailures: 0,
	}

	if !status.Available {
		t.Error("Available should be true")
	}
	if status.RTT != 100*time.Millisecond {
		t.Errorf("RTT = %v, want %v", status.RTT, 100*time.Millisecond)
	}
	if status.LastCheck != now {
		t.Errorf("LastCheck = %v, want %v", status.LastCheck, now)
	}
	if status.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0", status.ConsecutiveFailures)
	}
}

func TestTransportConfigFields(t *testing.T) {
	config := &TransportConfig{
		ServerAddr:       "server:8443",
		UUID:             "my-uuid",
		SNI:              "custom.sni.com",
		HandshakeTimeout: 10 * time.Second,
		DialTimeout:      15 * time.Second,
		RetryTimes:       5,
		IdleTimeout:      120 * time.Second,
		PingInterval:     60 * time.Second,
	}

	if config.ServerAddr != "server:8443" {
		t.Errorf("ServerAddr = %q, want %q", config.ServerAddr, "server:8443")
	}
	if config.UUID != "my-uuid" {
		t.Errorf("UUID = %q, want %q", config.UUID, "my-uuid")
	}
	if config.SNI != "custom.sni.com" {
		t.Errorf("SNI = %q, want %q", config.SNI, "custom.sni.com")
	}
	if config.HandshakeTimeout != 10*time.Second {
		t.Errorf("HandshakeTimeout = %v, want %v", config.HandshakeTimeout, 10*time.Second)
	}
	if config.DialTimeout != 15*time.Second {
		t.Errorf("DialTimeout = %v, want %v", config.DialTimeout, 15*time.Second)
	}
	if config.RetryTimes != 5 {
		t.Errorf("RetryTimes = %d, want 5", config.RetryTimes)
	}
	if config.IdleTimeout != 120*time.Second {
		t.Errorf("IdleTimeout = %v, want %v", config.IdleTimeout, 120*time.Second)
	}
	if config.PingInterval != 60*time.Second {
		t.Errorf("PingInterval = %v, want %v", config.PingInterval, 60*time.Second)
	}
}
