package shadowtlsplus

import (
	"context"
	"net"
	"time"
)

// Protocol represents the transport protocol type
type Protocol uint8

const (
	// ProtocolTCP represents TCP transport
	ProtocolTCP Protocol = 1
	// ProtocolUDP represents UDP transport (over KCP)
	ProtocolUDP Protocol = 2
)

// String returns the protocol name
func (p Protocol) String() string {
	switch p {
	case ProtocolTCP:
		return "TCP"
	case ProtocolUDP:
		return "UDP"
	default:
		return "UNKNOWN"
	}
}

// HealthStatus represents the health status of a transport
type HealthStatus struct {
	// Available indicates if the transport is available
	Available bool

	// RTT is the round-trip time
	RTT time.Duration

	// LastCheck is the last health check time
	LastCheck time.Time

	// ConsecutiveFailures is the number of consecutive failures
	ConsecutiveFailures int
}

// Transport defines the interface for transport implementations
type Transport interface {
	// Connect establishes a connection to the server
	Connect(ctx context.Context) error

	// OpenStream opens a new stream to the target address
	// network is "tcp" or "udp", address is "host:port"
	OpenStream(network, address string) (net.Conn, error)

	// Close closes the transport and all associated connections
	Close() error

	// Health returns the current health status
	Health() *HealthStatus

	// Protocol returns the transport protocol type
	Protocol() Protocol

	// IsConnected returns true if the transport is connected
	IsConnected() bool
}

// TransportConfig contains common transport configuration
type TransportConfig struct {
	// ServerAddr is the server address (host:port)
	ServerAddr string

	// UUID for authentication
	UUID string

	// SNI for TLS handshake camouflage
	SNI string

	// HandshakeTimeout is the timeout for handshake
	HandshakeTimeout time.Duration

	// DialTimeout is the timeout for dialing
	DialTimeout time.Duration

	// RetryTimes is the number of retry attempts
	RetryTimes int

	// IdleTimeout is the idle timeout for connections
	IdleTimeout time.Duration

	// PingInterval is the interval for keepalive pings
	PingInterval time.Duration

	// Dialer is an optional custom dialer
	Dialer ContextDialer
}

// DefaultTransportConfig returns default transport configuration
func DefaultTransportConfig(serverAddr, uuid string) *TransportConfig {
	return &TransportConfig{
		ServerAddr:       serverAddr,
		UUID:             uuid,
		SNI:              "www.cloudflare.com",
		HandshakeTimeout: 30 * time.Second,
		DialTimeout:      30 * time.Second,
		RetryTimes:       3,
		IdleTimeout:      60 * time.Second,
		PingInterval:     30 * time.Second,
	}
}

// ContextDialer is an interface for dialers that support context
type ContextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}
