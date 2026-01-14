package shadowtlsplus

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// TCPConfig contains TCP transport configuration
type TCPConfig struct {
	*TransportConfig

	// SessionConfig for multiplexing
	SessionConfig *SessionConfig
}

// DefaultTCPConfig returns default TCP configuration
func DefaultTCPConfig(serverAddr, uuid string) *TCPConfig {
	return &TCPConfig{
		TransportConfig: DefaultTransportConfig(serverAddr, uuid),
		SessionConfig:   DefaultSessionConfig(true),
	}
}

// TCPTransport implements Transport interface over TCP
type TCPTransport struct {
	config  *TCPConfig
	mu      sync.RWMutex
	session *Session
	conn    net.Conn

	// Health tracking
	health        atomic.Pointer[HealthStatus]
	lastRTT       atomic.Int64 // nanoseconds
	failureCount  atomic.Int32
	connected     atomic.Bool
	rttSamples    []time.Duration
	rttSamplesMu  sync.Mutex
	maxRTTSamples int
}

// NewTCPTransport creates a new TCP transport
func NewTCPTransport(config *TCPConfig) (*TCPTransport, error) {
	if config.ServerAddr == "" {
		return nil, fmt.Errorf("transport: server address required")
	}
	if config.UUID == "" {
		return nil, fmt.Errorf("transport: UUID required")
	}

	t := &TCPTransport{
		config:        config,
		maxRTTSamples: 10,
	}

	// Initialize health status
	t.health.Store(&HealthStatus{
		Available: false,
		LastCheck: time.Now(),
	})

	return t, nil
}

// Protocol returns the transport protocol
func (t *TCPTransport) Protocol() Protocol {
	return ProtocolTCP
}

// IsConnected returns true if connected
func (t *TCPTransport) IsConnected() bool {
	return t.connected.Load()
}

// Health returns current health status
func (t *TCPTransport) Health() *HealthStatus {
	h := t.health.Load()
	if h == nil {
		return &HealthStatus{Available: false}
	}
	return h
}

// updateHealth updates health status
func (t *TCPTransport) updateHealth(available bool, rtt time.Duration) {
	failures := int(t.failureCount.Load())
	t.health.Store(&HealthStatus{
		Available:           available,
		RTT:                 rtt,
		LastCheck:           time.Now(),
		ConsecutiveFailures: failures,
	})
}

// recordRTT records an RTT sample
func (t *TCPTransport) recordRTT(rtt time.Duration) {
	t.lastRTT.Store(int64(rtt))

	t.rttSamplesMu.Lock()
	t.rttSamples = append(t.rttSamples, rtt)
	if len(t.rttSamples) > t.maxRTTSamples {
		t.rttSamples = t.rttSamples[1:]
	}
	t.rttSamplesMu.Unlock()
}

// averageRTT returns average RTT from samples
func (t *TCPTransport) averageRTT() time.Duration {
	t.rttSamplesMu.Lock()
	defer t.rttSamplesMu.Unlock()

	if len(t.rttSamples) == 0 {
		return 0
	}

	var total time.Duration
	for _, rtt := range t.rttSamples {
		total += rtt
	}
	return total / time.Duration(len(t.rttSamples))
}

// createHandshaker creates a new handshaker
func (t *TCPTransport) createHandshaker() (*ClientHandshaker, error) {
	handshakeConfig := &HandshakeConfig{
		UUID:    t.config.UUID,
		SNI:     t.config.SNI,
		Timeout: t.config.HandshakeTimeout,
	}
	return NewClientHandshaker(handshakeConfig)
}

// Connect establishes a connection to the server
func (t *TCPTransport) Connect(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if already connected
	if t.session != nil && !t.session.IsClosed() {
		return nil
	}

	retryTimes := t.config.RetryTimes
	if retryTimes <= 0 {
		retryTimes = 1
	}

	var lastErr error
	for i := 0; i < retryTimes; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		startTime := time.Now()
		if err := t.connectOnce(ctx); err != nil {
			lastErr = err
			t.failureCount.Add(1)
			t.updateHealth(false, 0)

			if i < retryTimes-1 {
				backoff := time.Duration(100<<i) * time.Millisecond
				if backoff > time.Second {
					backoff = time.Second
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
			}
			continue
		}

		// Success - record RTT and update health
		rtt := time.Since(startTime)
		t.recordRTT(rtt)
		t.failureCount.Store(0)
		t.connected.Store(true)
		t.updateHealth(true, t.averageRTT())
		return nil
	}

	return lastErr
}

// connectOnce attempts a single connection
func (t *TCPTransport) connectOnce(ctx context.Context) error {
	debugf("TCP dialing %s", t.config.ServerAddr)

	var conn net.Conn
	var err error

	if t.config.Dialer != nil {
		conn, err = t.config.Dialer.DialContext(ctx, "tcp", t.config.ServerAddr)
	} else {
		dialer := net.Dialer{Timeout: t.config.DialTimeout}
		conn, err = dialer.DialContext(ctx, "tcp", t.config.ServerAddr)
	}
	if err != nil {
		debugf("TCP dial failed: %v", err)
		return fmt.Errorf("transport: failed to dial server: %w", err)
	}

	debugf("TCP connected, starting handshake (sni=%s)", t.config.SNI)

	// Create handshaker
	handshaker, err := t.createHandshaker()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("transport: failed to create handshaker: %w", err)
	}

	// Perform handshake
	result, err := handshaker.Handshake(conn)
	if err != nil {
		_ = conn.Close()
		debugf("TCP handshake failed: %v", err)
		return fmt.Errorf("transport: handshake failed: %w", err)
	}

	debugf("TCP handshake succeeded")

	// Create crypto engine
	engine, err := NewCryptoEngine(result.Keys, true)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("transport: failed to create crypto engine: %w", err)
	}

	// Create session
	t.conn = conn
	session, err := NewClientSession(conn, engine, t.config.SessionConfig)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("transport: failed to create session: %w", err)
	}
	t.session = session

	debugf("TCP session established")
	return nil
}

// OpenStream opens a new stream to the target address
func (t *TCPTransport) OpenStream(network, address string) (net.Conn, error) {
	debugf("TCP opening stream: %s://%s", network, address)

	// Ensure connected
	ctx, cancel := context.WithTimeout(context.Background(), t.config.DialTimeout)
	defer cancel()

	if err := t.Connect(ctx); err != nil {
		t.failureCount.Add(1)
		t.updateHealth(false, 0)
		return nil, err
	}

	t.mu.RLock()
	session := t.session
	t.mu.RUnlock()

	if session == nil || session.IsClosed() {
		debugf("TCP session closed, reconnecting")
		t.connected.Store(false)
		// Try to reconnect
		if err := t.Connect(ctx); err != nil {
			t.failureCount.Add(1)
			t.updateHealth(false, 0)
			return nil, err
		}
		t.mu.RLock()
		session = t.session
		t.mu.RUnlock()
	}

	addr, err := ParseAddress(network, address)
	if err != nil {
		return nil, fmt.Errorf("transport: invalid address: %w", err)
	}

	stream, err := session.OpenStream(addr)
	if err != nil {
		debugf("TCP open stream failed: %v", err)
		t.failureCount.Add(1)
		t.updateHealth(false, t.averageRTT())
		return nil, err
	}

	// Reset failure count on success
	t.failureCount.Store(0)
	debugf("TCP stream opened: %s://%s", network, address)
	return stream, nil
}

// Close closes the transport
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.connected.Store(false)
	t.updateHealth(false, 0)

	if t.session != nil {
		_ = t.session.Close()
		t.session = nil
	}
	if t.conn != nil {
		_ = t.conn.Close()
		t.conn = nil
	}
	return nil
}

// Stats returns current session statistics
func (t *TCPTransport) Stats() SessionStats {
	t.mu.RLock()
	session := t.session
	t.mu.RUnlock()

	if session != nil {
		return session.Stats()
	}
	return SessionStats{}
}

// Session returns the underlying mux session (for advanced usage)
func (t *TCPTransport) Session() *Session {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.session
}
