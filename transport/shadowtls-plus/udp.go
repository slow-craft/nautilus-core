package shadowtlsplus

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtaci/kcp-go/v5"
)

// KCPMode represents KCP operation mode
type KCPMode string

const (
	// KCPModeFast is optimized for low latency (aggressive retransmission)
	KCPModeFast KCPMode = "fast"
	// KCPModeNormal is balanced between latency and bandwidth
	KCPModeNormal KCPMode = "normal"
	// KCPModeDefault is optimized for bandwidth efficiency
	KCPModeDefault KCPMode = "default"
)

// KCPConfig contains KCP-specific configuration
type KCPConfig struct {
	// Mode is the KCP operation mode: fast, normal, default
	Mode KCPMode

	// MTU is the maximum transmission unit (default: 1400)
	MTU int

	// SndWnd is the send window size (default: 1024)
	SndWnd int

	// RcvWnd is the receive window size (default: 1024)
	RcvWnd int

	// DataShard is the number of data shards for FEC (0 to disable)
	DataShard int

	// ParityShard is the number of parity shards for FEC (0 to disable)
	ParityShard int

	// NoDelay, Interval, Resend, NC are internal KCP parameters
	// These are auto-configured based on Mode
	NoDelay  int
	Interval int
	Resend   int
	NC       int
}

// DefaultKCPConfig returns default KCP configuration
func DefaultKCPConfig() *KCPConfig {
	return &KCPConfig{
		Mode:        KCPModeFast,
		MTU:         1400,
		SndWnd:      1024,
		RcvWnd:      1024,
		DataShard:   0, // FEC disabled by default
		ParityShard: 0,
	}
}

// applyMode applies KCP parameters based on mode
func (c *KCPConfig) applyMode() {
	switch c.Mode {
	case KCPModeFast:
		// Fast mode: aggressive retransmission, low latency
		// NoDelay=1, Interval=10ms, Resend=2, NC=1
		c.NoDelay = 1
		c.Interval = 10
		c.Resend = 2
		c.NC = 1
	case KCPModeNormal:
		// Normal mode: balanced
		// NoDelay=0, Interval=30ms, Resend=2, NC=1
		c.NoDelay = 0
		c.Interval = 30
		c.Resend = 2
		c.NC = 1
	default:
		// Default mode: bandwidth efficient
		// NoDelay=0, Interval=40ms, Resend=0, NC=0
		c.NoDelay = 0
		c.Interval = 40
		c.Resend = 0
		c.NC = 0
	}
}

// UDPConfig contains UDP transport configuration
type UDPConfig struct {
	*TransportConfig

	// KCP is the KCP-specific configuration
	KCP *KCPConfig

	// SessionConfig for multiplexing
	SessionConfig *SessionConfig
}

// DefaultUDPConfig returns default UDP configuration
func DefaultUDPConfig(serverAddr, uuid string) *UDPConfig {
	return &UDPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr:       serverAddr,
			UUID:             uuid,
			SNI:              "www.cloudflare.com",
			HandshakeTimeout: 5 * time.Second,  // Short timeout for fast failover
			DialTimeout:      5 * time.Second,  // Short timeout for fast failover
			RetryTimes:       1,                // Single attempt for fast failover
			IdleTimeout:      60 * time.Second,
			PingInterval:     30 * time.Second,
		},
		KCP:           DefaultKCPConfig(),
		SessionConfig: DefaultSessionConfig(true),
	}
}

// UDPTransport implements Transport interface over UDP with KCP
type UDPTransport struct {
	config     *UDPConfig
	mu         sync.RWMutex
	kcpSession *kcp.UDPSession
	muxSession *Session

	// Health tracking
	health        atomic.Pointer[HealthStatus]
	lastRTT       atomic.Int64 // nanoseconds
	failureCount  atomic.Int32
	connected     atomic.Bool
	rttSamples    []time.Duration
	rttSamplesMu  sync.Mutex
	maxRTTSamples int
}

// NewUDPTransport creates a new UDP transport with KCP
func NewUDPTransport(config *UDPConfig) (*UDPTransport, error) {
	if config.ServerAddr == "" {
		return nil, fmt.Errorf("transport: server address required")
	}
	if config.UUID == "" {
		return nil, fmt.Errorf("transport: UUID required")
	}

	// Apply KCP mode parameters
	if config.KCP != nil {
		config.KCP.applyMode()
	}

	t := &UDPTransport{
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
func (t *UDPTransport) Protocol() Protocol {
	return ProtocolUDP
}

// IsConnected returns true if connected
func (t *UDPTransport) IsConnected() bool {
	return t.connected.Load()
}

// Health returns current health status
func (t *UDPTransport) Health() *HealthStatus {
	h := t.health.Load()
	if h == nil {
		return &HealthStatus{Available: false}
	}
	return h
}

// updateHealth updates health status
func (t *UDPTransport) updateHealth(available bool, rtt time.Duration) {
	failures := int(t.failureCount.Load())
	t.health.Store(&HealthStatus{
		Available:           available,
		RTT:                 rtt,
		LastCheck:           time.Now(),
		ConsecutiveFailures: failures,
	})
}

// recordRTT records an RTT sample
func (t *UDPTransport) recordRTT(rtt time.Duration) {
	t.lastRTT.Store(int64(rtt))

	t.rttSamplesMu.Lock()
	t.rttSamples = append(t.rttSamples, rtt)
	if len(t.rttSamples) > t.maxRTTSamples {
		t.rttSamples = t.rttSamples[1:]
	}
	t.rttSamplesMu.Unlock()
}

// averageRTT returns average RTT from samples
func (t *UDPTransport) averageRTT() time.Duration {
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
func (t *UDPTransport) createHandshaker() (*ClientHandshaker, error) {
	handshakeConfig := &HandshakeConfig{
		UUID:    t.config.UUID,
		SNI:     t.config.SNI,
		Timeout: t.config.HandshakeTimeout,
	}
	return NewClientHandshaker(handshakeConfig)
}

// configureKCPSession configures KCP session parameters
func (t *UDPTransport) configureKCPSession(session *kcp.UDPSession) {
	kcpCfg := t.config.KCP
	if kcpCfg == nil {
		kcpCfg = DefaultKCPConfig()
		kcpCfg.applyMode()
	}

	// Set KCP parameters
	session.SetMtu(kcpCfg.MTU)
	session.SetWindowSize(kcpCfg.SndWnd, kcpCfg.RcvWnd)
	session.SetNoDelay(kcpCfg.NoDelay, kcpCfg.Interval, kcpCfg.Resend, kcpCfg.NC)

	// Set read/write buffer size
	_ = session.SetReadBuffer(4 * 1024 * 1024)  // 4MB
	_ = session.SetWriteBuffer(4 * 1024 * 1024) // 4MB
}

// Connect establishes a connection to the server
func (t *UDPTransport) Connect(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if already connected
	if t.muxSession != nil && !t.muxSession.IsClosed() {
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
func (t *UDPTransport) connectOnce(ctx context.Context) error {
	// Create KCP session (dial with FEC if configured)
	kcpCfg := t.config.KCP
	if kcpCfg == nil {
		kcpCfg = DefaultKCPConfig()
	}

	var kcpSession *kcp.UDPSession
	var err error

	if kcpCfg.DataShard > 0 && kcpCfg.ParityShard > 0 {
		// With FEC
		kcpSession, err = kcp.DialWithOptions(
			t.config.ServerAddr,
			nil, // no block cipher (we use our own encryption)
			kcpCfg.DataShard,
			kcpCfg.ParityShard,
		)
	} else {
		// Without FEC
		conn, dialErr := kcp.Dial(t.config.ServerAddr)
		if dialErr != nil {
			err = dialErr
		} else {
			kcpSession = conn.(*kcp.UDPSession)
		}
	}

	if err != nil {
		return fmt.Errorf("transport: failed to dial KCP server: %w", err)
	}

	// Configure KCP session
	t.configureKCPSession(kcpSession)

	// Set deadline for handshake
	_ = kcpSession.SetDeadline(time.Now().Add(t.config.HandshakeTimeout))

	// Create handshaker
	handshaker, err := t.createHandshaker()
	if err != nil {
		_ = kcpSession.Close()
		return fmt.Errorf("transport: failed to create handshaker: %w", err)
	}

	// Perform handshake (reuse existing handshake logic)
	result, err := handshaker.Handshake(kcpSession)
	if err != nil {
		_ = kcpSession.Close()
		return fmt.Errorf("transport: handshake failed: %w", err)
	}

	// Clear deadline after handshake
	_ = kcpSession.SetDeadline(time.Time{})

	// Create crypto engine
	engine, err := NewCryptoEngine(result.Keys, true)
	if err != nil {
		_ = kcpSession.Close()
		return fmt.Errorf("transport: failed to create crypto engine: %w", err)
	}

	// Create mux session on top of encrypted KCP
	t.kcpSession = kcpSession
	muxSession, err := NewClientSession(kcpSession, engine, t.config.SessionConfig)
	if err != nil {
		_ = kcpSession.Close()
		return fmt.Errorf("transport: failed to create session: %w", err)
	}
	t.muxSession = muxSession

	return nil
}

// OpenStream opens a new stream to the target address
func (t *UDPTransport) OpenStream(network, address string) (net.Conn, error) {
	// Ensure connected
	ctx, cancel := context.WithTimeout(context.Background(), t.config.DialTimeout)
	defer cancel()

	if err := t.Connect(ctx); err != nil {
		t.failureCount.Add(1)
		t.updateHealth(false, 0)
		return nil, err
	}

	t.mu.RLock()
	session := t.muxSession
	t.mu.RUnlock()

	if session == nil || session.IsClosed() {
		t.connected.Store(false)
		// Try to reconnect
		if err := t.Connect(ctx); err != nil {
			t.failureCount.Add(1)
			t.updateHealth(false, 0)
			return nil, err
		}
		t.mu.RLock()
		session = t.muxSession
		t.mu.RUnlock()
	}

	addr, err := ParseAddress(network, address)
	if err != nil {
		return nil, fmt.Errorf("transport: invalid address: %w", err)
	}

	stream, err := session.OpenStream(addr)
	if err != nil {
		t.failureCount.Add(1)
		t.updateHealth(false, t.averageRTT())
		return nil, err
	}

	// Reset failure count on success
	t.failureCount.Store(0)
	return stream, nil
}

// Close closes the transport
func (t *UDPTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.connected.Store(false)
	t.updateHealth(false, 0)

	if t.muxSession != nil {
		_ = t.muxSession.Close()
		t.muxSession = nil
	}
	if t.kcpSession != nil {
		_ = t.kcpSession.Close()
		t.kcpSession = nil
	}
	return nil
}

// Stats returns current session statistics
func (t *UDPTransport) Stats() SessionStats {
	t.mu.RLock()
	session := t.muxSession
	t.mu.RUnlock()

	if session != nil {
		return session.Stats()
	}
	return SessionStats{}
}

// Session returns the underlying mux session (for advanced usage)
func (t *UDPTransport) Session() *Session {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.muxSession
}
