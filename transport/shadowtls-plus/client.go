package shadowtlsplus

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// ClientConfig contains client configuration
type ClientConfig struct {
	ServerAddr string
	UUID       string

	SNI              string
	HandshakeTimeout time.Duration
	IdleTimeout      time.Duration
	DialTimeout      time.Duration
	RetryTimes       int
	SessionConfig    *SessionConfig

	// Dialer is an optional custom dialer
	Dialer ContextDialer

	// UDP configuration for dual-stack
	UDP *UDPClientConfig
}

// UDPClientConfig contains UDP-specific client configuration
type UDPClientConfig struct {
	// Enabled enables UDP transport (default: false for backward compatibility)
	Enabled bool

	// DialTimeout is the UDP dial timeout (default: 5s for fast failover)
	DialTimeout time.Duration

	// HandshakeTimeout is the UDP handshake timeout (default: 5s for fast failover)
	HandshakeTimeout time.Duration

	// IdleTimeout is the UDP idle timeout (default: uses TCP's idle timeout if not set)
	IdleTimeout time.Duration

	// RetryTimes is the number of UDP retry attempts (default: 1 for fast failover)
	RetryTimes int

	// KCP contains KCP protocol configuration
	KCP *KCPConfig

	// Strategy contains dual-stack strategy configuration
	Strategy *StrategyConfig
}

// StrategyConfig contains dual-stack strategy configuration
type StrategyConfig struct {
	// Primary is the preferred protocol: auto/tcp/udp
	Primary string

	// RTTThreshold is the RTT threshold for switching
	RTTThreshold time.Duration

	// FailureThreshold is the number of consecutive failures before switching
	FailureThreshold int

	// RecoveryInterval is the time to wait before retrying failed transport
	RecoveryInterval time.Duration

	// WarmupBoth enables connecting both transports at startup
	WarmupBoth bool

	// HealthCheckInterval is the interval for health checks
	HealthCheckInterval time.Duration
}

// DefaultClientConfig returns a default client configuration
func DefaultClientConfig(serverAddr, uuid string) *ClientConfig {
	return &ClientConfig{
		ServerAddr:       serverAddr,
		UUID:             uuid,
		SNI:              "www.cloudflare.com",
		HandshakeTimeout: 30 * time.Second,
		IdleTimeout:      60 * time.Second,
		DialTimeout:      30 * time.Second,
		RetryTimes:       3,
		SessionConfig:    DefaultSessionConfig(true),
	}
}

// DefaultUDPClientConfig returns default UDP client configuration
func DefaultUDPClientConfig() *UDPClientConfig {
	return &UDPClientConfig{
		Enabled:          false,
		DialTimeout:      5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
		RetryTimes:       1,
		KCP:              DefaultKCPConfig(),
		Strategy:         DefaultStrategyConfig(),
	}
}

// DefaultStrategyConfig returns default strategy configuration
func DefaultStrategyConfig() *StrategyConfig {
	return &StrategyConfig{
		Primary:             "auto",
		RTTThreshold:        500 * time.Millisecond,
		FailureThreshold:    3,
		RecoveryInterval:    30 * time.Second,
		WarmupBoth:          true,
		HealthCheckInterval: 30 * time.Second,
	}
}

// Client is the STP client that provides proxy functionality
// It supports both single TCP mode and dual-stack TCP/UDP mode
type Client struct {
	config *ClientConfig
	mu     sync.Mutex

	// Single mode (TCP only)
	session *Session
	conn    net.Conn

	// Dual-stack mode
	manager     *TransportManager
	dualEnabled bool
}

// NewClient creates a new STP client
func NewClient(config *ClientConfig) (*Client, error) {
	if config.ServerAddr == "" {
		return nil, fmt.Errorf("client: server address required")
	}
	if config.UUID == "" {
		return nil, fmt.Errorf("client: UUID required")
	}

	client := &Client{
		config: config,
	}

	// Check if dual-stack is enabled
	if config.UDP != nil && config.UDP.Enabled {
		client.dualEnabled = true
		debugf("client created: server=%s, mode=dual-stack, strategy=%s",
			config.ServerAddr, config.UDP.Strategy.Primary)
	} else {
		debugf("client created: server=%s, mode=tcp-only", config.ServerAddr)
	}

	return client, nil
}

// Connect establishes a connection to the STP server
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.dualEnabled {
		return c.connectDualStack(ctx)
	}
	return c.connectTCPOnly(ctx)
}

// connectTCPOnly connects using TCP only (backward compatible)
func (c *Client) connectTCPOnly(ctx context.Context) error {
	if c.session != nil && !c.session.IsClosed() {
		return nil
	}

	retryTimes := c.config.RetryTimes
	if retryTimes <= 0 {
		retryTimes = 1
	}

	var lastErr error
	for i := 0; i < retryTimes; i++ {
		if err := c.connectOnce(ctx); err != nil {
			lastErr = err
			if i < retryTimes-1 {
				backoff := time.Duration(100<<i) * time.Millisecond
				if backoff > time.Second {
					backoff = time.Second
				}
				time.Sleep(backoff)
			}
			continue
		}
		return nil
	}
	return lastErr
}

func (c *Client) connectOnce(ctx context.Context) error {
	sni := c.config.SNI
	if sni == "" {
		sni = "www.cloudflare.com"
	}

	debugf("TCP connecting to %s (sni=%s)", c.config.ServerAddr, sni)

	var conn net.Conn
	var err error

	if c.config.Dialer != nil {
		conn, err = c.config.Dialer.DialContext(ctx, "tcp", c.config.ServerAddr)
	} else {
		dialer := net.Dialer{Timeout: c.config.DialTimeout}
		conn, err = dialer.DialContext(ctx, "tcp", c.config.ServerAddr)
	}
	if err != nil {
		debugf("TCP dial failed: %v", err)
		return fmt.Errorf("transport: failed to dial server: %w", err)
	}

	debugf("TCP connected, starting handshake")

	handshakeConfig := &HandshakeConfig{
		UUID:    c.config.UUID,
		SNI:     sni,
		Timeout: c.config.HandshakeTimeout,
	}

	handshaker, err := NewClientHandshaker(handshakeConfig)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("transport: failed to create handshaker: %w", err)
	}

	result, err := handshaker.Handshake(conn)
	if err != nil {
		_ = conn.Close()
		debugf("TCP handshake failed: %v", err)
		return fmt.Errorf("transport: handshake failed: %w", err)
	}

	debugf("TCP handshake succeeded")

	engine, err := NewCryptoEngine(result.Keys, true)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("transport: failed to create crypto engine: %w", err)
	}

	c.conn = conn
	session, err := NewClientSession(conn, engine, c.config.SessionConfig)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("transport: failed to create session: %w", err)
	}
	c.session = session

	debugf("TCP session established")
	return nil
}

// connectDualStack connects using both TCP and UDP
func (c *Client) connectDualStack(ctx context.Context) error {
	if c.manager != nil {
		// Already initialized
		return nil
	}

	// Create TCP transport
	tcpConfig := &TCPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr:       c.config.ServerAddr,
			UUID:             c.config.UUID,
			SNI:              c.config.SNI,
			HandshakeTimeout: c.config.HandshakeTimeout,
			DialTimeout:      c.config.DialTimeout,
			RetryTimes:       c.config.RetryTimes,
			IdleTimeout:      c.config.IdleTimeout,
			PingInterval:     30 * time.Second,
			Dialer:           c.config.Dialer,
		},
		SessionConfig: c.config.SessionConfig,
	}

	tcpTransport, err := NewTCPTransport(tcpConfig)
	if err != nil {
		return fmt.Errorf("client: failed to create TCP transport: %w", err)
	}

	// Create UDP transport
	udpCfg := c.config.UDP
	if udpCfg == nil {
		udpCfg = DefaultUDPClientConfig()
	}

	// Use UDP-specific idle timeout if set, otherwise fall back to TCP's
	udpIdleTimeout := udpCfg.IdleTimeout
	if udpIdleTimeout <= 0 {
		udpIdleTimeout = c.config.IdleTimeout
	}

	udpConfig := &UDPConfig{
		TransportConfig: &TransportConfig{
			ServerAddr:       c.config.ServerAddr,
			UUID:             c.config.UUID,
			SNI:              c.config.SNI,
			HandshakeTimeout: udpCfg.HandshakeTimeout,
			DialTimeout:      udpCfg.DialTimeout,
			RetryTimes:       udpCfg.RetryTimes,
			IdleTimeout:      udpIdleTimeout,
			PingInterval:     30 * time.Second,
		},
		KCP:           udpCfg.KCP,
		SessionConfig: c.config.SessionConfig,
	}

	// Pass dialer for TUN mode compatibility if it implements UDPDialer
	if udpDialer, ok := c.config.Dialer.(UDPDialer); ok {
		udpConfig.Dialer = udpDialer
	}

	udpTransport, err := NewUDPTransport(udpConfig)
	if err != nil {
		_ = tcpTransport.Close()
		return fmt.Errorf("client: failed to create UDP transport: %w", err)
	}

	// Create manager config from strategy
	strategy := udpCfg.Strategy
	if strategy == nil {
		strategy = DefaultStrategyConfig()
	}

	managerConfig := &ManagerConfig{
		SelectionMode:       parseSelectionMode(strategy.Primary),
		RTTThreshold:        strategy.RTTThreshold,
		FailureThreshold:    strategy.FailureThreshold,
		RecoveryInterval:    strategy.RecoveryInterval,
		WarmupBoth:          strategy.WarmupBoth,
		HealthCheckInterval: strategy.HealthCheckInterval,
	}

	// Create transport manager
	manager, err := NewTransportManager(tcpTransport, udpTransport, managerConfig)
	if err != nil {
		_ = tcpTransport.Close()
		_ = udpTransport.Close()
		return fmt.Errorf("client: failed to create transport manager: %w", err)
	}

	// Start the manager
	if err := manager.Start(ctx); err != nil {
		_ = manager.Close()
		return fmt.Errorf("client: failed to start transport manager: %w", err)
	}

	c.manager = manager
	return nil
}

// parseSelectionMode converts string to SelectionMode
func parseSelectionMode(mode string) SelectionMode {
	switch mode {
	case "tcp":
		return SelectionModeTCPFirst
	case "udp":
		return SelectionModeUDPFirst
	default:
		return SelectionModeAuto
	}
}

// OpenStream opens a new stream to the target address
func (c *Client) OpenStream(ctx context.Context, network, address string) (*Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.dualEnabled {
		return c.openStreamDualStack(ctx, network, address)
	}
	return c.openStreamTCPOnly(ctx, network, address)
}

// openStreamTCPOnly opens stream using TCP only
func (c *Client) openStreamTCPOnly(ctx context.Context, network, address string) (*Stream, error) {
	if err := c.connectTCPOnly(ctx); err != nil {
		return nil, err
	}

	session := c.session
	if session == nil || session.IsClosed() {
		if err := c.connectTCPOnly(ctx); err != nil {
			return nil, err
		}
		session = c.session
	}

	addr, err := ParseAddress(network, address)
	if err != nil {
		return nil, fmt.Errorf("transport: invalid address: %w", err)
	}

	return session.OpenStream(addr)
}

// openStreamDualStack opens stream using dual-stack manager
func (c *Client) openStreamDualStack(ctx context.Context, network, address string) (*Stream, error) {
	if err := c.connectDualStack(ctx); err != nil {
		return nil, err
	}

	conn, err := c.manager.OpenStream(network, address)
	if err != nil {
		return nil, err
	}

	// The manager returns net.Conn which is actually *Stream
	stream, ok := conn.(*Stream)
	if !ok {
		// Wrap it if needed
		return nil, fmt.Errorf("transport: unexpected connection type")
	}

	return stream, nil
}

// DialContext establishes a connection to the target through the STP tunnel
// For TCP: Returns a TCPStreamConn wrapper that reads the server response on first Read()
// For UDP: Returns the stream directly (no stream response for UDP)
func (c *Client) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	debugf("DialContext: network=%s, address=%s, dual-stack=%v", network, address, c.dualEnabled)

	if c.dualEnabled {
		return c.dialContextDualStack(ctx, network, address)
	}
	return c.dialContextTCPOnly(ctx, network, address)
}

// dialContextTCPOnly dials using TCP only
func (c *Client) dialContextTCPOnly(ctx context.Context, network, address string) (net.Conn, error) {
	stream, err := c.openStreamTCPOnly(ctx, network, address)
	if err != nil {
		return nil, err
	}

	if c.config.IdleTimeout > 0 {
		if err := stream.SetDeadline(time.Now().Add(c.config.IdleTimeout)); err != nil {
			_ = stream.Close()
			return nil, fmt.Errorf("client: failed to set deadline: %w", err)
		}
	}

	// For TCP: wrap with TCPStreamConn for delayed response reading
	// For UDP: return stream directly (no stream response)
	if network == "tcp" {
		return NewTCPStreamConn(stream), nil
	}
	return stream, nil
}

// dialContextDualStack dials using dual-stack manager
func (c *Client) dialContextDualStack(ctx context.Context, network, address string) (net.Conn, error) {
	if err := c.connectDualStack(ctx); err != nil {
		return nil, err
	}

	conn, err := c.manager.OpenStream(network, address)
	if err != nil {
		return nil, err
	}

	if c.config.IdleTimeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(c.config.IdleTimeout)); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("client: failed to set deadline: %w", err)
		}
	}

	// For TCP: wrap with TCPStreamConn for delayed response reading
	// For UDP: return stream directly (no stream response)
	if network == "tcp" {
		if stream, ok := conn.(*Stream); ok {
			return NewTCPStreamConn(stream), nil
		}
	}
	return conn, nil
}

// Stats returns the current connection statistics
func (c *Client) Stats() SessionStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.dualEnabled && c.manager != nil {
		// For dual-stack, return combined stats or TCP stats
		// Currently we just return empty stats, could be enhanced
		return SessionStats{}
	}

	if c.session != nil {
		return c.session.Stats()
	}
	return SessionStats{}
}

// Close closes the client and all connections
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.dualEnabled && c.manager != nil {
		return c.manager.Close()
	}

	if c.session != nil {
		_ = c.session.Close()
		c.session = nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	return nil
}

// IsClosed returns whether the client session is closed
func (c *Client) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.dualEnabled && c.manager != nil {
		return c.manager.closed.Load()
	}

	return c.session == nil || c.session.IsClosed()
}
