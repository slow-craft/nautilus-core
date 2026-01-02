package shadowtlsplus

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Client is the STP client that provides proxy functionality
type Client struct {
	config  *ClientConfig
	mu      sync.Mutex
	session *Session
	conn    net.Conn

	sniIndex atomic.Uint64
}

// ClientConfig contains client configuration
type ClientConfig struct {
	ServerAddr string
	Password   string

	SNIList           []string
	SNIRotateMode     string // "random" or "round_robin"
	SNIRotateFreq     string // "per_connection" or "per_minute"
	HandshakeTimeout  time.Duration
	IdleTimeout       time.Duration
	DialTimeout       time.Duration
	RetryTimes        int
	SessionConfig     *SessionConfig

	// Dialer is an optional custom dialer
	Dialer ContextDialer
}

// ContextDialer is an interface for dialers that support context
type ContextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// DefaultClientConfig returns a default client configuration
func DefaultClientConfig(serverAddr, password string) *ClientConfig {
	return &ClientConfig{
		ServerAddr: serverAddr,
		Password:   password,
		SNIList: []string{
			"time.cloudflare.com",
			"shopify.com",
			"time.is",
			"icook.hk",
			"www.visa.com",
			"www.digitalocean.com",
		},
		SNIRotateMode:    "random",
		SNIRotateFreq:    "per_connection",
		HandshakeTimeout: 30 * time.Second,
		IdleTimeout:      60 * time.Second,
		DialTimeout:      30 * time.Second,
		RetryTimes:       3,
		SessionConfig:    DefaultSessionConfig(true),
	}
}

// NewClient creates a new STP client
func NewClient(config *ClientConfig) (*Client, error) {
	if config.ServerAddr == "" {
		return nil, fmt.Errorf("client: server address required")
	}
	if config.Password == "" {
		return nil, fmt.Errorf("client: password required")
	}
	if len(config.SNIList) == 0 {
		return nil, fmt.Errorf("client: SNI list required")
	}

	return &Client{
		config: config,
	}, nil
}

// getSNI returns the current SNI value based on rotation mode
func (c *Client) getSNI() string {
	if len(c.config.SNIList) == 0 {
		return "www.cloudflare.com"
	}

	if c.config.SNIRotateMode == "round_robin" {
		idx := c.sniIndex.Add(1) - 1
		return c.config.SNIList[idx%uint64(len(c.config.SNIList))]
	}

	// random mode
	return c.config.SNIList[rand.Intn(len(c.config.SNIList))]
}

// Connect establishes a connection to the STP server
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

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
	sni := c.getSNI()

	var conn net.Conn
	var err error

	if c.config.Dialer != nil {
		conn, err = c.config.Dialer.DialContext(ctx, "tcp", c.config.ServerAddr)
	} else {
		dialer := net.Dialer{Timeout: c.config.DialTimeout}
		conn, err = dialer.DialContext(ctx, "tcp", c.config.ServerAddr)
	}
	if err != nil {
		return fmt.Errorf("transport: failed to dial server: %w", err)
	}

	handshakeConfig := &HandshakeConfig{
		Password: c.config.Password,
		SNI:      sni,
		Timeout:  c.config.HandshakeTimeout,
	}

	handshaker, err := NewClientHandshaker(handshakeConfig)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("transport: failed to create handshaker: %w", err)
	}

	result, err := handshaker.Handshake(conn)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("transport: handshake failed: %w", err)
	}

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

	return nil
}

// OpenStream opens a new stream to the target address
func (c *Client) OpenStream(ctx context.Context, network, address string) (*Stream, error) {
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}

	c.mu.Lock()
	session := c.session
	c.mu.Unlock()

	if session == nil || session.IsClosed() {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
		c.mu.Lock()
		session = c.session
		c.mu.Unlock()
	}

	addr, err := ParseAddress(network, address)
	if err != nil {
		return nil, fmt.Errorf("transport: invalid address: %w", err)
	}

	return session.OpenStream(addr)
}

// DialContext establishes a connection to the target through the STP tunnel
func (c *Client) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	stream, err := c.OpenStream(ctx, network, address)
	if err != nil {
		return nil, err
	}

	if c.config.IdleTimeout > 0 {
		if err := stream.SetDeadline(time.Now().Add(c.config.IdleTimeout)); err != nil {
			_ = stream.Close()
			return nil, fmt.Errorf("client: failed to set deadline: %w", err)
		}
	}

	return stream, nil
}

// Stats returns the current connection statistics
func (c *Client) Stats() SessionStats {
	c.mu.Lock()
	session := c.session
	c.mu.Unlock()

	if session != nil {
		return session.Stats()
	}
	return SessionStats{}
}

// Close closes the client and all connections
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

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
	session := c.session
	c.mu.Unlock()

	return session == nil || session.IsClosed()
}
