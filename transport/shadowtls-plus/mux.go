package shadowtlsplus

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/smux"
)

// Mux constants
const (
	DefaultWindowSize    = 64 * 1024
	MaxWindowSize        = 1024 * 1024
	MinWindowSize        = 16 * 1024
	DefaultIdleTimeout   = 60 * time.Second
	DefaultPingInterval  = 30 * time.Second
	MaxStreamsPerSession = 1024
)

// Protocol constants
const (
	NetworkTCP     = 0x01
	NetworkUDP     = 0x02
	AddrTypeIPv4   = 0x01
	AddrTypeIPv6   = 0x02
	AddrTypeDomain = 0x03
)

// Mux errors
var (
	ErrSessionClosed = errors.New("mux: session closed")
	ErrStreamClosed  = errors.New("mux: stream closed")
)

// SessionConfig contains configuration for a multiplexed session
type SessionConfig struct {
	InitialWindowSize int
	IdleTimeout       time.Duration
	PingInterval      time.Duration
	MaxStreams        int
	IsClient          bool
}

// DefaultSessionConfig returns default session configuration
func DefaultSessionConfig(isClient bool) *SessionConfig {
	return &SessionConfig{
		InitialWindowSize: DefaultWindowSize,
		IdleTimeout:       DefaultIdleTimeout,
		PingInterval:      DefaultPingInterval,
		MaxStreams:        MaxStreamsPerSession,
		IsClient:          isClient,
	}
}

// SessionStats contains statistics for a session
type SessionStats struct {
	mu           sync.RWMutex
	StreamCount  int
	ActiveCount  int
	TotalBytesTx int64
	TotalBytesRx int64
	PingCount    int
	CreatedAt    time.Time
}

// NewSessionStats creates new session statistics
func NewSessionStats() *SessionStats {
	return &SessionStats{
		CreatedAt: time.Now(),
	}
}

// AddStream increments stream count
func (s *SessionStats) AddStream() {
	s.mu.Lock()
	s.StreamCount++
	s.ActiveCount++
	s.mu.Unlock()
}

// RemoveStream decrements active count
func (s *SessionStats) RemoveStream() {
	s.mu.Lock()
	if s.ActiveCount > 0 {
		s.ActiveCount--
	}
	s.mu.Unlock()
}

// AddBytesTx adds transmitted bytes
func (s *SessionStats) AddBytesTx(n int64) {
	s.mu.Lock()
	s.TotalBytesTx += n
	s.mu.Unlock()
}

// AddBytesRx adds received bytes
func (s *SessionStats) AddBytesRx(n int64) {
	s.mu.Lock()
	s.TotalBytesRx += n
	s.mu.Unlock()
}

// Snapshot returns a copy of current stats
func (s *SessionStats) Snapshot() SessionStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return SessionStats{
		StreamCount:  s.StreamCount,
		ActiveCount:  s.ActiveCount,
		TotalBytesTx: s.TotalBytesTx,
		TotalBytesRx: s.TotalBytesRx,
		PingCount:    s.PingCount,
		CreatedAt:    s.CreatedAt,
	}
}

// Address represents a network address
type Address struct {
	Network  byte
	AddrType byte
	Host     string
	Port     uint16
}

// ParseAddress parses a network address string
func ParseAddress(network, addr string) (*Address, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, err
	}

	a := &Address{
		Host: host,
		Port: uint16(port),
	}

	switch network {
	case "tcp", "tcp4", "tcp6", "":
		a.Network = NetworkTCP
	case "udp", "udp4", "udp6":
		a.Network = NetworkUDP
	default:
		return nil, errors.New("unsupported network: " + network)
	}

	ip := net.ParseIP(host)
	if ip == nil {
		a.AddrType = AddrTypeDomain
	} else if ip4 := ip.To4(); ip4 != nil {
		a.AddrType = AddrTypeIPv4
		a.Host = ip4.String()
	} else {
		a.AddrType = AddrTypeIPv6
		a.Host = ip.String()
	}

	return a, nil
}

// String returns the address as a string
func (a *Address) String() string {
	return net.JoinHostPort(a.Host, strconv.Itoa(int(a.Port)))
}

// NetworkString returns the network type as a string
func (a *Address) NetworkString() string {
	if a.Network == NetworkUDP {
		return "udp"
	}
	return "tcp"
}

// Marshal serializes the address to bytes
func (a *Address) Marshal() []byte {
	var addrBytes []byte

	switch a.AddrType {
	case AddrTypeIPv4:
		ip := net.ParseIP(a.Host).To4()
		addrBytes = ip
	case AddrTypeIPv6:
		ip := net.ParseIP(a.Host).To16()
		addrBytes = ip
	case AddrTypeDomain:
		addrBytes = make([]byte, 1+len(a.Host))
		addrBytes[0] = byte(len(a.Host))
		copy(addrBytes[1:], a.Host)
	}

	result := make([]byte, 4+len(addrBytes))
	result[0] = a.Network
	result[1] = a.AddrType
	binary.BigEndian.PutUint16(result[2:4], a.Port)
	copy(result[4:], addrBytes)

	return result
}

// UnmarshalAddress deserializes an address from bytes
func UnmarshalAddress(data []byte) (*Address, int, error) {
	if len(data) < 4 {
		return nil, 0, errors.New("address data too short")
	}

	a := &Address{
		Network:  data[0],
		AddrType: data[1],
		Port:     binary.BigEndian.Uint16(data[2:4]),
	}

	offset := 4
	switch a.AddrType {
	case AddrTypeIPv4:
		if len(data) < offset+4 {
			return nil, 0, errors.New("address data too short for IPv4")
		}
		a.Host = net.IP(data[offset : offset+4]).String()
		offset += 4

	case AddrTypeIPv6:
		if len(data) < offset+16 {
			return nil, 0, errors.New("address data too short for IPv6")
		}
		a.Host = net.IP(data[offset : offset+16]).String()
		offset += 16

	case AddrTypeDomain:
		if len(data) < offset+1 {
			return nil, 0, errors.New("address data too short for domain length")
		}
		domainLen := int(data[offset])
		offset++
		if len(data) < offset+domainLen {
			return nil, 0, errors.New("address data too short for domain")
		}
		a.Host = string(data[offset : offset+domainLen])
		offset += domainLen

	default:
		return nil, 0, errors.New("unknown address type")
	}

	if a.Network != NetworkTCP && a.Network != NetworkUDP {
		return nil, 0, errors.New("unknown network type")
	}

	return a, offset, nil
}

// EncryptedConn wraps a net.Conn with encryption for use with smux
type EncryptedConn struct {
	conn   net.Conn
	engine *CryptoEngine

	readMu  sync.Mutex
	writeMu sync.Mutex

	readBuf []byte
}

// NewEncryptedConn creates a new encrypted connection wrapper
func NewEncryptedConn(conn net.Conn, engine *CryptoEngine) *EncryptedConn {
	return &EncryptedConn{
		conn:   conn,
		engine: engine,
	}
}

// Read reads decrypted data from the connection
func (c *EncryptedConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	if len(c.readBuf) > 0 {
		n := copy(p, c.readBuf)
		c.readBuf = c.readBuf[n:]
		return n, nil
	}

	var lenBuf [2]byte
	if _, err := io.ReadFull(c.conn, lenBuf[:]); err != nil {
		return 0, err
	}
	totalLen := int(binary.BigEndian.Uint16(lenBuf[:]))

	if totalLen < NonceSuffixSize+GCMTagSize {
		return 0, io.ErrUnexpectedEOF
	}

	encryptedData := make([]byte, totalLen)
	if _, err := io.ReadFull(c.conn, encryptedData); err != nil {
		return 0, err
	}

	var suffix [NonceSuffixSize]byte
	copy(suffix[:], encryptedData[:NonceSuffixSize])
	ciphertext := encryptedData[NonceSuffixSize:]

	plaintext, err := c.engine.Decrypt(suffix, ciphertext)
	if err != nil {
		return 0, err
	}

	n := copy(p, plaintext)
	if n < len(plaintext) {
		c.readBuf = append(c.readBuf, plaintext[n:]...)
	}

	return n, nil
}

// Write encrypts and writes data to the connection
func (c *EncryptedConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	suffix, ciphertext := c.engine.Encrypt(p, 0)

	totalLen := uint16(len(suffix) + len(ciphertext))
	buf := make([]byte, 2+int(totalLen))
	binary.BigEndian.PutUint16(buf[:2], totalLen)
	copy(buf[2:2+NonceSuffixSize], suffix[:])
	copy(buf[2+NonceSuffixSize:], ciphertext)

	_, err := c.conn.Write(buf)
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

// Close closes the underlying connection
func (c *EncryptedConn) Close() error {
	return c.conn.Close()
}

// LocalAddr returns the local network address
func (c *EncryptedConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address
func (c *EncryptedConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines
func (c *EncryptedConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline
func (c *EncryptedConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline
func (c *EncryptedConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// Verify EncryptedConn implements net.Conn
var _ net.Conn = (*EncryptedConn)(nil)

// Session represents a multiplexed connection session using smux
type Session struct {
	conn       net.Conn
	encConn    *EncryptedConn
	smuxSess   *smux.Session
	config     *SessionConfig
	smuxConfig *smux.Config

	closed     atomic.Bool
	closedOnce sync.Once

	stats *SessionStats
}

// NewClientSession creates a new client session
func NewClientSession(conn net.Conn, engine *CryptoEngine, config *SessionConfig) (*Session, error) {
	if config == nil {
		config = DefaultSessionConfig(true)
	}
	config.IsClient = true

	encConn := NewEncryptedConn(conn, engine)

	smuxConfig := smux.DefaultConfig()
	smuxConfig.Version = 2
	smuxConfig.KeepAliveInterval = config.PingInterval
	smuxConfig.KeepAliveTimeout = config.IdleTimeout
	smuxConfig.MaxFrameSize = 32768
	smuxConfig.MaxReceiveBuffer = config.InitialWindowSize
	smuxConfig.MaxStreamBuffer = config.InitialWindowSize

	smuxSess, err := smux.Client(encConn, smuxConfig)
	if err != nil {
		return nil, err
	}

	s := &Session{
		conn:       conn,
		encConn:    encConn,
		smuxSess:   smuxSess,
		config:     config,
		smuxConfig: smuxConfig,
		stats:      NewSessionStats(),
	}

	return s, nil
}

// OpenStream creates a new stream with the target address
func (s *Session) OpenStream(addr *Address) (*Stream, error) {
	if s.closed.Load() {
		return nil, ErrSessionClosed
	}

	smuxStream, err := s.smuxSess.OpenStream()
	if err != nil {
		return nil, err
	}

	s.stats.AddStream()

	addrBytes := addr.Marshal()
	lenBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBuf, uint16(len(addrBytes)))

	if _, err := smuxStream.Write(lenBuf); err != nil {
		_ = smuxStream.Close()
		return nil, err
	}
	if _, err := smuxStream.Write(addrBytes); err != nil {
		_ = smuxStream.Close()
		return nil, err
	}

	stream := &Stream{
		smuxStream: smuxStream,
		session:    s,
		remoteAddr: &netAddr{network: addr.NetworkString(), address: addr.String()},
		createdAt:  time.Now(),
	}

	return stream, nil
}

// Close closes the session gracefully
func (s *Session) Close() error {
	s.closedOnce.Do(func() {
		s.closed.Store(true)
		_ = s.smuxSess.Close()
		_ = s.conn.Close()
	})

	return nil
}

// IsClosed returns whether the session is closed
func (s *Session) IsClosed() bool {
	return s.closed.Load() || s.smuxSess.IsClosed()
}

// Stats returns session statistics
func (s *Session) Stats() SessionStats {
	return s.stats.Snapshot()
}

// NumStreams returns the number of active streams
func (s *Session) NumStreams() int {
	return s.smuxSess.NumStreams()
}

// Stream represents a multiplexed stream within a session
type Stream struct {
	smuxStream *smux.Stream
	session    *Session

	localAddr  net.Addr
	remoteAddr net.Addr

	createdAt time.Time
}

// ID returns the stream ID
func (s *Stream) ID() uint32 {
	return s.smuxStream.ID()
}

// Read reads data from the stream
func (s *Stream) Read(p []byte) (int, error) {
	n, err := s.smuxStream.Read(p)
	if n > 0 {
		s.session.stats.AddBytesRx(int64(n))
	}
	return n, err
}

// Write writes data to the stream
func (s *Stream) Write(p []byte) (int, error) {
	n, err := s.smuxStream.Write(p)
	if n > 0 {
		s.session.stats.AddBytesTx(int64(n))
	}
	return n, err
}

// Close closes the stream
func (s *Stream) Close() error {
	s.session.stats.RemoveStream()
	return s.smuxStream.Close()
}

// SetDeadline sets the read and write deadlines
func (s *Stream) SetDeadline(t time.Time) error {
	return s.smuxStream.SetDeadline(t)
}

// SetReadDeadline sets the read deadline
func (s *Stream) SetReadDeadline(t time.Time) error {
	return s.smuxStream.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline
func (s *Stream) SetWriteDeadline(t time.Time) error {
	return s.smuxStream.SetWriteDeadline(t)
}

// LocalAddr returns the local network address
func (s *Stream) LocalAddr() net.Addr {
	if s.localAddr != nil {
		return s.localAddr
	}
	return s.session.conn.LocalAddr()
}

// RemoteAddr returns the remote network address
func (s *Stream) RemoteAddr() net.Addr {
	return s.remoteAddr
}

// Verify Stream implements net.Conn
var _ net.Conn = (*Stream)(nil)

// netAddr implements net.Addr
type netAddr struct {
	network string
	address string
}

func (a *netAddr) Network() string { return a.network }
func (a *netAddr) String() string  { return a.address }
