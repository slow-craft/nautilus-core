package outbound

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	stp "github.com/metacubex/mihomo/transport/shadowtls-plus"
)

type ShadowTLSPlus struct {
	*Base
	option *ShadowTLSPlusOption

	client     *stp.Client
	clientOnce sync.Once
	clientErr  error
}

type ShadowTLSPlusOption struct {
	BasicOption
	Name     string   `proxy:"name"`
	Server   string   `proxy:"server"`
	Port     int      `proxy:"port"`
	Password string   `proxy:"password"`
	UDP      bool     `proxy:"udp,omitempty"`
	SNIList  []string `proxy:"sni-list,omitempty"`

	// SNI rotation settings
	SNIRotateMode string `proxy:"sni-rotate-mode,omitempty"` // "random" or "round_robin"
	SNIRotateFreq string `proxy:"sni-rotate-freq,omitempty"` // "per_connection" or "per_minute"

	// Timeouts
	HandshakeTimeout int `proxy:"handshake-timeout,omitempty"` // in seconds
	IdleTimeout      int `proxy:"idle-timeout,omitempty"`      // in seconds
	DialTimeout      int `proxy:"dial-timeout,omitempty"`      // in seconds
	RetryTimes       int `proxy:"retry-times,omitempty"`
}

// DialContext implements C.ProxyAdapter
func (s *ShadowTLSPlus) DialContext(ctx context.Context, metadata *C.Metadata) (_ C.Conn, err error) {
	client, err := s.getClient()
	if err != nil {
		return nil, err
	}

	address := metadata.RemoteAddress()
	c, err := client.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("%s connect error: %w", s.addr, err)
	}

	return NewConn(N.NewRefConn(c, s), s), nil
}

// ListenPacketContext implements C.ProxyAdapter
func (s *ShadowTLSPlus) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (_ C.PacketConn, err error) {
	if err = s.ResolveUDP(ctx, metadata); err != nil {
		return nil, err
	}

	client, err := s.getClient()
	if err != nil {
		return nil, err
	}

	address := metadata.UDPAddr().String()
	c, err := client.DialContext(ctx, "udp", address)
	if err != nil {
		return nil, fmt.Errorf("%s connect error: %w", s.addr, err)
	}

	pc := &shadowTLSPlusPacketConn{
		Conn:       c,
		rAddr:      metadata.UDPAddr(),
		bufferSize: 65535,
	}

	return newPacketConn(pc, s), nil
}

// SupportUOT implements C.ProxyAdapter
func (s *ShadowTLSPlus) SupportUOT() bool {
	return true
}

// ProxyInfo implements C.ProxyAdapter
func (s *ShadowTLSPlus) ProxyInfo() C.ProxyInfo {
	info := s.Base.ProxyInfo()
	info.DialerProxy = s.option.DialerProxy
	info.SMUX = true
	return info
}

// Close implements C.ProxyAdapter
func (s *ShadowTLSPlus) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *ShadowTLSPlus) getClient() (*stp.Client, error) {
	s.clientOnce.Do(func() {
		s.client, s.clientErr = s.createClient()
	})

	if s.clientErr != nil {
		return nil, s.clientErr
	}

	// Check if session is closed and recreate if needed
	if s.client.IsClosed() {
		s.clientOnce = sync.Once{}
		s.clientOnce.Do(func() {
			s.client, s.clientErr = s.createClient()
		})
	}

	return s.client, s.clientErr
}

func (s *ShadowTLSPlus) createClient() (*stp.Client, error) {
	config := stp.DefaultClientConfig(s.addr, s.option.Password)

	if len(s.option.SNIList) > 0 {
		config.SNIList = s.option.SNIList
	}

	if s.option.SNIRotateMode != "" {
		config.SNIRotateMode = s.option.SNIRotateMode
	}

	if s.option.SNIRotateFreq != "" {
		config.SNIRotateFreq = s.option.SNIRotateFreq
	}

	if s.option.HandshakeTimeout > 0 {
		config.HandshakeTimeout = time.Duration(s.option.HandshakeTimeout) * time.Second
	}

	if s.option.IdleTimeout > 0 {
		config.IdleTimeout = time.Duration(s.option.IdleTimeout) * time.Second
	}

	if s.option.DialTimeout > 0 {
		config.DialTimeout = time.Duration(s.option.DialTimeout) * time.Second
	}

	if s.option.RetryTimes > 0 {
		config.RetryTimes = s.option.RetryTimes
	}

	// Use the adapter's dialer
	config.Dialer = &stpDialerWrapper{dialer: s.dialer}

	return stp.NewClient(config)
}

// stpDialerWrapper wraps C.Dialer to implement stp.ContextDialer
type stpDialerWrapper struct {
	dialer C.Dialer
}

func (d *stpDialerWrapper) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.dialer.DialContext(ctx, network, address)
}

// shadowTLSPlusPacketConn wraps a stream connection for UDP
type shadowTLSPlusPacketConn struct {
	net.Conn
	rAddr      net.Addr
	bufferSize int
}

func (pc *shadowTLSPlusPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, err = pc.Conn.Read(p)
	return n, pc.rAddr, err
}

func (pc *shadowTLSPlusPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	return pc.Conn.Write(p)
}

func NewShadowTLSPlus(option ShadowTLSPlusOption) (*ShadowTLSPlus, error) {
	addr := net.JoinHostPort(option.Server, strconv.Itoa(option.Port))

	if option.Password == "" {
		return nil, fmt.Errorf("shadowtls-plus: password is required")
	}

	s := &ShadowTLSPlus{
		Base: &Base{
			name:   option.Name,
			addr:   addr,
			tp:     C.ShadowTLSPlus,
			pdName: option.ProviderName,
			udp:    option.UDP,
			tfo:    option.TFO,
			mpTcp:  option.MPTCP,
			iface:  option.Interface,
			rmark:  option.RoutingMark,
			prefer: option.IPVersion,
		},
		option: &option,
	}
	s.dialer = option.NewDialer(s.DialOptions())

	return s, nil
}
