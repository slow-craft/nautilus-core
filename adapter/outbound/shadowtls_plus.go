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
	Name   string `proxy:"name"`
	Server string `proxy:"server"`
	Port   int    `proxy:"port"`
	UUID   string `proxy:"uuid"`
	UDP    bool   `proxy:"udp,omitempty"`
	SNI    string `proxy:"sni,omitempty"`

	// Timeouts
	HandshakeTimeout int `proxy:"handshake-timeout,omitempty"` // in seconds
	IdleTimeout      int `proxy:"idle-timeout,omitempty"`      // in seconds
	DialTimeout      int `proxy:"dial-timeout,omitempty"`      // in seconds
	RetryTimes       int `proxy:"retry-times,omitempty"`

	// UDP/KCP transport configuration (for dual-stack support)
	UDPTransport *UDPTransportOption `proxy:"udp-transport,omitempty"`
}

// UDPTransportOption contains UDP transport configuration
type UDPTransportOption struct {
	// Enabled enables UDP transport for dual-stack mode
	Enabled bool `proxy:"enabled,omitempty"`

	// DialTimeout is the UDP dial timeout in seconds (default: 5)
	DialTimeout int `proxy:"dial-timeout,omitempty"`

	// HandshakeTimeout is the UDP handshake timeout in seconds (default: 5)
	HandshakeTimeout int `proxy:"handshake-timeout,omitempty"`

	// RetryTimes is the number of UDP retry attempts (default: 1)
	RetryTimes int `proxy:"retry-times,omitempty"`

	// KCP contains KCP protocol configuration
	KCP *KCPOption `proxy:"kcp,omitempty"`

	// Strategy contains dual-stack strategy configuration
	Strategy *StrategyOption `proxy:"strategy,omitempty"`
}

// KCPOption contains KCP protocol configuration
type KCPOption struct {
	// Mode is the KCP operation mode: fast/normal/default
	Mode string `proxy:"mode,omitempty"`

	// MTU is the maximum transmission unit (default: 1400)
	MTU int `proxy:"mtu,omitempty"`

	// SndWnd is the send window size (default: 1024)
	SndWnd int `proxy:"snd-wnd,omitempty"`

	// RcvWnd is the receive window size (default: 1024)
	RcvWnd int `proxy:"rcv-wnd,omitempty"`

	// DataShard is the number of data shards for FEC (0 to disable)
	DataShard int `proxy:"data-shard,omitempty"`

	// ParityShard is the number of parity shards for FEC (0 to disable)
	ParityShard int `proxy:"parity-shard,omitempty"`
}

// StrategyOption contains dual-stack strategy configuration
type StrategyOption struct {
	// Primary is the preferred protocol: auto/tcp/udp
	Primary string `proxy:"primary,omitempty"`

	// RTTThreshold is the RTT threshold in milliseconds for switching (default: 500)
	RTTThreshold int `proxy:"rtt-threshold,omitempty"`

	// FailureThreshold is the number of consecutive failures before switching (default: 3)
	FailureThreshold int `proxy:"failure-threshold,omitempty"`

	// RecoveryInterval is the time in seconds to wait before retrying failed transport (default: 30)
	RecoveryInterval int `proxy:"recovery-interval,omitempty"`

	// WarmupBoth enables connecting both transports at startup (default: true)
	WarmupBoth *bool `proxy:"warmup-both,omitempty"`

	// HealthCheckInterval is the interval in seconds for health checks (default: 30)
	HealthCheckInterval int `proxy:"health-check-interval,omitempty"`
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
	config := stp.DefaultClientConfig(s.addr, s.option.UUID)

	if s.option.SNI != "" {
		config.SNI = s.option.SNI
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

	// Configure UDP transport if enabled
	if s.option.UDPTransport != nil && s.option.UDPTransport.Enabled {
		config.UDP = s.buildUDPConfig(s.option.UDPTransport)
	}

	// Use the adapter's dialer
	config.Dialer = &stpDialerWrapper{dialer: s.dialer}

	return stp.NewClient(config)
}

// buildUDPConfig builds UDP client configuration from options
func (s *ShadowTLSPlus) buildUDPConfig(opt *UDPTransportOption) *stp.UDPClientConfig {
	udpConfig := stp.DefaultUDPClientConfig()
	udpConfig.Enabled = true

	if opt.DialTimeout > 0 {
		udpConfig.DialTimeout = time.Duration(opt.DialTimeout) * time.Second
	}

	if opt.HandshakeTimeout > 0 {
		udpConfig.HandshakeTimeout = time.Duration(opt.HandshakeTimeout) * time.Second
	}

	if opt.RetryTimes > 0 {
		udpConfig.RetryTimes = opt.RetryTimes
	}

	// Configure KCP
	if opt.KCP != nil {
		udpConfig.KCP = s.buildKCPConfig(opt.KCP)
	}

	// Configure strategy
	if opt.Strategy != nil {
		udpConfig.Strategy = s.buildStrategyConfig(opt.Strategy)
	}

	return udpConfig
}

// buildKCPConfig builds KCP configuration from options
func (s *ShadowTLSPlus) buildKCPConfig(opt *KCPOption) *stp.KCPConfig {
	kcpConfig := stp.DefaultKCPConfig()

	if opt.Mode != "" {
		kcpConfig.Mode = stp.KCPMode(opt.Mode)
	}

	if opt.MTU > 0 {
		kcpConfig.MTU = opt.MTU
	}

	if opt.SndWnd > 0 {
		kcpConfig.SndWnd = opt.SndWnd
	}

	if opt.RcvWnd > 0 {
		kcpConfig.RcvWnd = opt.RcvWnd
	}

	if opt.DataShard > 0 {
		kcpConfig.DataShard = opt.DataShard
	}

	if opt.ParityShard > 0 {
		kcpConfig.ParityShard = opt.ParityShard
	}

	return kcpConfig
}

// buildStrategyConfig builds strategy configuration from options
func (s *ShadowTLSPlus) buildStrategyConfig(opt *StrategyOption) *stp.StrategyConfig {
	strategyConfig := stp.DefaultStrategyConfig()

	if opt.Primary != "" {
		strategyConfig.Primary = opt.Primary
	}

	if opt.RTTThreshold > 0 {
		strategyConfig.RTTThreshold = time.Duration(opt.RTTThreshold) * time.Millisecond
	}

	if opt.FailureThreshold > 0 {
		strategyConfig.FailureThreshold = opt.FailureThreshold
	}

	if opt.RecoveryInterval > 0 {
		strategyConfig.RecoveryInterval = time.Duration(opt.RecoveryInterval) * time.Second
	}

	if opt.WarmupBoth != nil {
		strategyConfig.WarmupBoth = *opt.WarmupBoth
	}

	if opt.HealthCheckInterval > 0 {
		strategyConfig.HealthCheckInterval = time.Duration(opt.HealthCheckInterval) * time.Second
	}

	return strategyConfig
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

	if option.UUID == "" {
		return nil, fmt.Errorf("shadowtls-plus: uuid is required")
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
