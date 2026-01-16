package outbound

import (
	"context"
	"fmt"
	"net"
	"net/netip"
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

	// Transport configuration (nested structure like original)
	Transport *TransportOption `proxy:"transport,omitempty"`
}

// TransportOption contains all transport layer configurations
type TransportOption struct {
	// TCP transport configuration (always enabled)
	TCP *TCPTransportOption `proxy:"tcp,omitempty"`

	// UDP transport configuration (KCP-based, for dual-stack mode)
	UDP *UDPTransportOption `proxy:"udp,omitempty"`

	// Mux configuration (shared by TCP and UDP)
	Mux *MuxOption `proxy:"mux,omitempty"`

	// Strategy configuration (for dual-stack mode)
	Strategy *StrategyOption `proxy:"strategy,omitempty"`
}

// TCPTransportOption contains TCP transport configuration
type TCPTransportOption struct {
	// DialTimeout is the TCP dial timeout in seconds (default: 30)
	DialTimeout int `proxy:"dial-timeout,omitempty"`

	// HandshakeTimeout is the TCP handshake timeout in seconds (default: 30)
	HandshakeTimeout int `proxy:"handshake-timeout,omitempty"`

	// IdleTimeout is the TCP idle timeout in seconds (default: 60)
	IdleTimeout int `proxy:"idle-timeout,omitempty"`

	// RetryTimes is the number of TCP retry attempts (default: 3)
	RetryTimes int `proxy:"retry-times,omitempty"`
}

// UDPTransportOption contains UDP transport configuration
type UDPTransportOption struct {
	// Enabled enables UDP transport for dual-stack mode (default: true)
	Enabled *bool `proxy:"enabled,omitempty"`

	// DialTimeout is the UDP dial timeout in seconds (default: 30)
	DialTimeout int `proxy:"dial-timeout,omitempty"`

	// HandshakeTimeout is the UDP handshake timeout in seconds (default: 30)
	HandshakeTimeout int `proxy:"handshake-timeout,omitempty"`

	// IdleTimeout is the UDP idle timeout in seconds (default: 60)
	IdleTimeout int `proxy:"idle-timeout,omitempty"`

	// RetryTimes is the number of UDP retry attempts (default: 3)
	RetryTimes int `proxy:"retry-times,omitempty"`

	// KCP contains KCP protocol configuration
	KCP *KCPOption `proxy:"kcp,omitempty"`
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

// MuxOption contains smux multiplexing configuration
type MuxOption struct {
	// MaxStreams is the maximum number of concurrent streams (default: 1024)
	MaxStreams int `proxy:"max-streams,omitempty"`

	// WindowSize is the flow control window size in bytes (default: 262144)
	WindowSize int `proxy:"window-size,omitempty"`

	// PingInterval is the heartbeat interval in seconds (default: 30)
	PingInterval int `proxy:"ping-interval,omitempty"`
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

	// Apply transport configuration
	if s.option.Transport != nil {
		s.applyTransportConfig(config, s.option.Transport)
	} else if s.option.UDP {
		// When udp: true but no transport config, auto-enable UDP transport
		// This matches the original client behavior where UDP is enabled by default
		config.UDP = stp.DefaultUDPClientConfig()
		config.UDP.Enabled = true
	}

	// Use the adapter's dialer
	config.Dialer = &stpDialerWrapper{dialer: s.dialer}

	return stp.NewClient(config)
}

// applyTransportConfig applies transport configuration to client config
func (s *ShadowTLSPlus) applyTransportConfig(config *stp.ClientConfig, transport *TransportOption) {
	// Apply TCP configuration
	if transport.TCP != nil {
		if transport.TCP.HandshakeTimeout > 0 {
			config.HandshakeTimeout = time.Duration(transport.TCP.HandshakeTimeout) * time.Second
		}
		if transport.TCP.IdleTimeout > 0 {
			config.IdleTimeout = time.Duration(transport.TCP.IdleTimeout) * time.Second
		}
		if transport.TCP.DialTimeout > 0 {
			config.DialTimeout = time.Duration(transport.TCP.DialTimeout) * time.Second
		}
		if transport.TCP.RetryTimes > 0 {
			config.RetryTimes = transport.TCP.RetryTimes
		}
	}

	// Apply mux configuration to session config
	if transport.Mux != nil {
		if config.SessionConfig == nil {
			config.SessionConfig = stp.DefaultSessionConfig(true)
		}
		if transport.Mux.MaxStreams > 0 {
			config.SessionConfig.MaxStreams = transport.Mux.MaxStreams
		}
		if transport.Mux.WindowSize > 0 {
			config.SessionConfig.InitialWindowSize = transport.Mux.WindowSize
		}
		if transport.Mux.PingInterval > 0 {
			config.SessionConfig.PingInterval = time.Duration(transport.Mux.PingInterval) * time.Second
		}
	}

	// Apply UDP configuration if enabled
	if transport.UDP != nil {
		// UDP is enabled by default if udp section exists, unless explicitly disabled
		enabled := true
		if transport.UDP.Enabled != nil {
			enabled = *transport.UDP.Enabled
		}

		if enabled {
			config.UDP = s.buildUDPConfig(transport.UDP, transport.Strategy)
		}
	}
}

// buildUDPConfig builds UDP client configuration from options
func (s *ShadowTLSPlus) buildUDPConfig(opt *UDPTransportOption, strategy *StrategyOption) *stp.UDPClientConfig {
	udpConfig := stp.DefaultUDPClientConfig()
	udpConfig.Enabled = true

	if opt.DialTimeout > 0 {
		udpConfig.DialTimeout = time.Duration(opt.DialTimeout) * time.Second
	}

	if opt.HandshakeTimeout > 0 {
		udpConfig.HandshakeTimeout = time.Duration(opt.HandshakeTimeout) * time.Second
	}

	if opt.IdleTimeout > 0 {
		udpConfig.IdleTimeout = time.Duration(opt.IdleTimeout) * time.Second
	}

	if opt.RetryTimes > 0 {
		udpConfig.RetryTimes = opt.RetryTimes
	}

	// Configure KCP
	if opt.KCP != nil {
		udpConfig.KCP = s.buildKCPConfig(opt.KCP)
	}

	// Configure strategy (from transport level)
	if strategy != nil {
		udpConfig.Strategy = s.buildStrategyConfig(strategy)
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

// stpDialerWrapper wraps C.Dialer to implement stp.ContextDialer and stp.UDPDialer
type stpDialerWrapper struct {
	dialer C.Dialer
}

func (d *stpDialerWrapper) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.dialer.DialContext(ctx, network, address)
}

func (d *stpDialerWrapper) ListenPacket(ctx context.Context, network, address string, rAddrPort netip.AddrPort) (net.PacketConn, error) {
	return d.dialer.ListenPacket(ctx, network, address, rAddrPort)
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
