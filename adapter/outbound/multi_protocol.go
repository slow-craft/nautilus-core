package outbound

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/structure"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

const (
	defaultMaxFailures       = 3
	defaultDialTimeout       = 5
	defaultProbeRate         = 0.1
	defaultMinProbeRate      = 0.02
	defaultRecoveryThreshold = 2
)

type MultiProtocolOption struct {
	BasicOption
	Name              string           `proxy:"name"`
	Protocols         []map[string]any `proxy:"protocols"`
	MaxFailures       int              `proxy:"max-failures,omitempty"`
	DialTimeout       int              `proxy:"dial-timeout,omitempty"`
	ProbeRate         float64          `proxy:"probe-rate,omitempty"`
	MinProbeRate      float64          `proxy:"min-probe-rate,omitempty"`
	RecoveryThreshold int              `proxy:"recovery-threshold,omitempty"`
}

type protocolState struct {
	proxy         ProxyAdapter
	alive         atomic.Bool
	failCount     atomic.Int32
	shadowFails   atomic.Int32
	recoveryCount atomic.Int32
	probing       atomic.Bool
}

type MultiProtocol struct {
	*Base
	protocols         []*protocolState
	activeIndex       atomic.Int32
	maxFailures       int32
	dialTimeout       time.Duration
	probeRate         float64
	minProbeRate      float64
	recoveryThreshold int32
	cancel            context.CancelFunc
	ctx               context.Context
}

func NewMultiProtocol(option MultiProtocolOption) (*MultiProtocol, error) {
	if len(option.Protocols) == 0 {
		return nil, fmt.Errorf("multi-protocol: at least one protocol required")
	}

	maxFailures := option.MaxFailures
	if maxFailures <= 0 {
		maxFailures = defaultMaxFailures
	}
	dialTimeout := option.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultDialTimeout
	}
	probeRate := option.ProbeRate
	if probeRate <= 0 {
		probeRate = defaultProbeRate
	}
	minProbeRate := option.MinProbeRate
	if minProbeRate <= 0 {
		minProbeRate = defaultMinProbeRate
	}
	recoveryThreshold := option.RecoveryThreshold
	if recoveryThreshold <= 0 {
		recoveryThreshold = defaultRecoveryThreshold
	}

	decoder := structure.NewDecoder(structure.Option{
		TagName: "proxy", WeaklyTypedInput: true,
		KeyReplacer: structure.DefaultKeyReplacer,
	})

	protocols := make([]*protocolState, 0, len(option.Protocols))
	hasUDP := false

	for i, protoMap := range option.Protocols {
		proxy, err := parseProtocolProxy(decoder, protoMap, option.BasicOption)
		if err != nil {
			return nil, fmt.Errorf("multi-protocol: protocol[%d]: %w", i, err)
		}
		state := &protocolState{proxy: proxy}
		state.alive.Store(true)
		protocols = append(protocols, state)
		if proxy.SupportUDP() {
			hasUDP = true
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	mp := &MultiProtocol{
		Base: NewBase(BaseOption{
			Name:        option.Name,
			Addr:        protocols[0].proxy.Addr(),
			Type:        C.MultiProtocol,
			UDP:         hasUDP,
			Interface:   option.Interface,
			RoutingMark: option.RoutingMark,
			Prefer:      option.IPVersion,
		}),
		protocols:         protocols,
		maxFailures:       int32(maxFailures),
		dialTimeout:       time.Duration(dialTimeout) * time.Second,
		probeRate:         probeRate,
		minProbeRate:      minProbeRate,
		recoveryThreshold: int32(recoveryThreshold),
		ctx:               ctx,
		cancel:            cancel,
	}

	log.Infoln("[MultiProtocol] %s: initialized with %d protocols, maxFailures=%d, dialTimeout=%ds, probeRate=%.2f, recoveryThreshold=%d",
		option.Name, len(protocols), maxFailures, dialTimeout, probeRate, recoveryThreshold)
	for i, p := range protocols {
		log.Debugln("[MultiProtocol] %s: protocol[%d] = %s (%s), udp=%v",
			option.Name, i, p.proxy.Name(), p.proxy.Type().String(), p.proxy.SupportUDP())
	}

	return mp, nil
}

func (m *MultiProtocol) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	var lastErr error
	startIdx := int(m.activeIndex.Load())
	n := len(m.protocols)

	for j := 0; j < n; j++ {
		i := (startIdx + j) % n
		p := m.protocols[i]
		if !p.alive.Load() {
			log.Debugln("[MultiProtocol] %s: TCP skip dead protocol[%d] %s", m.Name(), i, p.proxy.Name())
			continue
		}

		log.Debugln("[MultiProtocol] %s: TCP trying protocol[%d] %s for %s:%d",
			m.Name(), i, p.proxy.Name(), metadata.String(), metadata.DstPort)
		dialCtx, cancel := context.WithTimeout(ctx, m.dialTimeout)
		conn, err := p.proxy.DialContext(dialCtx, metadata)
		cancel()

		if err == nil {
			p.failCount.Store(0)
			if int32(i) < m.activeIndex.Load() {
				m.activeIndex.Store(int32(i))
			}
			log.Debugln("[MultiProtocol] %s: TCP connected via protocol[%d] %s", m.Name(), i, p.proxy.Name())
			if i != 0 {
				m.triggerShadowDial(false)
			}
			return conn, nil
		}

		lastErr = err
		newFails := p.failCount.Add(1)
		log.Debugln("[MultiProtocol] %s: TCP protocol[%d] %s dial failed (%d/%d): %v",
			m.Name(), i, p.proxy.Name(), newFails, m.maxFailures, err)
		if newFails >= m.maxFailures {
			p.alive.Store(false)
			log.Warnln("[MultiProtocol] %s: protocol %s marked unavailable after %d failures",
				m.Name(), p.proxy.Name(), newFails)
			m.updateActiveIndex()
		}

		if ctx.Err() != nil {
			break
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("multi-protocol %s: all protocols unavailable", m.Name())
	}
	m.resetAllIfAllDead()
	return nil, lastErr
}

func (m *MultiProtocol) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	var lastErr error
	startIdx := int(m.activeIndex.Load())
	n := len(m.protocols)

	for j := 0; j < n; j++ {
		i := (startIdx + j) % n
		p := m.protocols[i]
		if !p.alive.Load() {
			log.Debugln("[MultiProtocol] %s: UDP skip dead protocol[%d] %s", m.Name(), i, p.proxy.Name())
			continue
		}
		if !p.proxy.SupportUDP() {
			log.Debugln("[MultiProtocol] %s: UDP skip non-UDP protocol[%d] %s", m.Name(), i, p.proxy.Name())
			continue
		}

		log.Debugln("[MultiProtocol] %s: UDP trying protocol[%d] %s for %s:%d",
			m.Name(), i, p.proxy.Name(), metadata.String(), metadata.DstPort)
		dialCtx, cancel := context.WithTimeout(ctx, m.dialTimeout)
		pc, err := p.proxy.ListenPacketContext(dialCtx, metadata)
		cancel()

		if err == nil {
			p.failCount.Store(0)
			if int32(i) < m.activeIndex.Load() {
				m.activeIndex.Store(int32(i))
			}
			log.Debugln("[MultiProtocol] %s: UDP connected via protocol[%d] %s", m.Name(), i, p.proxy.Name())
			if i != 0 {
				m.triggerShadowDial(true)
			}
			return pc, nil
		}

		lastErr = err
		newFails := p.failCount.Add(1)
		log.Debugln("[MultiProtocol] %s: UDP protocol[%d] %s listen failed (%d/%d): %v",
			m.Name(), i, p.proxy.Name(), newFails, m.maxFailures, err)
		if newFails >= m.maxFailures {
			p.alive.Store(false)
			log.Warnln("[MultiProtocol] %s: protocol %s marked unavailable after %d failures",
				m.Name(), p.proxy.Name(), newFails)
			m.updateActiveIndex()
		}

		if ctx.Err() != nil {
			break
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("multi-protocol %s: no UDP-capable protocol available", m.Name())
	}
	m.resetAllIfAllDead()
	return nil, lastErr
}

func (m *MultiProtocol) Addr() string {
	idx := m.activeIndex.Load()
	if int(idx) < len(m.protocols) {
		return m.protocols[idx].proxy.Addr()
	}
	return m.protocols[0].proxy.Addr()
}

func (m *MultiProtocol) SupportUDP() bool {
	for _, p := range m.protocols {
		if p.alive.Load() && p.proxy.SupportUDP() {
			return true
		}
	}
	return false
}

func (m *MultiProtocol) SupportUOT() bool {
	idx := m.activeIndex.Load()
	if int(idx) < len(m.protocols) {
		return m.protocols[idx].proxy.SupportUOT()
	}
	return false
}

func (m *MultiProtocol) IsL3Protocol(metadata *C.Metadata) bool {
	idx := m.activeIndex.Load()
	if int(idx) < len(m.protocols) {
		return m.protocols[idx].proxy.IsL3Protocol(metadata)
	}
	return false
}

func (m *MultiProtocol) MarshalJSON() ([]byte, error) {
	activeIdx := m.activeIndex.Load()
	if int(activeIdx) >= len(m.protocols) {
		activeIdx = 0
	}
	protoStates := make([]map[string]any, 0, len(m.protocols))
	for i, p := range m.protocols {
		protoStates = append(protoStates, map[string]any{
			"type":       p.proxy.Type().String(),
			"addr":       p.proxy.Addr(),
			"alive":      p.alive.Load(),
			"fail_count": p.failCount.Load(),
			"active":     int32(i) == activeIdx,
		})
	}
	return json.Marshal(map[string]any{
		"type":            m.Type().String(),
		"id":              m.Id(),
		"active_protocol": m.protocols[activeIdx].proxy.Type().String(),
		"protocols":       protoStates,
	})
}

func (m *MultiProtocol) Close() error {
	m.cancel()
	for _, p := range m.protocols {
		_ = p.proxy.Close()
	}
	return nil
}

// --- internal ---

func (m *MultiProtocol) updateActiveIndex() {
	for i, p := range m.protocols {
		if p.alive.Load() {
			old := m.activeIndex.Swap(int32(i))
			if old != int32(i) {
				log.Infoln("[MultiProtocol] %s: active protocol switched from [%d] to [%d] %s",
					m.Name(), old, i, p.proxy.Name())
			}
			return
		}
	}
	m.activeIndex.Store(0)
}

func (m *MultiProtocol) resetAllIfAllDead() {
	for _, p := range m.protocols {
		if p.alive.Load() {
			return
		}
	}
	log.Warnln("[MultiProtocol] %s: all protocols dead, resetting all to alive", m.Name())
	for i, p := range m.protocols {
		p.alive.Store(true)
		p.failCount.Store(0)
		log.Debugln("[MultiProtocol] %s: reset protocol[%d] %s to alive", m.Name(), i, p.proxy.Name())
	}
	m.activeIndex.Store(0)
}

func newProbeMetadata(udp bool) *C.Metadata {
	meta := &C.Metadata{
		DstIP: netip.MustParseAddr("1.1.1.1"),
	}
	if udp {
		meta.DstPort = 53
		meta.NetWork = C.UDP
	} else {
		meta.DstPort = 443
		meta.NetWork = C.TCP
	}
	return meta
}

func (m *MultiProtocol) triggerShadowDial(udp bool) {
	activeIdx := int(m.activeIndex.Load())
	if activeIdx == 0 {
		return
	}

	network := "TCP"
	if udp {
		network = "UDP"
	}

	for i := 0; i < activeIdx; i++ {
		p := m.protocols[i]
		if p.alive.Load() || p.probing.Load() {
			continue
		}
		if udp && !p.proxy.SupportUDP() {
			continue
		}

		rate := m.adaptiveRate(p)
		if rand.Float64() > rate {
			log.Debugln("[MultiProtocol] %s: shadow dial skipped for protocol[%d] %s (rate=%.4f)",
				m.Name(), i, p.proxy.Name(), rate)
			continue
		}

		if !p.probing.CompareAndSwap(false, true) {
			continue
		}

		log.Debugln("[MultiProtocol] %s: shadow dial %s started for protocol[%d] %s (rate=%.4f)",
			m.Name(), network, i, p.proxy.Name(), rate)

		go func(idx int, ps *protocolState) {
			defer ps.probing.Store(false)

			probeMeta := newProbeMetadata(udp)

			ctx, cancel := context.WithTimeout(m.ctx, m.dialTimeout)
			defer cancel()

			var err error
			if udp {
				pc, e := ps.proxy.ListenPacketContext(ctx, probeMeta)
				if e == nil {
					_ = pc.Close()
				}
				err = e
			} else {
				conn, e := ps.proxy.DialContext(ctx, probeMeta)
				if e == nil {
					_ = conn.Close()
				}
				err = e
			}

			if err == nil {
				newCount := ps.recoveryCount.Add(1)
				log.Debugln("[MultiProtocol] %s: shadow dial %s succeeded for protocol[%d] %s (recovery %d/%d)",
					m.Name(), network, idx, ps.proxy.Name(), newCount, m.recoveryThreshold)
				if newCount >= m.recoveryThreshold {
					ps.alive.Store(true)
					ps.failCount.Store(0)
					ps.shadowFails.Store(0)
					ps.recoveryCount.Store(0)
					m.updateActiveIndex()
					log.Infoln("[MultiProtocol] %s: protocol %s recovered via shadow dial",
						m.Name(), ps.proxy.Name())
				}
			} else {
				ps.recoveryCount.Store(0)
				newShadowFails := ps.shadowFails.Add(1)
				log.Debugln("[MultiProtocol] %s: shadow dial %s failed for protocol[%d] %s (shadowFails=%d): %v",
					m.Name(), network, idx, ps.proxy.Name(), newShadowFails, err)
			}
		}(i, p)
	}
}

func (m *MultiProtocol) adaptiveRate(p *protocolState) float64 {
	fails := float64(p.shadowFails.Load())
	rate := m.probeRate * math.Pow(0.5, fails)
	if rate < m.minProbeRate {
		return m.minProbeRate
	}
	return rate
}

// allowedMultiProtocolTypes defines the whitelist of protocol types that can be
// used as sub-protocols in multi-protocol. L3 protocols (wireguard, masque) are
// excluded because they operate at the IP layer and are incompatible with
// application-layer failover.
var allowedMultiProtocolTypes = map[string]bool{
	"ss":          true,
	"ssr":         true,
	"socks5":      true,
	"http":        true,
	"vmess":       true,
	"vless":       true,
	"snell":       true,
	"trojan":      true,
	"hysteria":    true,
	"hysteria2":   true,
	"tuic":        true,
	"ssh":         true,
	"mieru":       true,
	"anytls":      true,
	"sudoku":      true,
	"trusttunnel": true,
}

// parseProtocolProxy creates a ProxyAdapter from a protocol config map.
func parseProtocolProxy(decoder *structure.Decoder, mapping map[string]any, basicOption BasicOption) (ProxyAdapter, error) {
	proxyType, ok := mapping["type"].(string)
	if !ok {
		return nil, fmt.Errorf("missing type")
	}

	if !allowedMultiProtocolTypes[proxyType] {
		log.Warnln("[MultiProtocol] protocol type %q rejected: not in allowed types (L3 protocols like wireguard/masque are not supported)", proxyType)
		return nil, fmt.Errorf("protocol type %q is not allowed in multi-protocol (L3 protocols like wireguard/masque are not supported)", proxyType)
	}

	// Auto-generate name if not provided, so sub-protocols don't require explicit names.
	// Use a shallow copy to avoid mutating the caller's map.
	if _, hasName := mapping["name"]; !hasName {
		copied := make(map[string]any, len(mapping)+1)
		for k, v := range mapping {
			copied[k] = v
		}
		if server, ok := mapping["server"].(string); ok {
			copied["name"] = fmt.Sprintf("%s-%s", proxyType, server)
		} else {
			copied["name"] = proxyType
		}
		mapping = copied
	}

	var (
		proxy ProxyAdapter
		err   error
	)

	switch proxyType {
	case "ss":
		opt := &ShadowSocksOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewShadowSocks(*opt)
		}
	case "ssr":
		opt := &ShadowSocksROption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewShadowSocksR(*opt)
		}
	case "socks5":
		opt := &Socks5Option{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewSocks5(*opt)
		}
	case "http":
		opt := &HttpOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewHttp(*opt)
		}
	case "vmess":
		opt := &VmessOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewVmess(*opt)
		}
	case "vless":
		opt := &VlessOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewVless(*opt)
		}
	case "snell":
		opt := &SnellOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewSnell(*opt)
		}
	case "trojan":
		opt := &TrojanOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewTrojan(*opt)
		}
	case "hysteria":
		opt := &HysteriaOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewHysteria(*opt)
		}
	case "hysteria2":
		opt := &Hysteria2Option{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewHysteria2(*opt)
		}
	case "tuic":
		opt := &TuicOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewTuic(*opt)
		}
	case "ssh":
		opt := &SshOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewSsh(*opt)
		}
	case "mieru":
		opt := &MieruOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewMieru(*opt)
		}
	case "anytls":
		opt := &AnyTLSOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewAnyTLS(*opt)
		}
	case "sudoku":
		opt := &SudokuOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewSudoku(*opt)
		}
	case "trusttunnel":
		opt := &TrustTunnelOption{BasicOption: basicOption}
		if err = decoder.Decode(mapping, opt); err == nil {
			proxy, err = NewTrustTunnel(*opt)
		}
	default:
		return nil, fmt.Errorf("unsupported protocol type in multi-protocol: %s", proxyType)
	}

	return proxy, err
}
