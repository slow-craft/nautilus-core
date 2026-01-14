package shadowtlsplus

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// SelectionMode defines how transport is selected
type SelectionMode int

const (
	// SelectionModeAuto automatically selects the best available transport
	SelectionModeAuto SelectionMode = iota
	// SelectionModeTCPFirst prefers TCP when both are available
	SelectionModeTCPFirst
	// SelectionModeUDPFirst prefers UDP when both are available
	SelectionModeUDPFirst
)

// TransportManager manages multiple transports with dual-channel support
// Both TCP and UDP channels work in parallel - whichever is available gets used
type TransportManager struct {
	tcp Transport
	udp Transport

	config    *ManagerConfig
	healthMon *HealthMonitor

	closed atomic.Bool

	// Channels for control
	stopCh chan struct{}

	// Last known health status for change detection
	lastTCPAvailable bool
	lastUDPAvailable bool
}

// ManagerConfig contains transport manager configuration
type ManagerConfig struct {
	// SelectionMode determines how transport is selected when both are available
	// Auto: choose based on current health/RTT (lower RTT wins)
	// TCPFirst/UDPFirst: prefer one when both are healthy
	SelectionMode SelectionMode

	// RTTThreshold is the RTT threshold (default: 500ms)
	// Transport with RTT above this is considered degraded
	RTTThreshold time.Duration

	// FailureThreshold is the number of consecutive failures
	// before marking transport as temporarily unavailable
	FailureThreshold int

	// RecoveryInterval is time to wait before retrying failed transport
	RecoveryInterval time.Duration

	// WarmupBoth enables connecting both transports at startup
	WarmupBoth bool

	// HealthCheckInterval is the interval for health checks (default: 5s)
	HealthCheckInterval time.Duration
}

// DefaultManagerConfig returns default manager configuration
func DefaultManagerConfig() *ManagerConfig {
	return &ManagerConfig{
		SelectionMode:       SelectionModeAuto,
		RTTThreshold:        500 * time.Millisecond,
		FailureThreshold:    3,
		RecoveryInterval:    30 * time.Second,
		WarmupBoth:          true,
		HealthCheckInterval: 5 * time.Second,
	}
}

// NewTransportManager creates a new transport manager
func NewTransportManager(tcp, udp Transport, config *ManagerConfig) (*TransportManager, error) {
	if tcp == nil && udp == nil {
		return nil, errors.New("transport: at least one transport required")
	}
	if config == nil {
		config = DefaultManagerConfig()
	}

	m := &TransportManager{
		tcp:    tcp,
		udp:    udp,
		config: config,
		stopCh: make(chan struct{}),
	}

	// Initialize health monitor
	m.healthMon = &HealthMonitor{
		failureThreshold: config.FailureThreshold,
		recoveryInterval: config.RecoveryInterval,
		tcpFailures:      0,
		udpFailures:      0,
	}

	return m, nil
}

// Start starts the transport manager and connects available transports
func (m *TransportManager) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	var tcpErr, udpErr error

	// Connect both transports in parallel
	if m.tcp != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tcpErr = m.tcp.Connect(ctx)
		}()
	}

	if m.udp != nil && m.config.WarmupBoth {
		wg.Add(1)
		go func() {
			defer wg.Done()
			udpErr = m.udp.Connect(ctx)
		}()
	}

	wg.Wait()

	// At least one must succeed
	if tcpErr != nil && udpErr != nil {
		return errors.New("transport: failed to connect any transport")
	}

	// Start health check loop
	go m.healthLoop(ctx)

	return nil
}

// OpenStream opens a new stream through the best available transport
func (m *TransportManager) OpenStream(network, address string) (net.Conn, error) {
	if m.closed.Load() {
		return nil, errors.New("transport: manager closed")
	}

	// Get the best transport based on current health
	transport := m.selectBestTransport()
	if transport == nil {
		return nil, errors.New("transport: no available transport")
	}

	conn, err := transport.OpenStream(network, address)
	if err != nil {
		// Record failure
		m.healthMon.RecordFailure(transport.Protocol())

		// Try the other transport
		alternative := m.getAlternative(transport)
		if alternative != nil {
			// Connect if not connected
			if !alternative.IsConnected() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_ = alternative.Connect(ctx)
				cancel()
			}

			conn, err = alternative.OpenStream(network, address)
			if err == nil {
				m.healthMon.RecordSuccess(alternative.Protocol())
				return conn, nil
			}
			m.healthMon.RecordFailure(alternative.Protocol())
		}

		return nil, err
	}

	m.healthMon.RecordSuccess(transport.Protocol())
	return conn, nil
}

// selectBestTransport selects the best available transport
func (m *TransportManager) selectBestTransport() Transport {
	tcpHealth := m.getTransportHealth(m.tcp)
	udpHealth := m.getTransportHealth(m.udp)

	tcpAvailable := tcpHealth != nil && tcpHealth.Available && !m.healthMon.IsBlocked(ProtocolTCP)
	udpAvailable := udpHealth != nil && udpHealth.Available && !m.healthMon.IsBlocked(ProtocolUDP)

	// Neither available - try to connect
	if !tcpAvailable && !udpAvailable {
		// Try to reconnect whichever is not blocked
		if m.tcp != nil && !m.healthMon.IsBlocked(ProtocolTCP) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := m.tcp.Connect(ctx); err == nil {
				cancel()
				return m.tcp
			}
			cancel()
		}
		if m.udp != nil && !m.healthMon.IsBlocked(ProtocolUDP) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := m.udp.Connect(ctx); err == nil {
				cancel()
				return m.udp
			}
			cancel()
		}
		return nil
	}

	// Only one available
	if tcpAvailable && !udpAvailable {
		return m.tcp
	}
	if udpAvailable && !tcpAvailable {
		return m.udp
	}

	// Both available - select based on mode and health
	switch m.config.SelectionMode {
	case SelectionModeTCPFirst:
		return m.tcp
	case SelectionModeUDPFirst:
		return m.udp
	default:
		// Auto mode: choose based on RTT
		tcpRTT := m.getRTT(tcpHealth)
		udpRTT := m.getRTT(udpHealth)

		// If one has significantly better RTT (>20% difference), prefer it
		if tcpRTT > 0 && udpRTT > 0 {
			if tcpRTT < udpRTT*8/10 { // TCP is 20%+ faster
				return m.tcp
			}
			if udpRTT < tcpRTT*8/10 { // UDP is 20%+ faster
				return m.udp
			}
		}

		// Default to TCP if RTTs are similar
		return m.tcp
	}
}

// getAlternative returns the alternative transport
func (m *TransportManager) getAlternative(current Transport) Transport {
	if current == nil {
		return nil
	}
	switch current.Protocol() {
	case ProtocolTCP:
		return m.udp
	case ProtocolUDP:
		return m.tcp
	}
	return nil
}

// getTransportHealth safely gets transport health
func (m *TransportManager) getTransportHealth(t Transport) *HealthStatus {
	if t == nil {
		return nil
	}
	return t.Health()
}

// getRTT safely gets RTT from health status
func (m *TransportManager) getRTT(h *HealthStatus) time.Duration {
	if h == nil {
		return 0
	}
	return h.RTT
}

// healthLoop runs periodic health checks
func (m *TransportManager) healthLoop(ctx context.Context) {
	interval := m.config.HealthCheckInterval
	if interval <= 0 {
		interval = 5 * time.Second // Default to 5 seconds if not set
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkHealth()
		}
	}
}

// checkHealth checks transport health
func (m *TransportManager) checkHealth() {
	// Check TCP status
	if m.tcp != nil {
		h := m.tcp.Health()
		tcpAvailable := h.Available && !m.healthMon.IsBlocked(ProtocolTCP)
		m.lastTCPAvailable = tcpAvailable
	}

	// Check UDP status
	if m.udp != nil {
		h := m.udp.Health()
		udpAvailable := h.Available && !m.healthMon.IsBlocked(ProtocolUDP)
		m.lastUDPAvailable = udpAvailable
	}

	// Try to recover blocked transports
	m.healthMon.TryRecover()
}

// Close closes all transports
func (m *TransportManager) Close() error {
	if m.closed.Swap(true) {
		return nil
	}

	close(m.stopCh)

	var errs []error
	if m.tcp != nil {
		if err := m.tcp.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if m.udp != nil {
		if err := m.udp.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Stats returns manager statistics
func (m *TransportManager) Stats() *ManagerStats {
	stats := &ManagerStats{}

	if m.tcp != nil {
		stats.TCPHealth = m.tcp.Health()
		stats.TCPBlocked = m.healthMon.IsBlocked(ProtocolTCP)
	}
	if m.udp != nil {
		stats.UDPHealth = m.udp.Health()
		stats.UDPBlocked = m.healthMon.IsBlocked(ProtocolUDP)
	}

	return stats
}

// ManagerStats contains manager statistics
type ManagerStats struct {
	TCPHealth  *HealthStatus
	TCPBlocked bool
	UDPHealth  *HealthStatus
	UDPBlocked bool
}

// HealthMonitor tracks transport health and blocks failing transports temporarily
type HealthMonitor struct {
	mu               sync.Mutex
	failureThreshold int
	recoveryInterval time.Duration

	tcpFailures  int
	udpFailures  int
	tcpBlockedAt time.Time
	udpBlockedAt time.Time
}

// RecordSuccess records a successful operation
func (h *HealthMonitor) RecordSuccess(protocol Protocol) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch protocol {
	case ProtocolTCP:
		h.tcpFailures = 0
	case ProtocolUDP:
		h.udpFailures = 0
	}
}

// RecordFailure records a failed operation
func (h *HealthMonitor) RecordFailure(protocol Protocol) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch protocol {
	case ProtocolTCP:
		h.tcpFailures++
		if h.tcpFailures >= h.failureThreshold {
			h.tcpBlockedAt = time.Now()
		}
	case ProtocolUDP:
		h.udpFailures++
		if h.udpFailures >= h.failureThreshold {
			h.udpBlockedAt = time.Now()
		}
	}
}

// IsBlocked returns true if transport is temporarily blocked
func (h *HealthMonitor) IsBlocked(protocol Protocol) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch protocol {
	case ProtocolTCP:
		if h.tcpBlockedAt.IsZero() {
			return false
		}
		return time.Since(h.tcpBlockedAt) < h.recoveryInterval
	case ProtocolUDP:
		if h.udpBlockedAt.IsZero() {
			return false
		}
		return time.Since(h.udpBlockedAt) < h.recoveryInterval
	}
	return false
}

// TryRecover attempts to recover blocked transports
func (h *HealthMonitor) TryRecover() {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Reset TCP block if recovery interval passed
	if !h.tcpBlockedAt.IsZero() && time.Since(h.tcpBlockedAt) >= h.recoveryInterval {
		h.tcpBlockedAt = time.Time{}
		h.tcpFailures = 0
	}

	// Reset UDP block if recovery interval passed
	if !h.udpBlockedAt.IsZero() && time.Since(h.udpBlockedAt) >= h.recoveryInterval {
		h.udpBlockedAt = time.Time{}
		h.udpFailures = 0
	}
}
