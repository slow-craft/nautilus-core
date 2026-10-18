//go:build slim_client

package outbound

import (
	"errors"
	"fmt"
)

var ErrSlimClientDisabled = errors.New("outbound type disabled in slim client build")

type (
	ShadowSocksROption struct{ BasicOption }
	Socks5Option       struct{ BasicOption }
	VlessOption        struct{ BasicOption }
	SnellOption        struct{ BasicOption }
	WireGuardOption    struct{ BasicOption }
	GostRelayOption    struct{ BasicOption }
	SshOption          struct{ BasicOption }
	SudokuOption       struct{ BasicOption }
	MasqueOption       struct{ BasicOption }
	TrustTunnelOption  struct{ BasicOption }
	OpenVPNOption      struct{ BasicOption }
)

func disabledSlimClient(name string) (ProxyAdapter, error) {
	return nil, fmt.Errorf("%w: %s", ErrSlimClientDisabled, name)
}

func NewShadowSocksR(ShadowSocksROption) (ProxyAdapter, error) {
	return disabledSlimClient("ssr")
}

func NewSocks5(Socks5Option) (ProxyAdapter, error) {
	return disabledSlimClient("socks5")
}

func NewVless(VlessOption) (ProxyAdapter, error) {
	return disabledSlimClient("vless")
}

func NewSnell(SnellOption) (ProxyAdapter, error) {
	return disabledSlimClient("snell")
}

func NewWireGuard(WireGuardOption) (ProxyAdapter, error) {
	return disabledSlimClient("wireguard")
}

func NewGostRelay(GostRelayOption) (ProxyAdapter, error) {
	return disabledSlimClient("gost-relay")
}

func NewSsh(SshOption) (ProxyAdapter, error) {
	return disabledSlimClient("ssh")
}

func NewSudoku(SudokuOption) (ProxyAdapter, error) {
	return disabledSlimClient("sudoku")
}

func NewMasque(MasqueOption) (ProxyAdapter, error) {
	return disabledSlimClient("masque")
}

func NewTrustTunnel(TrustTunnelOption) (ProxyAdapter, error) {
	return disabledSlimClient("trusttunnel")
}

func NewOpenVPN(OpenVPNOption) (ProxyAdapter, error) {
	return disabledSlimClient("openvpn")
}
