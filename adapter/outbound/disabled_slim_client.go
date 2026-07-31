//go:build slim_client

package outbound

import (
	"errors"
	"fmt"
)

var ErrSlimClientDisabled = errors.New("outbound type disabled in slim client build")

type ShadowSocksROption struct{ BasicOption }
type Socks5Option struct{ BasicOption }
type VlessOption struct{ BasicOption }
type SnellOption struct{ BasicOption }
type WireGuardOption struct{ BasicOption }
type GostRelayOption struct{ BasicOption }
type SshOption struct{ BasicOption }
type SudokuOption struct{ BasicOption }
type MasqueOption struct{ BasicOption }
type TrustTunnelOption struct{ BasicOption }
type OpenVPNOption struct{ BasicOption }

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
