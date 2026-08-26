//go:build slim_client

package adapter

import (
	"errors"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
)

func TestSlimClientRejectsExcludedOutboundTypes(t *testing.T) {
	t.Parallel()

	excludedTypes := []string{
		"ssr",
		"socks5",
		"vless",
		"snell",
		"wireguard",
		"gost-relay",
		"ssh",
		"sudoku",
		"masque",
		"trusttunnel",
		"openvpn",
	}

	for _, proxyType := range excludedTypes {
		proxyType := proxyType
		t.Run(proxyType, func(t *testing.T) {
			t.Parallel()

			_, err := ParseProxy(map[string]any{
				"name": "excluded",
				"type": proxyType,
			})
			if !errors.Is(err, outbound.ErrSlimClientDisabled) {
				t.Fatalf("ParseProxy(%q) error = %v, want ErrSlimClientDisabled", proxyType, err)
			}
		})
	}
}

func TestSlimClientRetainsMobileOutboundTypes(t *testing.T) {
	t.Parallel()

	retainedTypes := []string{
		"ss",
		"http",
		"vmess",
		"trojan",
		"hysteria",
		"hysteria2",
		"tuic",
		"mieru",
		"anytls",
		"direct",
		"dns",
		"reject",
	}

	for _, proxyType := range retainedTypes {
		proxyType := proxyType
		t.Run(proxyType, func(t *testing.T) {
			t.Parallel()

			_, err := ParseProxy(map[string]any{
				"name": "retained",
				"type": proxyType,
			})
			if errors.Is(err, outbound.ErrSlimClientDisabled) {
				t.Fatalf("ParseProxy(%q) was disabled in slim_client build", proxyType)
			}
		})
	}
}
