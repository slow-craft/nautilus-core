package adapter

import (
	"strings"
	"testing"
)

func TestParseProxy_RejectsSmuxOnMultiProtocol(t *testing.T) {
	// Bug 9: smux creates a fixed mux.Client that breaks when multi-protocol
	// fails over to a different underlying protocol. The parser should reject
	// this combination.
	mapping := map[string]any{
		"name": "test-mp",
		"type": "multi-protocol",
		"protocols": []any{
			map[string]any{
				"type":   "socks5",
				"server": "127.0.0.1",
				"port":   1080,
			},
		},
		"smux": map[string]any{
			"enabled": true,
		},
	}

	_, err := ParseProxy(mapping)
	if err == nil {
		t.Fatal("BUG: parser should reject smux on multi-protocol")
	}
	if !strings.Contains(err.Error(), "smux") {
		t.Fatalf("expected smux-related error, got: %v", err)
	}
}
