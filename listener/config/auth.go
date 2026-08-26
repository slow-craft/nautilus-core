package config

import (
	"github.com/slow-craft/nautilus-core/component/auth"
	"github.com/slow-craft/nautilus-core/listener/reality"
)

// AuthServer for http/socks/mixed server
type AuthServer struct {
	Enable         bool
	Listen         string
	AuthStore      auth.AuthStore
	Certificate    string
	PrivateKey     string
	ClientAuthType string
	ClientAuthCert string
	EchKey         string
	RealityConfig  reality.Config
}
