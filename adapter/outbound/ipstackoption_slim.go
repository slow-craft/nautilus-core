//go:build slim_client

package outbound

// IPStackOption mirrors the ip-stack configuration option for slim_client
// builds, where the full IP stack machinery in wireguard.go (which normally
// defines this type) is excluded by the !slim_client build tag. It is kept in
// sync with that definition so config structs that reference it - such as the
// ZeroTier stub - still compile in slim builds. The normalize/validate methods
// are intentionally omitted here because no slim-included code path invokes
// them.
type IPStackOption struct {
	Mode                 string `proxy:"mode,omitempty"`
	CongestionController string `proxy:"congestion-controller,omitempty"`
}
