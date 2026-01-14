package shadowtlsplus

import (
	"github.com/metacubex/mihomo/log"
)

const logPrefix = "[STP]"

// debugf logs debug messages with STP prefix
func debugf(format string, args ...any) {
	log.Debugln(logPrefix+" "+format, args...)
}

// infof logs info messages with STP prefix
func infof(format string, args ...any) {
	log.Infoln(logPrefix+" "+format, args...)
}

// warnf logs warning messages with STP prefix
func warnf(format string, args ...any) {
	log.Warnln(logPrefix+" "+format, args...)
}

// errorf logs error messages with STP prefix
func errorf(format string, args ...any) {
	log.Errorln(logPrefix+" "+format, args...)
}
