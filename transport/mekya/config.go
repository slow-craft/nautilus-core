package mekya

import "github.com/slow-craft/nautilus-core/transport/mkcp"

type Config struct {
	KCP                            mkcp.Config
	URL                            string
	H2PoolSize                     int
	MaxWriteDelay                  int
	MaxRequestSize                 int
	PollingIntervalInitial         int
	MaxWriteSize                   int
	MaxWriteDurationMs             int
	MaxSimultaneousWriteConnection int
	PacketWritingBuffer            int
}
