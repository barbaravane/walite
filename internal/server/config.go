// Package: server
// File: config.go
// Core Work: Configuration parameters and defaults for the walited high-throughput TCP server.
// Exported: Config, DefaultConfig
package server

/*
Functions Manifest & Usage:
- DefaultConfig(): Config: Produces sane default parameters for server operation.
*/

import (
	"time"
)

// Config holds operational settings for the walited server.
type Config struct {
	Addr                string
	DataDir             string
	MaxSegmentBytes     uint64
	IndexIntervalBytes  uint32
	FlushInterval       time.Duration
	SyncAlways          bool
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	OffsetRetentionAge  time.Duration
	OffsetMaxBytes      uint64
	OffsetCleanInterval time.Duration
}

// DefaultConfig returns production-ready default server configuration.
func DefaultConfig() Config {
	return Config{
		Addr:                ":9092",
		DataDir:             "./data",
		MaxSegmentBytes:     10 * 1024 * 1024, // 10MB
		IndexIntervalBytes:  4096,             // 4KB
		FlushInterval:       500 * time.Millisecond,
		SyncAlways:          false,
		ReadTimeout:         30 * time.Second,
		WriteTimeout:        30 * time.Second,
		OffsetRetentionAge:  0,
		OffsetCleanInterval: 10 * time.Minute,
	}
}
