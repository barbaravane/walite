// Package: main
// File: main.go
// Core Work: Daemon entrypoint for walited, command line flag parsing, and OS signal management.
// Exported: none (executable main)
package main

/*
Functions Manifest & Usage:
- main(): Parses command-line flags, initializes walited server, and oversees clean signal termination.
*/

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/barbaravane/walite/internal/server"
)

func main() {
	addr := flag.String("addr", ":9092", "TCP listen address")
	dataDir := flag.String("dir", "./data", "Storage directory for topic partitions and offsets")
	maxSegment := flag.Uint64("max-segment", 10*1024*1024, "Maximum segment size in bytes before rollover")
	flushInterval := flag.Duration("flush", 500*time.Millisecond, "Background physical flush interval")
	syncAlways := flag.Bool("sync", false, "Force physical fsync on every append")
	offsetRetention := flag.Duration("offset-retention", 0, "Retention age for consumer group offset segments (0 = disabled)")
	offsetCleanInterval := flag.Duration("offset-clean-interval", 10*time.Minute, "Background GC interval for consumer offsets")
	flag.Parse()

	cfg := server.Config{
		Addr:                *addr,
		DataDir:             *dataDir,
		MaxSegmentBytes:     *maxSegment,
		IndexIntervalBytes:  4096,
		FlushInterval:       *flushInterval,
		SyncAlways:          *syncAlways,
		ReadTimeout:         30 * time.Second,
		WriteTimeout:        30 * time.Second,
		OffsetRetentionAge:  *offsetRetention,
		OffsetCleanInterval: *offsetCleanInterval,
	}

	srv, err := server.NewServer(cfg)
	if err != nil {
		log.Fatalf("walited: failed to initialize server: %v", err)
	}

	if err := srv.Start(); err != nil {
		log.Fatalf("walited: failed to start server: %v", err)
	}

	fmt.Printf("walited listening on %s (data: %s)\n", srv.Addr().String(), *dataDir)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nwalited: shutting down...")
	if err := srv.Close(); err != nil {
		log.Printf("walited: error during shutdown: %v", err)
	}
	fmt.Println("walited: shutdown complete.")
}
