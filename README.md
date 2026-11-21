# walite

A lightweight, zero-dependency, crash-resilient write-ahead log and partitioned commit stream engine written in pure Go.

## Features
- **Zero Third-Party Dependencies:** Built entirely with the Go standard library.
- **Append-Only Storage:** Immutable segment files with binary layout and sparse indexing.
- **Data Integrity:** Hardware/software corruption protection via CRC32 IEEE checksums.
- **Concurrent Readers:** Lock-free / lock-minimal reading up to partition high-watermark.
- **Configurable Durability:** Immediate fsync, batch-based, or interval-based flush modes.

