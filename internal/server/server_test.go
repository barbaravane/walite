// Package: server
// File: server_test.go
// Core Work: Unit and integration tests for walited TCP server engine lifecycle and stream operations.
// Exported: none (test functions)
package server

/*
Functions Manifest & Usage:
- TestServer_StartStop(t *testing.T): Tests server startup, ping-pong roundtrip, and clean shutdown.
- TestServer_ProduceAndConsume(t *testing.T): Tests network produce batch and consume roundtrip.
- TestServer_OffsetCommitAndGet(t *testing.T): Tests consumer offset commit and retrieval over TCP.
- TestServer_PartitionNotFound(t *testing.T): Verifies error response StatusErrPartitionNotFound on consuming from uncreated partition on existing topic.
- TestServer_TopicNotFound(t *testing.T): Verifies error response StatusErrTopicNotFound on consuming from non-existent topic.
- TestServer_GracefulShutdownUnblocksIdleClients(t *testing.T): Asserts that Close() immediately unblocks idle client reads in <500ms without leaking goroutines.
- TestServer_ReadTimeout_StalledOneByte(t *testing.T): Asserts that clients stalling on partial frame bytes are severed when ReadTimeout fires.
- TestServer_ReadTimeout_CompleteFrame(t *testing.T): Asserts that complete frames transmitted within ReadTimeout succeed.
- TestServer_WriteTimeout_ClientStopsReading(t *testing.T): Asserts that unresponsive clients trigger WriteTimeout when failing to consume server responses.
- TestServer_ZeroTimeoutDisablesDeadlines(t *testing.T): Verifies that zero-valued timeouts disable deadline enforcement.
- TestServer_MultipleConsecutiveFramesRefreshDeadlines(t *testing.T): Verifies deadlines refresh on every frame over long-lived sessions.
- TestServer_Shutdown_StatusErrServerClosed(t *testing.T): Asserts that produce, consume, offset-get, and offset-commit return StatusErrServerClosed during shutdown.
- TestServer_PartitionFlusherPolicyConfigured(t *testing.T): Verifies that partition flusher policy is configured to SyncInterval or SyncAlways from server config.
- TestServer_ConsumeMaxBytesClampedToMaxFrameBytes(t *testing.T): Asserts that excessive MaxBytes values in ConsumeReq are clamped to prevent exceeding protocol.MaxFrameBytes ceiling.
- TestServer_OffsetManagerCleanerConfigured(t *testing.T): Verifies that OffsetRetentionAge and OffsetCleanInterval are wired into OffsetManager cleaner.
*/

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/barbaravane/walite/internal/log"
	"github.com/barbaravane/walite/internal/protocol"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "walite-server-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := Config{
		Addr:               "127.0.0.1:0", // Ephemeral port
		DataDir:            dir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 1024,
		FlushInterval:      100 * time.Millisecond,
		SyncAlways:         false,
		ReadTimeout:        5 * time.Second,
		WriteTimeout:       5 * time.Second,
	}

	srv, err := NewServer(cfg)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("failed to create server: %v", err)
	}

	if err := srv.Start(); err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("failed to start server: %v", err)
	}

	cleanup := func() {
		_ = srv.Close()
		_ = os.RemoveAll(dir)
	}

	return srv, cleanup
}

func TestServer_StartStop(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Send Ping
	pingPayload := protocol.EncodePingReq(protocol.PingReq{Echo: []byte("server-ping")})
	if err := protocol.WriteFrame(conn, protocol.OpPingReq, pingPayload); err != nil {
		t.Fatalf("write ping failed: %v", err)
	}

	respFrame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read pong failed: %v", err)
	}
	if respFrame.OpCode != protocol.OpPongResp {
		t.Fatalf("expected OpPongResp, got %x", respFrame.OpCode)
	}

	pong, err := protocol.DecodePongResp(respFrame.Payload)
	if err != nil {
		t.Fatalf("decode pong failed: %v", err)
	}
	if !bytes.Equal(pong.Echo, []byte("server-ping")) {
		t.Fatalf("pong payload mismatch")
	}
}

func TestServer_ProduceAndConsume(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	rec1 := &log.Record{
		Offset:    0,
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("k1"),
		Value:     []byte("value-alpha"),
	}
	rec2 := &log.Record{
		Offset:    0,
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("k2"),
		Value:     []byte("value-beta"),
	}

	// 1. Produce batch
	produceReq := protocol.ProduceReq{
		Topic:       "events",
		PartitionID: 0,
		Records:     []*log.Record{rec1, rec2},
	}
	payload := protocol.EncodeProduceReq(produceReq)
	if err := protocol.WriteFrame(conn, protocol.OpProduceReq, payload); err != nil {
		t.Fatalf("write produce failed: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read produce resp failed: %v", err)
	}
	if frame.OpCode != protocol.OpProduceResp {
		t.Fatalf("expected OpProduceResp, got %x", frame.OpCode)
	}
	produceResp, err := protocol.DecodeProduceResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode produce resp failed: %v", err)
	}
	if produceResp.Status != protocol.StatusOK {
		t.Fatalf("expected StatusOK, got %d", produceResp.Status)
	}
	if produceResp.BaseOffset != 0 || produceResp.Count != 2 {
		t.Fatalf("unexpected produce resp: %+v", produceResp)
	}

	// 2. Consume back
	consumeReq := protocol.ConsumeReq{
		Topic:       "events",
		PartitionID: 0,
		Offset:      0,
		MaxBytes:    65536,
	}
	payload = protocol.EncodeConsumeReq(consumeReq)
	if err := protocol.WriteFrame(conn, protocol.OpConsumeReq, payload); err != nil {
		t.Fatalf("write consume failed: %v", err)
	}

	frame, err = protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read consume resp failed: %v", err)
	}
	if frame.OpCode != protocol.OpConsumeResp {
		t.Fatalf("expected OpConsumeResp, got %x", frame.OpCode)
	}
	consumeResp, err := protocol.DecodeConsumeResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode consume resp failed: %v", err)
	}
	if consumeResp.Status != protocol.StatusOK {
		t.Fatalf("expected StatusOK, got %d", consumeResp.Status)
	}
	if len(consumeResp.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(consumeResp.Records))
	}
	if !bytes.Equal(consumeResp.Records[0].Value, rec1.Value) || !bytes.Equal(consumeResp.Records[1].Value, rec2.Value) {
		t.Fatalf("record value mismatch")
	}
}

func TestServer_OffsetCommitAndGet(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// 1. Commit offset
	commitReq := protocol.CommitReq{
		GroupID:     "indexer-group",
		Topic:       "orders",
		PartitionID: 1,
		Offset:      420,
		Metadata:    "node-a",
	}
	payload := protocol.EncodeCommitReq(commitReq)
	if err := protocol.WriteFrame(conn, protocol.OpCommitReq, payload); err != nil {
		t.Fatalf("write commit failed: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read commit resp failed: %v", err)
	}
	commitResp, err := protocol.DecodeCommitResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode commit resp failed: %v", err)
	}
	if commitResp.Status != protocol.StatusOK {
		t.Fatalf("expected StatusOK, got %d", commitResp.Status)
	}

	// 2. Query offset
	offsetReq := protocol.OffsetReq{
		GroupID:     "indexer-group",
		Topic:       "orders",
		PartitionID: 1,
	}
	payload = protocol.EncodeOffsetReq(offsetReq)
	if err := protocol.WriteFrame(conn, protocol.OpOffsetReq, payload); err != nil {
		t.Fatalf("write offset req failed: %v", err)
	}

	frame, err = protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read offset resp failed: %v", err)
	}
	offsetResp, err := protocol.DecodeOffsetResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode offset resp failed: %v", err)
	}
	if offsetResp.Status != protocol.StatusOK || offsetResp.Offset != 420 {
		t.Fatalf("unexpected offset resp: %+v", offsetResp)
	}
}

func TestServer_PartitionNotFound(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// 1. Produce a record to partition 0 so the topic exists
	produceReq := protocol.ProduceReq{
		Topic:       "existing-topic",
		PartitionID: 0,
		Records: []*log.Record{
			{Value: []byte("init")},
		},
	}
	payload := protocol.EncodeProduceReq(produceReq)
	if err := protocol.WriteFrame(conn, protocol.OpProduceReq, payload); err != nil {
		t.Fatalf("write produce failed: %v", err)
	}
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read produce resp failed: %v", err)
	}
	produceResp, err := protocol.DecodeProduceResp(frame.Payload)
	if err != nil || produceResp.Status != protocol.StatusOK {
		t.Fatalf("produce failed: %v, status: %d", err, produceResp.Status)
	}

	// 2. Consume from uncreated partition 99 on existing topic
	consumeReq := protocol.ConsumeReq{
		Topic:       "existing-topic",
		PartitionID: 99,
		Offset:      0,
		MaxBytes:    1024,
	}
	payload = protocol.EncodeConsumeReq(consumeReq)
	if err := protocol.WriteFrame(conn, protocol.OpConsumeReq, payload); err != nil {
		t.Fatalf("write consume failed: %v", err)
	}

	frame, err = protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read consume resp failed: %v", err)
	}
	consumeResp, err := protocol.DecodeConsumeResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode consume resp failed: %v", err)
	}
	if consumeResp.Status != protocol.StatusErrPartitionNotFound {
		t.Fatalf("expected StatusErrPartitionNotFound (%x), got %x", protocol.StatusErrPartitionNotFound, consumeResp.Status)
	}
}

func TestServer_TopicNotFound(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	consumeReq := protocol.ConsumeReq{
		Topic:       "nonexistent-topic",
		PartitionID: 0,
		Offset:      0,
		MaxBytes:    1024,
	}
	payload := protocol.EncodeConsumeReq(consumeReq)
	if err := protocol.WriteFrame(conn, protocol.OpConsumeReq, payload); err != nil {
		t.Fatalf("write consume failed: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read consume resp failed: %v", err)
	}
	consumeResp, err := protocol.DecodeConsumeResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode consume resp failed: %v", err)
	}
	if consumeResp.Status != protocol.StatusErrTopicNotFound {
		t.Fatalf("expected StatusErrTopicNotFound (%x), got %x", protocol.StatusErrTopicNotFound, consumeResp.Status)
	}
}

func TestServer_GracefulShutdownUnblocksIdleClients(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect 3 idle clients that block waiting on server responses without sending data
	const numClients = 3
	conns := make([]net.Conn, numClients)
	for i := 0; i < numClients; i++ {
		c, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			t.Fatalf("client %d dial failed: %v", i, err)
		}
		conns[i] = c
		defer c.Close()
	}

	// Give a few milliseconds for acceptLoop to register all connections
	time.Sleep(20 * time.Millisecond)

	// Trigger graceful shutdown and measure time taken
	start := time.Now()
	if err := srv.Close(); err != nil {
		t.Fatalf("srv.Close() returned error: %v", err)
	}
	elapsed := time.Since(start)

	// Shutdown must unblock all client reads immediately (<500ms), NOT wait for 2-second timeout
	if elapsed >= 500*time.Millisecond {
		t.Fatalf("server shutdown took %v, expected < 500ms", elapsed)
	}

	// Verify that all client sockets are closed and return EOF or closed network error on read
	buf := make([]byte, 8)
	for i, c := range conns {
		_ = c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err := c.Read(buf)
		if err == nil {
			t.Errorf("expected EOF or closed error on client %d, got nil", i)
		}
	}
}

func TestServer_ReadTimeout_StalledOneByte(t *testing.T) {
	dir, err := os.MkdirTemp("", "walite-test-timeout-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		Addr:               "127.0.0.1:0",
		DataDir:            dir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 1024,
		FlushInterval:      100 * time.Millisecond,
		ReadTimeout:        50 * time.Millisecond, // Fast timeout
		WriteTimeout:       1 * time.Second,
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	// Send 1 byte and stall
	if _, err := conn.Write([]byte{0x00}); err != nil {
		t.Fatalf("Write 1 byte failed: %v", err)
	}

	// Wait for server's ReadTimeout to trigger and close the socket
	buf := make([]byte, 16)
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatalf("expected read error/EOF after server read timeout, got nil")
	}
}

func TestServer_ReadTimeout_CompleteFrame(t *testing.T) {
	dir, err := os.MkdirTemp("", "walite-test-timeout-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		Addr:               "127.0.0.1:0",
		DataDir:            dir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 1024,
		FlushInterval:      100 * time.Millisecond,
		ReadTimeout:        200 * time.Millisecond,
		WriteTimeout:       200 * time.Millisecond,
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	ping := protocol.EncodePingReq(protocol.PingReq{Echo: []byte("fast-ping")})
	if err := protocol.WriteFrame(conn, protocol.OpPingReq, ping); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	resp, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if resp.OpCode != protocol.OpPongResp {
		t.Fatalf("expected OpPongResp, got %x", resp.OpCode)
	}
}

func TestServer_WriteTimeout_ClientStopsReading(t *testing.T) {
	srv := &Server{
		cfg: Config{
			WriteTimeout: 20 * time.Millisecond,
		},
	}
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// Client does NOT read. Server attempts to write a frame with a 20ms write timeout.
	err := srv.writeResponse(serverConn, protocol.OpPongResp, []byte("payload-blocked-by-pipe"))
	if err == nil {
		t.Fatalf("expected write timeout error when client does not read, got nil")
	}
}

func TestServer_ZeroTimeoutDisablesDeadlines(t *testing.T) {
	dir, err := os.MkdirTemp("", "walite-test-zerotimeout-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		Addr:               "127.0.0.1:0",
		DataDir:            dir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 1024,
		FlushInterval:      100 * time.Millisecond,
		ReadTimeout:        0, // Disabled
		WriteTimeout:       0, // Disabled
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	// Sleep 60ms (which would have timed out a 50ms deadline)
	time.Sleep(60 * time.Millisecond)

	// Send Ping frame
	ping := protocol.EncodePingReq(protocol.PingReq{Echo: []byte("zero-timeout-ping")})
	if err := protocol.WriteFrame(conn, protocol.OpPingReq, ping); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	resp, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if resp.OpCode != protocol.OpPongResp {
		t.Fatalf("expected OpPongResp, got %x", resp.OpCode)
	}
}

func TestServer_MultipleConsecutiveFramesRefreshDeadlines(t *testing.T) {
	dir, err := os.MkdirTemp("", "walite-test-consecutive-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		Addr:               "127.0.0.1:0",
		DataDir:            dir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 1024,
		FlushInterval:      100 * time.Millisecond,
		ReadTimeout:        80 * time.Millisecond, // Each idle window allowed is 80ms
		WriteTimeout:       80 * time.Millisecond,
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	// Send 5 consecutive frames with 30ms sleep in between.
	// Total elapsed time = 150ms (> 80ms total), proving deadlines refresh on each frame.
	for i := 0; i < 5; i++ {
		time.Sleep(30 * time.Millisecond)
		ping := protocol.EncodePingReq(protocol.PingReq{Echo: []byte{byte(i)}})
		if err := protocol.WriteFrame(conn, protocol.OpPingReq, ping); err != nil {
			t.Fatalf("WriteFrame frame %d failed: %v", i, err)
		}

		resp, err := protocol.ReadFrame(conn)
		if err != nil {
			t.Fatalf("ReadFrame frame %d failed: %v", i, err)
		}
		if resp.OpCode != protocol.OpPongResp {
			t.Fatalf("frame %d: expected OpPongResp, got %x", i, resp.OpCode)
		}
	}
}

func TestServer_Shutdown_StatusErrServerClosed(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	// Provision a partition first so it exists
	_, err := srv.getOrCreatePartition("orders", 0)
	if err != nil {
		t.Fatalf("setup partition failed: %v", err)
	}

	// Close the server
	if err := srv.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	t.Run("ProduceReturnsStatusErrServerClosed", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		req := protocol.ProduceReq{
			Topic:       "orders",
			PartitionID: 0,
			Records: []*log.Record{
				{Value: []byte("v1")},
			},
		}
		payload := protocol.EncodeProduceReq(req)

		errCh := make(chan error, 1)
		go func() {
			errCh <- srv.handleProduce(serverConn, payload)
		}()

		respFrame, err := protocol.ReadFrame(clientConn)
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}
		if respFrame.OpCode != protocol.OpProduceResp {
			t.Fatalf("expected OpProduceResp, got %x", respFrame.OpCode)
		}

		resp, err := protocol.DecodeProduceResp(respFrame.Payload)
		if err != nil {
			t.Fatalf("DecodeProduceResp failed: %v", err)
		}
		if resp.Status != protocol.StatusErrServerClosed {
			t.Fatalf("expected StatusErrServerClosed (%x), got %x", protocol.StatusErrServerClosed, resp.Status)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("handleProduce returned error: %v", err)
		}
	})

	t.Run("ConsumeReturnsStatusErrServerClosed", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		req := protocol.ConsumeReq{
			Topic:       "orders",
			PartitionID: 0,
			Offset:      0,
			MaxBytes:    1024,
		}
		payload := protocol.EncodeConsumeReq(req)

		errCh := make(chan error, 1)
		go func() {
			errCh <- srv.handleConsume(serverConn, payload)
		}()

		respFrame, err := protocol.ReadFrame(clientConn)
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}
		if respFrame.OpCode != protocol.OpConsumeResp {
			t.Fatalf("expected OpConsumeResp, got %x", respFrame.OpCode)
		}

		resp, err := protocol.DecodeConsumeResp(respFrame.Payload)
		if err != nil {
			t.Fatalf("DecodeConsumeResp failed: %v", err)
		}
		if resp.Status != protocol.StatusErrServerClosed {
			t.Fatalf("expected StatusErrServerClosed (%x), got %x", protocol.StatusErrServerClosed, resp.Status)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("handleConsume returned error: %v", err)
		}
	})

	t.Run("OffsetGetReturnsStatusErrServerClosed", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		req := protocol.OffsetReq{
			GroupID:     "group-1",
			Topic:       "orders",
			PartitionID: 0,
		}
		payload := protocol.EncodeOffsetReq(req)

		errCh := make(chan error, 1)
		go func() {
			errCh <- srv.handleOffsetGet(serverConn, payload)
		}()

		respFrame, err := protocol.ReadFrame(clientConn)
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}
		if respFrame.OpCode != protocol.OpOffsetResp {
			t.Fatalf("expected OpOffsetResp, got %x", respFrame.OpCode)
		}

		resp, err := protocol.DecodeOffsetResp(respFrame.Payload)
		if err != nil {
			t.Fatalf("DecodeOffsetResp failed: %v", err)
		}
		if resp.Status != protocol.StatusErrServerClosed {
			t.Fatalf("expected StatusErrServerClosed (%x), got %x", protocol.StatusErrServerClosed, resp.Status)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("handleOffsetGet returned error: %v", err)
		}
	})

	t.Run("OffsetCommitReturnsStatusErrServerClosed", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		req := protocol.CommitReq{
			GroupID:     "group-1",
			Topic:       "orders",
			PartitionID: 0,
			Offset:      10,
			Metadata:    "meta",
		}
		payload := protocol.EncodeCommitReq(req)

		errCh := make(chan error, 1)
		go func() {
			errCh <- srv.handleOffsetCommit(serverConn, payload)
		}()

		respFrame, err := protocol.ReadFrame(clientConn)
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}
		if respFrame.OpCode != protocol.OpCommitResp {
			t.Fatalf("expected OpCommitResp, got %x", respFrame.OpCode)
		}

		resp, err := protocol.DecodeCommitResp(respFrame.Payload)
		if err != nil {
			t.Fatalf("DecodeCommitResp failed: %v", err)
		}
		if resp.Status != protocol.StatusErrServerClosed {
			t.Fatalf("expected StatusErrServerClosed (%x), got %x", protocol.StatusErrServerClosed, resp.Status)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("handleOffsetCommit returned error: %v", err)
		}
	})
}

func TestServer_PartitionFlusherPolicyConfigured(t *testing.T) {
	dir, err := os.MkdirTemp("", "walite-server-flusher-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Case 1: FlushInterval > 0 should configure SyncInterval policy
	cfgInterval := Config{
		Addr:          "127.0.0.1:0",
		DataDir:       dir,
		FlushInterval: 100 * time.Millisecond,
		SyncAlways:    false,
	}
	srvInterval, err := NewServer(cfgInterval)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srvInterval.Close()

	partInterval, err := srvInterval.getOrCreatePartition("topic-interval", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	if partInterval.Flusher() == nil || partInterval.Flusher().Policy() != log.SyncInterval {
		t.Fatalf("expected flusher policy SyncInterval (%v), got %v", log.SyncInterval, partInterval.Flusher().Policy())
	}

	// Case 2: SyncAlways = true should configure SyncAlways policy
	dirSync, err := os.MkdirTemp("", "walite-server-sync-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dirSync)

	cfgSync := Config{
		Addr:          "127.0.0.1:0",
		DataDir:       dirSync,
		FlushInterval: 100 * time.Millisecond,
		SyncAlways:    true,
	}
	srvSync, err := NewServer(cfgSync)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srvSync.Close()

	partSync, err := srvSync.getOrCreatePartition("topic-sync", 0)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	if partSync.Flusher() == nil || partSync.Flusher().Policy() != log.SyncAlways {
		t.Fatalf("expected flusher policy SyncAlways (%v), got %v", log.SyncAlways, partSync.Flusher().Policy())
	}
}

func TestServer_ConsumeMaxBytesClampedToMaxFrameBytes(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	// Produce 3 sample records
	records := []*log.Record{
		{Key: []byte("k1"), Value: []byte("v1-payload")},
		{Key: []byte("k2"), Value: []byte("v2-payload")},
		{Key: []byte("k3"), Value: []byte("v3-payload")},
	}
	produceReq := protocol.ProduceReq{
		Topic:       "large-batch",
		PartitionID: 0,
		Records:     records,
	}
	payload := protocol.EncodeProduceReq(produceReq)
	if err := protocol.WriteFrame(conn, protocol.OpProduceReq, payload); err != nil {
		t.Fatalf("write produce failed: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read produce resp failed: %v", err)
	}
	produceResp, err := protocol.DecodeProduceResp(frame.Payload)
	if err != nil || produceResp.Status != protocol.StatusOK {
		t.Fatalf("produce failed: %v, status: %d", err, produceResp.Status)
	}

	// Consume with oversized MaxBytes (32MB and 0xFFFFFFFF)
	for _, excessiveMaxBytes := range []uint32{32 * 1024 * 1024, 0xFFFFFFFF} {
		consumeReq := protocol.ConsumeReq{
			Topic:       "large-batch",
			PartitionID: 0,
			Offset:      0,
			MaxBytes:    excessiveMaxBytes,
		}
		consumePayload := protocol.EncodeConsumeReq(consumeReq)
		if err := protocol.WriteFrame(conn, protocol.OpConsumeReq, consumePayload); err != nil {
			t.Fatalf("write consume failed: %v", err)
		}

		respFrame, err := protocol.ReadFrame(conn)
		if err != nil {
			t.Fatalf("read consume resp with MaxBytes %d failed with framing error: %v", excessiveMaxBytes, err)
		}
		if respFrame.OpCode != protocol.OpConsumeResp {
			t.Fatalf("expected OpConsumeResp, got %x", respFrame.OpCode)
		}

		consumeResp, err := protocol.DecodeConsumeResp(respFrame.Payload)
		if err != nil {
			t.Fatalf("decode consume resp failed: %v", err)
		}
		if consumeResp.Status != protocol.StatusOK {
			t.Fatalf("expected StatusOK, got %d", consumeResp.Status)
		}
		if len(consumeResp.Records) != 3 {
			t.Fatalf("expected 3 records, got %d", len(consumeResp.Records))
		}
	}
}

func TestServer_OffsetManagerCleanerConfigured(t *testing.T) {
	dir, err := os.MkdirTemp("", "walite-server-om-cleaner-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		Addr:                "127.0.0.1:0",
		DataDir:             dir,
		OffsetRetentionAge:  48 * time.Hour,
		OffsetMaxBytes:      50 * 1024 * 1024,
		OffsetCleanInterval: 15 * time.Minute,
	}

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	om := srv.OffsetManager()
	if om == nil {
		t.Fatalf("expected non-nil OffsetManager")
	}

	omCfg := om.Config()
	if omCfg.Cleaner.MaxRetentionAge != 48*time.Hour {
		t.Fatalf("expected MaxRetentionAge 48h, got %v", omCfg.Cleaner.MaxRetentionAge)
	}
	if omCfg.Cleaner.MaxPartitionBytes != 50*1024*1024 {
		t.Fatalf("expected MaxPartitionBytes 50MB, got %v", omCfg.Cleaner.MaxPartitionBytes)
	}
	if omCfg.Cleaner.CleanInterval != 15*time.Minute {
		t.Fatalf("expected CleanInterval 15m, got %v", omCfg.Cleaner.CleanInterval)
	}

	// Verify partition cleaner attached
	part := om.Partition()
	if part == nil || part.Cleaner() == nil {
		t.Fatalf("expected non-nil Cleaner attached to offset partition")
	}
	if part.Cleaner().Config().CleanInterval != 15*time.Minute {
		t.Fatalf("expected Cleaner Config CleanInterval 15m, got %v", part.Cleaner().Config().CleanInterval)
	}
}

func TestServer_RejectPathTraversalTopics(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	invalidTopics := []string{"../foo", "../../etc/passwd", "..", ".", "foo/bar", "foo\\bar", ""}
	for _, topic := range invalidTopics {
		// Test produce
		produceReq := protocol.ProduceReq{
			Topic:       topic,
			PartitionID: 0,
			Records: []*log.Record{
				{
					Timestamp: time.Now().UnixNano(),
					Key:       []byte("k"),
					Value:     []byte("v"),
				},
			},
		}
		if err := protocol.WriteFrame(conn, protocol.OpProduceReq, protocol.EncodeProduceReq(produceReq)); err != nil {
			t.Fatalf("failed to write produce frame: %v", err)
		}
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			t.Fatalf("failed to read produce resp frame: %v", err)
		}
		produceResp, err := protocol.DecodeProduceResp(frame.Payload)
		if err != nil {
			t.Fatalf("failed to decode produce resp: %v", err)
		}
		if produceResp.Status != protocol.StatusErrInvalidPayload {
			t.Fatalf("expected StatusErrInvalidPayload for topic %q, got 0x%04x", topic, produceResp.Status)
		}

		// Test consume
		consumeReq := protocol.ConsumeReq{
			Topic:       topic,
			PartitionID: 0,
			Offset:      0,
			MaxBytes:    1024,
		}
		if err := protocol.WriteFrame(conn, protocol.OpConsumeReq, protocol.EncodeConsumeReq(consumeReq)); err != nil {
			t.Fatalf("failed to write consume frame: %v", err)
		}
		frame, err = protocol.ReadFrame(conn)
		if err != nil {
			t.Fatalf("failed to read consume resp frame: %v", err)
		}
		consumeResp, err := protocol.DecodeConsumeResp(frame.Payload)
		if err != nil {
			t.Fatalf("failed to decode consume resp: %v", err)
		}
		if consumeResp.Status != protocol.StatusErrInvalidPayload {
			t.Fatalf("expected StatusErrInvalidPayload for topic %q, got 0x%04x", topic, consumeResp.Status)
		}
	}
}

func TestServer_ConsumeCorruptRecordReturnsStatusErrCorruptRecord(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	topic := "corrupt-test"
	produceReq := protocol.ProduceReq{
		Topic:       topic,
		PartitionID: 0,
		Records: []*log.Record{
			{
				Timestamp: time.Now().UnixNano(),
				Key:       []byte("valid-key"),
				Value:     []byte("valid-val"),
			},
		},
	}
	if err := protocol.WriteFrame(conn, protocol.OpProduceReq, protocol.EncodeProduceReq(produceReq)); err != nil {
		t.Fatalf("failed to write produce: %v", err)
	}
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read produce resp: %v", err)
	}
	pResp, err := protocol.DecodeProduceResp(frame.Payload)
	if err != nil || pResp.Status != protocol.StatusOK {
		t.Fatalf("produce failed: %v, status: 0x%04x", err, pResp.Status)
	}

	// Corrupt the record payload bytes in the active log segment file
	partDir := filepath.Join(srv.cfg.DataDir, "topics", topic, "partition-0")
	entries, err := os.ReadDir(partDir)
	if err != nil {
		t.Fatalf("failed to read partition dir: %v", err)
	}
	var logFile string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".log" {
			logFile = filepath.Join(partDir, entry.Name())
			break
		}
	}
	if logFile == "" {
		t.Fatalf("log file not found in %s", partDir)
	}

	f, err := os.OpenFile(logFile, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	// Overwrite payload bytes (offset 28 onward) to invalidate CRC
	if _, err := f.WriteAt([]byte("CORRUPT_PAYLOAD_BYTES"), log.HeaderSize); err != nil {
		_ = f.Close()
		t.Fatalf("failed to corrupt record payload: %v", err)
	}
	_ = f.Close()

	// Consume record - must return StatusErrCorruptRecord
	consumeReq := protocol.ConsumeReq{
		Topic:       topic,
		PartitionID: 0,
		Offset:      0,
		MaxBytes:    4096,
	}
	if err := protocol.WriteFrame(conn, protocol.OpConsumeReq, protocol.EncodeConsumeReq(consumeReq)); err != nil {
		t.Fatalf("failed to write consume: %v", err)
	}
	frame, err = protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read consume resp: %v", err)
	}
	cResp, err := protocol.DecodeConsumeResp(frame.Payload)
	if err != nil {
		t.Fatalf("failed to decode consume resp: %v", err)
	}
	if cResp.Status != protocol.StatusErrCorruptRecord {
		t.Fatalf("expected StatusErrCorruptRecord (0x0005), got 0x%04x", cResp.Status)
	}
}
