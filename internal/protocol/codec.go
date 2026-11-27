// Package: protocol
// File: codec.go
// Core Work: Binary wire framing, length-prefixed packet validation, and zero-allocation frame encoding/decoding.
// Exported: Frame, ReadFrame, WriteFrame, EncodeFrame, Magic, Version1, HeaderSize, MaxFrameBytes, ErrInvalidMagic, ErrUnsupportedVersion, ErrFrameTooLarge, ErrTruncatedFrame, ErrMalformedPayload, ErrInvalidFrameLen
package protocol

/*
Functions Manifest & Usage:
- EncodeFrame(opCode uint8, payload []byte) []byte: Serializes a frame header and payload into a wire-ready byte slice.
- ReadFrame(r io.Reader) (Frame, error): Reads an 8-byte header from a stream, validates magic/version/length, and reads the full payload.
- WriteFrame(w io.Writer, opCode uint8, payload []byte) error: Writes a length-prefixed protocol frame to an io.Writer.
*/

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

// Magic is the fixed 2-byte protocol identifier 0x574C ('W', 'L').
const Magic uint16 = 0x574C

// Version1 is protocol version 1.
const Version1 uint8 = 0x01

// HeaderSize is the fixed size of the frame header in bytes:
// FrameLen (4B) + Magic (2B) + Version (1B) + OpCode (1B) = 8 Bytes.
const HeaderSize = 8

// MaxFrameBytes is the maximum allowed FrameLen (16MB) to protect against memory exhaustion.
const MaxFrameBytes uint32 = 16 * 1024 * 1024

// OpCodes defined in docs/WIRE_PROTOCOL.md
const (
	OpProduceReq  uint8 = 0x01
	OpProduceResp uint8 = 0x81
	OpConsumeReq  uint8 = 0x02
	OpConsumeResp uint8 = 0x82
	OpOffsetReq   uint8 = 0x03
	OpOffsetResp  uint8 = 0x83
	OpCommitReq   uint8 = 0x04
	OpCommitResp  uint8 = 0x84
	OpPingReq     uint8 = 0x05
	OpPongResp    uint8 = 0x85
)

// Status codes
const (
	StatusOK                   uint16 = 0x0000
	StatusErrUnknown           uint16 = 0x0001
	StatusErrTopicNotFound     uint16 = 0x0002
	StatusErrPartitionNotFound uint16 = 0x0003
	StatusErrOffsetOutOfRange  uint16 = 0x0004
	StatusErrCorruptRecord     uint16 = 0x0005
	StatusErrServerClosed      uint16 = 0x0006
	StatusErrInvalidPayload    uint16 = 0x0007
	StatusErrOffsetNotFound    uint16 = 0x0008
)

var (
	// ErrInvalidMagic is returned when frame magic does not match Magic (0x574C).
	ErrInvalidMagic = errors.New("walite: invalid protocol magic bytes")

	// ErrUnsupportedVersion is returned when frame version is unrecognized.
	ErrUnsupportedVersion = errors.New("walite: unsupported protocol version")

	// ErrFrameTooLarge is returned when FrameLen exceeds MaxFrameBytes.
	ErrFrameTooLarge = errors.New("walite: frame length exceeds maximum limit of 16MB")

	// ErrTruncatedFrame is returned when the stream ends before completing payload reading.
	ErrTruncatedFrame = errors.New("walite: truncated frame payload")

	// ErrMalformedPayload is returned when payload bytes cannot be decoded into expected types.
	ErrMalformedPayload = errors.New("walite: malformed message payload")

	// ErrInvalidFrameLen is returned when FrameLen is smaller than the 4-byte header remainder.
	ErrInvalidFrameLen = errors.New("walite: frame length less than header minimum")
)

// Frame represents a decoded wire protocol packet.
type Frame struct {
	OpCode  uint8
	Payload []byte
}

var headerPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, HeaderSize)
		return &b
	},
}

// EncodeFrame serializes a frame header and payload into a wire-ready byte slice.
func EncodeFrame(opCode uint8, payload []byte) []byte {
	payloadLen := len(payload)
	frameLen := uint32(4 + payloadLen) // 4 bytes of Magic(2)+Version(1)+OpCode(1) + payload
	totalLen := 4 + int(frameLen)      // 4 bytes FrameLen + frameLen

	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:4], frameLen)
	binary.BigEndian.PutUint16(buf[4:6], Magic)
	buf[6] = Version1
	buf[7] = opCode

	if payloadLen > 0 {
		copy(buf[8:], payload)
	}

	return buf
}

// ReadFrame reads an 8-byte header from a stream, validates magic/version/length, and reads the full payload.
func ReadFrame(r io.Reader) (Frame, error) {
	headerPtr := headerPool.Get().(*[]byte)
	header := *headerPtr
	defer headerPool.Put(headerPtr)

	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, ErrTruncatedFrame
		}
		return Frame{}, err
	}

	frameLen := binary.BigEndian.Uint32(header[0:4])
	magic := binary.BigEndian.Uint16(header[4:6])
	version := header[6]
	opCode := header[7]

	if magic != Magic {
		return Frame{}, ErrInvalidMagic
	}
	if version != Version1 {
		return Frame{}, ErrUnsupportedVersion
	}
	if frameLen < 4 {
		return Frame{}, ErrInvalidFrameLen
	}
	if frameLen > MaxFrameBytes {
		return Frame{}, ErrFrameTooLarge
	}

	payloadLen := int(frameLen - 4)
	if payloadLen == 0 {
		return Frame{OpCode: opCode, Payload: nil}, nil
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, ErrTruncatedFrame
		}
		return Frame{}, err
	}

	return Frame{
		OpCode:  opCode,
		Payload: payload,
	}, nil
}

// WriteFrame writes a length-prefixed protocol frame to an io.Writer.
func WriteFrame(w io.Writer, opCode uint8, payload []byte) error {
	buf := EncodeFrame(opCode, payload)
	_, err := w.Write(buf)
	return err
}
