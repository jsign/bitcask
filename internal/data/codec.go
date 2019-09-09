package data

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/prologic/bitcask/internal"
)

const (
	keySize      = 4
	valueSize    = 8
	checksumSize = 4
)

// NewEncoder creates a streaming Entry encoder.
func newEncoder(w io.Writer) *encoder {
	return &encoder{w: bufio.NewWriter(w)}
}

// encoder wraps an underlying io.Writer and allows you to stream
// Entry encodings on it.
type encoder struct {
	w *bufio.Writer
}

// Encode takes any Entry and streams it to the underlying writer.
// Messages are framed with a key-length and value-length prefix.
func (e *encoder) encode(msg internal.Entry) (int64, error) {
	var bufKeyValue = make([]byte, keySize+valueSize)
	binary.BigEndian.PutUint32(bufKeyValue[:keySize], uint32(len(msg.Key)))
	binary.BigEndian.PutUint64(bufKeyValue[keySize:keySize+valueSize], uint64(len(msg.Value)))
	if _, err := e.w.Write(bufKeyValue); err != nil {
		return 0, errors.Wrap(err, "failed writing key & value length prefix")
	}

	if _, err := e.w.Write(msg.Key); err != nil {
		return 0, errors.Wrap(err, "failed writing key data")
	}
	if _, err := e.w.Write(msg.Value); err != nil {
		return 0, errors.Wrap(err, "failed writing value data")
	}

	bufChecksumSize := bufKeyValue[:checksumSize]
	binary.BigEndian.PutUint32(bufChecksumSize, msg.Checksum)
	if _, err := e.w.Write(bufChecksumSize); err != nil {
		return 0, errors.Wrap(err, "failed writing checksum data")
	}

	if err := e.w.Flush(); err != nil {
		return 0, errors.Wrap(err, "failed flushing data")
	}

	return int64(keySize + valueSize + len(msg.Key) + len(msg.Value) + checksumSize), nil
}

// NewDecoder creates a streaming Entry decoder.
func newDecoder(r io.Reader) *decoder {
	return &decoder{r: r}
}

// decoder wraps an underlying io.Reader and allows you to stream
// Entry decodings on it.
type decoder struct {
	r io.Reader
}

func (d *decoder) decode(v *internal.Entry) (int64, error) {
	prefixBuf := make([]byte, keySize+valueSize)

	_, err := io.ReadFull(d.r, prefixBuf)
	if err != nil {
		return 0, err
	}

	actualKeySize, actualValueSize := getKeyValueSizes(prefixBuf)
	buf := make([]byte, actualKeySize+actualValueSize+checksumSize)
	if _, err = io.ReadFull(d.r, buf); err != nil {
		return 0, errors.Wrap(translateError(err), "failed reading saved data")
	}

	decodeWithoutPrefix(buf, actualKeySize, v)
	return int64(keySize + valueSize + actualKeySize + actualValueSize + checksumSize), nil
}

func getKeyValueSizes(buf []byte) (uint64, uint64) {
	actualKeySize := binary.BigEndian.Uint32(buf[:keySize])
	actualValueSize := binary.BigEndian.Uint64(buf[keySize:])

	return uint64(actualKeySize), actualValueSize
}

func decodeWithoutPrefix(buf []byte, valueOffset uint64, v *internal.Entry) {
	v.Key = buf[:valueOffset]
	v.Value = buf[valueOffset : len(buf)-checksumSize]
	v.Checksum = binary.BigEndian.Uint32(buf[len(buf)-checksumSize:])
}

func translateError(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
