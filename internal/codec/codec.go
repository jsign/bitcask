package codec

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/prologic/bitcask/internal/model"
)

const (
	KeySize      = 4
	ValueSize    = 8
	checksumSize = 4
)

// NewEncoder creates a streaming Entry encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: bufio.NewWriter(w)}
}

// Encoder wraps an underlying io.Writer and allows you to stream
// Entry encodings on it.
type Encoder struct {
	w *bufio.Writer
}

// Encode takes any Entry and streams it to the underlying writer.
// Messages are framed with a key-length and value-length prefix.
func (e *Encoder) Encode(msg model.Entry) (int64, error) {
	var bufKeyValue = make([]byte, ValueSize)

	bufKeySize := bufKeyValue[:KeySize]
	binary.BigEndian.PutUint32(bufKeySize, uint32(len(msg.Key)))
	if _, err := e.w.Write(bufKeySize); err != nil {
		return 0, errors.Wrap(err, "failed writing key length prefix")
	}

	bufValueSize := bufKeyValue[:ValueSize]
	binary.BigEndian.PutUint64(bufValueSize, uint64(len(msg.Value)))
	if _, err := e.w.Write(bufValueSize); err != nil {
		return 0, errors.Wrap(err, "failed writing value length prefix")
	}

	if _, err := e.w.Write([]byte(msg.Key)); err != nil {
		return 0, errors.Wrap(err, "failed writing key data")
	}
	if _, err := e.w.Write(msg.Value); err != nil {
		return 0, errors.Wrap(err, "failed writing value data")
	}

	bufChecksumSize := make([]byte, checksumSize)
	binary.BigEndian.PutUint32(bufChecksumSize, msg.Checksum)
	if _, err := e.w.Write(bufChecksumSize); err != nil {
		return 0, errors.Wrap(err, "failed writing checksum data")
	}

	if err := e.w.Flush(); err != nil {
		return 0, errors.Wrap(err, "failed flushing data")
	}

	return int64(KeySize + ValueSize + len(msg.Key) + len(msg.Value) + checksumSize), nil
}

// NewDecoder creates a streaming Entry decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// Decoder wraps an underlying io.Reader and allows you to stream
// Entry decodings on it.
type Decoder struct {
	r io.Reader
}

func (d *Decoder) Decode(v *model.Entry) (int64, error) {
	prefixBuf := make([]byte, KeySize+ValueSize)

	_, err := io.ReadFull(d.r, prefixBuf)
	if err != nil {
		return 0, err
	}

	actualKeySize, actualValueSize := GetKeyValueSizes(prefixBuf)
	buf := make([]byte, actualKeySize+actualValueSize+checksumSize)
	if _, err = io.ReadFull(d.r, buf); err != nil {
		return 0, errors.Wrap(translateError(err), "failed reading saved data")
	}

	DecodeWithoutPrefix(buf, actualValueSize, v)
	return int64(KeySize + ValueSize + actualKeySize + actualValueSize + checksumSize), nil
}

func GetKeyValueSizes(buf []byte) (uint64, uint64) {
	actualKeySize := binary.BigEndian.Uint32(buf[:KeySize])
	actualValueSize := binary.BigEndian.Uint64(buf[KeySize:])

	return uint64(actualKeySize), actualValueSize
}

func DecodeWithoutPrefix(buf []byte, valueOffset uint64, v *model.Entry) {
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
