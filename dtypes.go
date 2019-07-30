package bitcask

import (
	"encoding/binary"
	"errors"
)

var (
	ErrDecodeError = errors.New("error: decode error")
	ErrEncodeError = errors.New("error: encode error")
)

func (b *Bitcask) GetInt(key string) (value int64, err error) {
	buf, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	value, n := binary.Varint(buf)
	if n < 0 {
		return 0, ErrDecodeError
	}
	return value, nil
}

func (b *Bitcask) PutInt(key string, value int64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, value)
	return b.Put(key, buf)
}

func (b *Bitcask) GetUint(key string) (value uint64, err error) {
	buf, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	value, n := binary.Uvarint(buf)
	if n != len(buf) {
		return 0, ErrDecodeError
	}
	return value, nil
}

func (b *Bitcask) PutUint(key string, value uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, value)
	return b.Put(key, buf)
}
