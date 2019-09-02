package internal

import (
	"encoding/binary"
	"io"

	art "github.com/plar/go-adaptive-radix-tree"
)

const (
	int32Size  = 4
	int64Size  = 8
	fileIDSize = int32Size
	offsetSize = int64Size
	sizeSize   = int64Size
)

func ReadBytes(r io.Reader) ([]byte, error) {
	s := make([]byte, int32Size)
	_, err := io.ReadFull(r, s)
	if err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(s)
	b := make([]byte, size)
	_, err = io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func WriteBytes(b []byte, w io.Writer) (int, error) {
	s := make([]byte, int32Size)
	binary.BigEndian.PutUint32(s, uint32(len(b)))
	n, err := w.Write(s)
	if err != nil {
		return n, err
	}
	m, err := w.Write(b)
	if err != nil {
		return (n + m), err
	}
	return (n + m), nil
}

func ReadItem(r io.Reader) (Item, error) {
	buf := make([]byte, (fileIDSize + offsetSize + sizeSize))
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return Item{}, err
	}

	return Item{
		FileID: int(binary.BigEndian.Uint32(buf[:fileIDSize])),
		Offset: int64(binary.BigEndian.Uint64(buf[fileIDSize:(fileIDSize + offsetSize)])),
		Size:   int64(binary.BigEndian.Uint64(buf[(fileIDSize + offsetSize):])),
	}, nil
}

func WriteItem(item Item, w io.Writer) (int, error) {
	buf := make([]byte, (fileIDSize + offsetSize + sizeSize))
	binary.BigEndian.PutUint32(buf[:fileIDSize], uint32(item.FileID))
	binary.BigEndian.PutUint64(buf[fileIDSize:(fileIDSize+offsetSize)], uint64(item.Offset))
	binary.BigEndian.PutUint64(buf[(fileIDSize+offsetSize):], uint64(item.Size))
	n, err := w.Write(buf)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func ReadIndex(r io.Reader, t art.Tree) error {
	for {
		key, err := ReadBytes(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		item, err := ReadItem(r)
		if err != nil {
			return err
		}

		t.Insert(key, item)
	}

	return nil
}

func WriteIndex(t art.Tree, w io.Writer) (err error) {
	t.ForEach(func(node art.Node) bool {
		_, err = WriteBytes(node.Key(), w)
		if err != nil {
			return false
		}

		item := node.Value().(Item)
		_, err := WriteItem(item, w)
		if err != nil {
			return false
		}

		return true
	})
	return
}
