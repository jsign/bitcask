package internal

import (
	"bytes"
	"encoding/gob"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

type Item struct {
	FileID int
	Offset int64
	Size   int64
}

type Keydir struct {
	sync.RWMutex
	kv map[string]Item
}

func NewKeydir() *Keydir {
	return &Keydir{
		kv: make(map[string]Item),
	}
}

func (k *Keydir) Add(key string, fileid int, offset, size int64) Item {
	item := Item{
		FileID: fileid,
		Offset: offset,
		Size:   size,
	}

	k.Lock()
	k.kv[key] = item
	k.Unlock()

	return item
}

func (k *Keydir) Get(key string) (Item, bool) {
	k.RLock()
	item, ok := k.kv[key]
	k.RUnlock()
	return item, ok
}

func (k *Keydir) Delete(key string) {
	k.Lock()
	delete(k.kv, key)
	k.Unlock()
}

func (k *Keydir) Len() int {
	return len(k.kv)
}

func (k *Keydir) Keys() chan string {
	ch := make(chan string)
	go func() {
		k.RLock()
		for key := range k.kv {
			ch <- key
		}
		close(ch)
		k.RUnlock()
	}()
	return ch
}

func (k *Keydir) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(k.kv)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (k *Keydir) Load(fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := gob.NewDecoder(f)
	if err := dec.Decode(&k.kv); err != nil {
		return err
	}

	return nil
}

func (k *Keydir) Save(fn string) error {
	data, err := k.Bytes()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(fn, data, 0644)
}

func NewKeydirFromBytes(r io.Reader) (*Keydir, error) {
	k := NewKeydir()
	dec := gob.NewDecoder(r)
	err := dec.Decode(&k.kv)
	if err != nil {
		return nil, err
	}
	return k, nil
}
