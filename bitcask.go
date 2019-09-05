package bitcask

import (
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"
	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/prologic/bitcask/internal"
	"github.com/prologic/bitcask/internal/data"
	"github.com/prologic/bitcask/internal/index"
)

var (
	// ErrKeyNotFound is the error returned when a key is not found
	ErrKeyNotFound = errors.New("error: key not found")

	// ErrKeyTooLarge is the error returned for a key that exceeds the
	// maximum allowed key size (configured with WithMaxKeySize).
	ErrKeyTooLarge = errors.New("error: key too large")

	// ErrValueTooLarge is the error returned for a value that exceeds the
	// maximum allowed value size (configured with WithMaxValueSize).
	ErrValueTooLarge = errors.New("error: value too large")

	// ErrChecksumFailed is the error returned if a key/value retrieved does
	// not match its CRC checksum
	ErrChecksumFailed = errors.New("error: checksum failed")

	// ErrDatabaseLocked is the error returned if the database is locked
	// (typically opened by another process)
	ErrDatabaseLocked = errors.New("error: database locked")
)

// Bitcask is a struct that represents a on-disk LSM and WAL data structure
// and in-memory hash of key/value pairs as per the Bitcask paper and seen
// in the Riak database.
type Bitcask struct {
	mu sync.RWMutex

	*flock.Flock

	config    *config
	options   []Option
	path      string
	curr      *data.Datafile
	datafiles map[int]*data.Datafile
	trie      art.Tree
}

// Stats is a struct returned by Stats() on an open Bitcask instance
type Stats struct {
	Datafiles int
	Keys      int
	Size      int64
}

// Stats returns statistics about the database including the number of
// data files, keys and overall size on disk of the data
func (b *Bitcask) Stats() (stats Stats, err error) {
	var size int64

	size, err = internal.DirSize(b.path)
	if err != nil {
		return
	}

	stats.Datafiles = len(b.datafiles)
	b.mu.RLock()
	stats.Keys = b.trie.Size()
	b.mu.RUnlock()
	stats.Size = size

	return
}

// Close closes the database and removes the lock. It is important to call
// Close() as this is the only way to cleanup the lock held by the open
// database.
func (b *Bitcask) Close() error {
	defer func() {
		b.Flock.Unlock()
		os.Remove(b.Flock.Path())
	}()

	f, err := os.OpenFile(filepath.Join(b.path, "index"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := index.WriteIndex(b.trie, f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}

	for _, df := range b.datafiles {
		if err := df.Close(); err != nil {
			return err
		}
	}

	return b.curr.Close()
}

// Sync flushes all buffers to disk ensuring all data is written
func (b *Bitcask) Sync() error {
	return b.curr.Sync()
}

// Get retrieves the value of the given key. If the key is not found or an/I/O
// error occurs a null byte slice is returned along with the error.
func (b *Bitcask) Get(key []byte) ([]byte, error) {
	var df *data.Datafile

	b.mu.RLock()
	value, found := b.trie.Search(key)
	b.mu.RUnlock()
	if !found {
		return nil, ErrKeyNotFound
	}

	item := value.(internal.Item)

	if item.FileID == b.curr.FileID() {
		df = b.curr
	} else {
		df = b.datafiles[item.FileID]
	}

	e, err := df.ReadAt(item.Offset, item.Size)
	if err != nil {
		return nil, err
	}

	checksum := crc32.ChecksumIEEE(e.Value)
	if checksum != e.Checksum {
		return nil, ErrChecksumFailed
	}

	return e.Value, nil
}

// Has returns true if the key exists in the database, false otherwise.
func (b *Bitcask) Has(key []byte) bool {
	b.mu.RLock()
	_, found := b.trie.Search(key)
	b.mu.RUnlock()
	return found
}

// Put stores the key and value in the database.
func (b *Bitcask) Put(key, value []byte) error {
	if len(key) > b.config.maxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > b.config.maxValueSize {
		return ErrValueTooLarge
	}

	offset, n, err := b.put(key, value)
	if err != nil {
		return err
	}

	if b.config.sync {
		if err := b.curr.Sync(); err != nil {
			return err
		}
	}

	item := internal.Item{FileID: b.curr.FileID(), Offset: offset, Size: n}
	b.mu.Lock()
	b.trie.Insert(key, item)
	b.mu.Unlock()

	return nil
}

// Delete deletes the named key. If the key doesn't exist or an I/O error
// occurs the error is returned.
func (b *Bitcask) Delete(key []byte) error {
	_, _, err := b.put(key, []byte{})
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.trie.Delete(key)
	b.mu.Unlock()

	return nil
}

// Scan performs a prefix scan of keys matching the given prefix and calling
// the function `f` with the keys found. If the function returns an error
// no further keys are processed and the first error returned.
func (b *Bitcask) Scan(prefix []byte, f func(key []byte) error) (err error) {
	b.trie.ForEachPrefix(prefix, func(node art.Node) bool {
		// Skip the root node
		if len(node.Key()) == 0 {
			return true
		}

		if err = f(node.Key()); err != nil {
			return false
		}
		return true
	})
	return
}

// Len returns the total number of keys in the database
func (b *Bitcask) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.trie.Size()
}

// Keys returns all keys in the database as a channel of keys
func (b *Bitcask) Keys() chan []byte {
	ch := make(chan []byte)
	go func() {
		b.mu.RLock()
		defer b.mu.RUnlock()

		for it := b.trie.Iterator(); it.HasNext(); {
			node, _ := it.Next()

			// Skip the root node
			if len(node.Key()) == 0 {
				continue
			}

			ch <- node.Key()
		}
		close(ch)
	}()

	return ch
}

// Fold iterates over all keys in the database calling the function `f` for
// each key. If the function returns an error, no further keys are processed
// and the error returned.
func (b *Bitcask) Fold(f func(key []byte) error) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEach(func(node art.Node) bool {
		if err := f(node.Key()); err != nil {
			return false
		}
		return true
	})

	return nil
}

func (b *Bitcask) put(key, value []byte) (int64, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	size := b.curr.Size()
	if size >= int64(b.config.maxDatafileSize) {
		err := b.curr.Close()
		if err != nil {
			return -1, 0, err
		}

		id := b.curr.FileID()

		df, err := data.NewDatafile(b.path, id, true)
		if err != nil {
			return -1, 0, err
		}

		b.datafiles[id] = df

		id = b.curr.FileID() + 1
		curr, err := data.NewDatafile(b.path, id, false)
		if err != nil {
			return -1, 0, err
		}
		b.curr = curr
	}

	e := internal.NewEntry(key, value)
	return b.curr.Write(e)
}

func (b *Bitcask) readConfig() error {
	if internal.Exists(filepath.Join(b.path, "config.json")) {
		data, err := ioutil.ReadFile(filepath.Join(b.path, "config.json"))
		if err != nil {
			return err
		}

		if err := json.Unmarshal(data, &b.config); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bitcask) writeConfig() error {
	data, err := json.Marshal(b.config)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filepath.Join(b.path, "config.json"), data, 0600)
}

func (b *Bitcask) reopen() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	fns, err := internal.GetDatafiles(b.path)
	if err != nil {
		return err
	}

	ids, err := internal.ParseIds(fns)
	if err != nil {
		return err
	}

	datafiles := make(map[int]*data.Datafile, len(ids))

	for _, id := range ids {
		df, err := data.NewDatafile(b.path, id, true)
		if err != nil {
			return err
		}
		datafiles[id] = df
	}

	t, found, err := index.ReadFromFile(b.path, b.config.maxKeySize, b.config.maxValueSize)
	if err != nil {
		return err
	}
	if !found {
		for i, df := range datafiles {
			var offset int64
			for {
				e, n, err := df.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}

				// Tombstone value  (deleted key)
				if len(e.Value) == 0 {
					t.Delete(e.Key)
					offset += n
					continue
				}
				item := internal.Item{FileID: ids[i], Offset: offset, Size: n}
				t.Insert(e.Key, item)
				offset += n
			}
		}
	}

	var id int
	if len(ids) > 0 {
		id = ids[(len(ids) - 1)]
	}

	curr, err := data.NewDatafile(b.path, id, false)
	if err != nil {
		return err
	}

	b.trie = t
	b.curr = curr
	b.datafiles = datafiles

	return nil
}

// Merge merges all datafiles in the database. Old keys are squashed
// and deleted keys removes. Duplicate key/value pairs are also removed.
// Call this function periodically to reclaim disk space.
func (b *Bitcask) Merge() error {
	// Temporary merged database path
	temp, err := ioutil.TempDir(b.path, "merge")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)

	// Create a merged database
	mdb, err := Open(temp, b.options...)
	if err != nil {
		return err
	}

	// Rewrite all key/value pairs into merged database
	// Doing this automatically strips deleted keys and
	// old key/value pairs
	err = b.Fold(func(key []byte) error {
		value, err := b.Get(key)
		if err != nil {
			return err
		}

		if err := mdb.Put(key, value); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	err = mdb.Close()
	if err != nil {
		return err
	}

	// Close the database
	err = b.Close()
	if err != nil {
		return err
	}

	// Remove all data files
	files, err := ioutil.ReadDir(b.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if !file.IsDir() {
			err := os.RemoveAll(path.Join([]string{b.path, file.Name()}...))
			if err != nil {
				return err
			}
		}
	}

	// Rename all merged data files
	files, err = ioutil.ReadDir(mdb.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		err := os.Rename(
			path.Join([]string{mdb.path, file.Name()}...),
			path.Join([]string{b.path, file.Name()}...),
		)
		if err != nil {
			return err
		}
	}

	// And finally reopen the database
	return b.reopen()
}

// Open opens the database at the given path with optional options.
// Options can be provided with the `WithXXX` functions that provide
// configuration options as functions.
func Open(path string, options ...Option) (*Bitcask, error) {
	var (
		cfg *config
		err error
	)

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	cfg, err = getConfig(path)
	if err != nil {
		cfg = newDefaultConfig()
	}

	bitcask := &Bitcask{
		Flock:   flock.New(filepath.Join(path, "lock")),
		config:  cfg,
		options: options,
		path:    path,
	}

	for _, opt := range options {
		if err := opt(bitcask.config); err != nil {
			return nil, err
		}
	}

	locked, err := bitcask.Flock.TryLock()
	if err != nil {
		return nil, err
	}

	if !locked {
		return nil, ErrDatabaseLocked
	}

	if err := bitcask.writeConfig(); err != nil {
		return nil, err
	}

	if err := bitcask.reopen(); err != nil {
		return nil, err
	}

	return bitcask, nil
}
