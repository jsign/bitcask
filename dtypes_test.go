package bitcask

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInts(t *testing.T) {
	var (
		db      *Bitcask
		testdir string
		err     error
	)

	assert := assert.New(t)

	testdir, err = ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	t.Run("Open", func(t *testing.T) {
		db, err = Open(testdir)
		assert.NoError(err)
	})

	t.Run("PutInt", func(t *testing.T) {
		err = db.PutInt("foo", 1234)
		assert.NoError(err)
	})

	t.Run("GetInt", func(t *testing.T) {
		val, err := db.GetInt("foo")
		assert.NoError(err)
		assert.Equal(int64(1234), val)
	})

	t.Run("Close", func(t *testing.T) {
		err = db.Close()
		assert.NoError(err)
	})
}
