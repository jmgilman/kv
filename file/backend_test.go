package file

import (
	"fmt"
	"io"
	"testing"

	"github.com/jmgilman/kv"
	"github.com/matryer/is"
	"github.com/spf13/afero"
)

func NewMockSegmentBackend() SegmentBackend {
	return SegmentBackend{
		fs:   afero.NewMemMapFs(),
		root: "test",
	}
}

func TestSegmentBackendLoad(t *testing.T) {
	is := is.New(t)
	id := kv.NewSegmentID()
	filePath := fmt.Sprintf("test/segment-%s.dat", id.String())
	backend := NewMockSegmentBackend()

	// Create test file
	file, err := backend.fs.Create(filePath)
	is.NoErr(err)

	file.WriteString("test")
	file.Close()

	// Load file
	result, err := backend.Load(id)
	is.NoErr(err)

	// File contents are the same
	data, err := io.ReadAll(result)
	is.NoErr(err)
	is.Equal(string(data), "test")

}

func TestSegmentBackendgetFileName(t *testing.T) {
	is := is.New(t)
	id := kv.NewSegmentID()
	filePath := fmt.Sprintf("segment-%s.dat", id.String())
	backend := NewMockSegmentBackend()

	is.Equal(backend.getFileName(id), filePath)
}

func TestSegmentBackendNew(t *testing.T) {
	is := is.New(t)
	id := kv.NewSegmentID()
	filePath := fmt.Sprintf("test/segment-%s.dat", id.String())
	backend := NewMockSegmentBackend()

	// Create file and write test data
	result, err := backend.New(id)
	is.NoErr(err)
	result.Write([]byte("test"))
	result.Close()

	// Verify correct file exists
	s, err := backend.fs.Stat(filePath)
	is.NoErr(err)
	is.Equal(s.Size(), int64(4))
}
