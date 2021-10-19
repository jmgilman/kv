package file

import (
	"fmt"
	"io"
	"path"

	"github.com/jmgilman/kv"
	"github.com/spf13/afero"
)

// SegmentBackend implements kv.SegmentBackend by providing a filesystem for
// storing and retrieving Segment's.
type SegmentBackend struct {
	fs   afero.Fs
	root string
}

// Load opens the file with the given id from the internal filesystem.
func (s *SegmentBackend) Load(id kv.SegmentID) (io.ReadSeekCloser, error) {
	filePath := path.Join(s.root, s.getFileName(id))
	return s.fs.Open(filePath)
}

// getFileName returns the format in which Segment's are stored by id on the
// local filesystem.
func (s *SegmentBackend) getFileName(id kv.SegmentID) string {
	return fmt.Sprintf("segment-%s.dat", id.String())
}

// New creates a new file with the given id on the internal filesystem.
func (s *SegmentBackend) New(id kv.SegmentID) (io.WriteCloser, error) {
	filePath := path.Join(s.root, s.getFileName(id))
	return s.fs.Create(filePath)
}

func NewSegmentBackend(root string) SegmentBackend {
	return SegmentBackend{
		fs:   afero.NewOsFs(),
		root: root,
	}
}
