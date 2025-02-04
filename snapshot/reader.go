package snapshot

import (
	"io"
	"io/fs"
	"os"
	"path"

	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

func (snapshot *Snapshot) NewReader(pathname string) (io.ReadCloser, error) {
	return NewReader(snapshot, pathname)
}

func NewReader(snap *Snapshot, pathname string) (io.ReadCloser, error) {
	pathname = path.Clean(pathname)

	fsc, err := vfs.NewFilesystem(snap.Repository(), snap.Header.GetSource(0).VFS)
	if err != nil {
		return nil, err
	}

	file, err := fsc.Open(pathname)
	if err != nil {
		return nil, err
	}

	if _, isdir := file.(fs.ReadDirFile); isdir {
		file.Close()
		return nil, os.ErrInvalid
	}
	return file, nil
}
