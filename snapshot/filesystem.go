package snapshot

import (
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

func (s *Snapshot) Filesystem() (*vfs.Filesystem, error) {
	if s.filesystem != nil {
		return s.filesystem, nil
	} else if fs, err := vfs.NewFilesystem(s.repository, s.Header.GetSource(0).VFS); err != nil {
		return nil, err
	} else {
		s.filesystem = fs
		return fs, nil
	}
}
