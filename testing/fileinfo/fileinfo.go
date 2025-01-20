package fileinfo

import (
	"io/fs"
	"syscall"
	"time"
)

type MockFileInfo struct {
	name    string      // base name of the file
	size    int64       // length in bytes for regular files; system-dependent for others
	mode    fs.FileMode // file mode bits
	modTime time.Time   // modification time
	isDir   bool        // abbreviation for Mode().IsDir()
	sys     any         // underlying data source (can return nil)
}

func New() MockFileInfo {
	return MockFileInfo{
		name:    "test",
		size:    100,
		mode:    0644,
		modTime: time.Now(),
		sys:     &syscall.Stat_t{},
	}
}

func (m MockFileInfo) Name() string {
	return m.name
}

func (m MockFileInfo) Size() int64 {
	return m.size
}

func (m MockFileInfo) Mode() fs.FileMode {
	return m.mode
}

func (m MockFileInfo) ModTime() time.Time {
	return m.modTime
}

func (m MockFileInfo) IsDir() bool {
	return m.isDir
}

func (m MockFileInfo) Sys() any {
	return m.sys
}
