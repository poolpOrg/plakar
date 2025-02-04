package plakarfs

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"syscall"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"
)

type Dir struct {
	parent   *Dir
	name     string
	fullpath string
	repo     *repository.Repository
	snap     *snapshot.Snapshot
	vfs      *vfs.Filesystem
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	if d.name == "/" {
		d.fullpath = d.name
		a.Inode = 1
		a.Mode = os.ModeDir | 0o700
		a.Uid = uint32(os.Geteuid())
		a.Gid = uint32(os.Getgid())
	} else if d.parent.name == "/" {
		snapshotID, err := hex.DecodeString(d.name)
		if err != nil {
			return err
		}
		if len(snapshotID) != 32 {
			return fmt.Errorf("invalid snapshot id length %d", len(snapshotID))
		}
		snap, err := snapshot.Load(d.repo, objects.Checksum(snapshotID))
		if err != nil {
			return err
		}
		snapfs, err := snap.Filesystem()
		if err != nil {
			return err
		}

		d.snap = snap
		d.repo = d.parent.repo
		d.vfs = snapfs
		d.fullpath = "/"

		a.Inode = rand.Uint64()
		a.Mode = os.ModeDir | 0o700
		a.Uid = uint32(os.Geteuid())
		a.Gid = uint32(os.Getgid())
		a.Ctime = snap.Header.Timestamp
		a.Mtime = snap.Header.Timestamp
		a.Atime = snap.Header.Timestamp
		a.Size = snap.Header.GetSource(0).Summary.Directory.Size + snap.Header.GetSource(0).Summary.Below.Size
	} else {
		d.snap = d.parent.snap
		d.repo = d.parent.repo
		d.vfs = d.parent.vfs
		d.fullpath = d.parent.fullpath + "/" + d.name

		d.fullpath = filepath.Clean(d.fullpath)

		fi, err := d.vfs.GetEntry(d.fullpath)
		if err != nil {
			return syscall.ENOENT
		}

		if !fi.Stat().IsDir() {
			panic(fmt.Sprintf("unexpected type %T", fi))
		}

		a.Rdev = uint32(fi.Stat().Dev())
		a.Inode = fi.Stat().Ino()
		a.Mode = fi.Stat().Mode()
		a.Uid = uint32(fi.Stat().Uid())
		a.Gid = uint32(fi.Stat().Gid())
		a.Ctime = fi.Stat().ModTime()
		a.Mtime = fi.Stat().ModTime()
		a.Size = uint64(fi.Stat().Size())
	}
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if d.name == "/" {
		return &Dir{parent: d, name: name, repo: d.repo}, nil
	} else if d.parent.name == "/" {
		return &Dir{parent: d, name: name}, nil
	} else {
		cleanpath := filepath.Clean(d.fullpath + "/" + name)
		entry, err := d.vfs.GetEntry(cleanpath)
		if err != nil {
			return nil, err
		}

		if entry.Stat().IsDir() {
			return &Dir{parent: d, name: name}, nil
		}
		return &File{parent: d, name: name}, nil
	}
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	if d.name == "/" {

		d.repo.RebuildState()

		snapshotIDs, err := d.repo.GetSnapshots()
		if err != nil {
			return nil, err
		}
		dirDirs := make([]fuse.Dirent, 0)
		for idx, snapshotID := range snapshotIDs {
			dirDirs = append(dirDirs, fuse.Dirent{
				Inode: uint64(idx),
				Name:  fmt.Sprintf("%x", snapshotID),
				Type:  fuse.DT_Dir,
			})
		}
		return dirDirs, nil
	}

	children, err := d.vfs.Children(d.fullpath)
	if err != nil {
		return nil, err
	}

	dirDirs := make([]fuse.Dirent, 0)
	for child, err := range children {
		if err != nil {
			return nil, err
		}
		cleanpath := filepath.Clean(d.fullpath + "/" + child)
		entry, err := d.vfs.GetEntry(cleanpath)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		dirEnt := fuse.Dirent{
			Inode: entry.Stat().Ino(),
			Name:  child,
			Type:  fuse.DT_File,
		}
		if entry.Stat().IsDir() {
			dirEnt.Type = fuse.DT_Dir
		}

		dirDirs = append(dirDirs, dirEnt)
	}
	return dirDirs, nil
}
