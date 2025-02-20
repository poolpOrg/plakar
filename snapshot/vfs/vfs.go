package vfs

import (
	"io"
	"io/fs"
	"iter"
	"path"
	"strings"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/iterator"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	versioning.Register(resources.RT_VFS_BTREE, versioning.FromString(btree.BTREE_VERSION))
	versioning.Register(resources.RT_VFS_NODE, versioning.FromString(btree.NODE_VERSION))
}

type Score struct {
	Key   string  `msgpack:"key" json:"key"`
	Value float64 `msgpack:"value" json:"value"`
}

type Classification struct {
	Analyzer string   `msgpack:"analyzer" json:"analyzer"`
	Classes  []string `msgpack:"classes" json:"classes"`
	Scores   []Score  `msgpack:"scores" json:"scores"`
}

type ExtendedAttribute struct {
	Name  string `msgpack:"name" json:"name"`
	Value []byte `msgpack:"value" json:"value"`
}

type CustomMetadata struct {
	Key   string `msgpack:"key" json:"key"`
	Value []byte `msgpack:"value" json:"value"`
}

type Filesystem struct {
	tree   *btree.BTree[string, objects.MAC, objects.MAC]
	xattrs *btree.BTree[string, objects.MAC, objects.MAC]
	errors *btree.BTree[string, objects.MAC, objects.MAC]
	repo   *repository.Repository
}

func PathCmp(a, b string) int {
	da := strings.Count(a, "/")
	db := strings.Count(b, "/")

	if da > db {
		return 1
	}
	if da < db {
		return -1
	}
	return strings.Compare(a, b)
}

func isEntryBelow(parent, entry string) bool {
	if !strings.HasPrefix(entry, parent) {
		return false
	}
	if strings.Index(entry[len(parent):], "/") != -1 {
		return false
	}
	return true
}

func NodeFromBytes(data []byte) (*btree.Node[string, objects.MAC, objects.MAC], error) {
	var node btree.Node[string, objects.MAC, objects.MAC]
	err := msgpack.Unmarshal(data, &node)
	return &node, err
}

func NewFilesystem(repo *repository.Repository, root, xattrs, errors objects.MAC) (*Filesystem, error) {
	rd, err := repo.GetBlob(resources.RT_VFS_BTREE, root)
	if err != nil {
		return nil, err
	}

	fsstore := repository.NewRepositoryStore[string, objects.MAC](repo, resources.RT_VFS_NODE)
	tree, err := btree.Deserialize(rd, fsstore, PathCmp)
	if err != nil {
		return nil, err
	}

	rd, err = repo.GetBlob(resources.RT_XATTR_BTREE, xattrs)
	if err != nil {
		return nil, err
	}

	xstore := repository.NewRepositoryStore[string, objects.MAC](repo, resources.RT_XATTR_NODE)
	xtree, err := btree.Deserialize(rd, xstore, strings.Compare)
	if err != nil {
		return nil, err
	}

	rd, err = repo.GetBlob(resources.RT_ERROR_BTREE, errors)
	if err != nil {
		return nil, err
	}

	errstore := repository.NewRepositoryStore[string, objects.MAC](repo, resources.RT_ERROR_NODE)
	errtree, err := btree.Deserialize(rd, errstore, strings.Compare)
	if err != nil {
		return nil, err
	}

	fs := &Filesystem{
		tree:   tree,
		xattrs: xtree,
		errors: errtree,
		repo:   repo,
	}

	return fs, nil
}

func (fsc *Filesystem) lookup(entrypath string) (*Entry, error) {
	if !strings.HasPrefix(entrypath, "/") {
		entrypath = "/" + entrypath
	}
	entrypath = path.Clean(entrypath)

	if entrypath == "" {
		entrypath = "/"
	}

	csum, found, err := fsc.tree.Find(entrypath)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fs.ErrNotExist
	}

	return fsc.ResolveEntry(csum)
}

func (fsc *Filesystem) ResolveEntry(csum objects.MAC) (*Entry, error) {
	rd, err := fsc.repo.GetBlob(resources.RT_VFS_ENTRY, csum)
	if err != nil {
		return nil, err
	}

	bytes, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	entry, err := EntryFromBytes(bytes)
	if err != nil {
		return nil, err
	}

	if entry.HasObject() {
		rd, err := fsc.repo.GetBlob(resources.RT_OBJECT, entry.Object)
		if err != nil {
			return nil, err
		}

		bytes, err := io.ReadAll(rd)
		if err != nil {
			return nil, err
		}

		obj, err := objects.NewObjectFromBytes(bytes)
		if err != nil {
			return nil, err
		}

		entry.ResolvedObject = obj
	}

	return entry, nil

}

func (fsc *Filesystem) ResolveXattr(mac objects.MAC) (*Xattr, error) {
	rd, err := fsc.repo.GetBlob(resources.RT_XATTR_ENTRY, mac)
	if err != nil {
		return nil, err
	}

	bytes, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	xattr, err := XattrFromBytes(bytes)
	if err != nil {
		return nil, err
	}

	rd, err = fsc.repo.GetBlob(resources.RT_OBJECT, xattr.Object)
	if err != nil {
		return nil, err
	}

	bytes, err = io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	xattr.ResolvedObject, err = objects.NewObjectFromBytes(bytes)
	return xattr, err
}

func (fsc *Filesystem) Open(path string) (fs.File, error) {
	entry, err := fsc.lookup(path)
	if err != nil {
		return nil, err
	}

	return entry.Open(fsc, path), nil
}

func (fsc *Filesystem) ReadDir(path string) (entries []fs.DirEntry, err error) {
	fp, err := fsc.Open(path)
	if err != nil {
		return
	}
	dir, ok := fp.(fs.ReadDirFile)
	if !ok {
		return entries, fs.ErrInvalid
	}

	return dir.ReadDir(-1)
}

func (fsc *Filesystem) Files() iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		iter, err := fsc.tree.ScanAll()
		if err != nil {
			yield("", err)
			return
		}

		for iter.Next() {
			path, csum := iter.Current()
			entry, err := fsc.ResolveEntry(csum)
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}
			if entry.FileInfo.Lmode.IsRegular() {
				if !yield(path, nil) {
					return
				}
			}
		}
		if err := iter.Err(); err != nil {
			yield("", err)
			return
		}
	}
}

func (fsc *Filesystem) Pathnames() iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		iter, err := fsc.tree.ScanAll()
		if err != nil {
			yield("", err)
			return
		}

		for iter.Next() {
			path, _ := iter.Current()
			if !yield(path, nil) {
				return
			}
		}

		if err := iter.Err(); err != nil {
			yield("", err)
			return
		}
	}
}

func (fsc *Filesystem) GetEntry(path string) (*Entry, error) {
	return fsc.lookup(path)
}

func (fsc *Filesystem) Children(path string) (iter.Seq2[string, error], error) {
	fp, err := fsc.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	dir, ok := fp.(fs.ReadDirFile)
	if !ok {
		return nil, fs.ErrInvalid
	}

	return func(yield func(string, error) bool) {
		for {
			entries, err := dir.ReadDir(16)
			if err != nil {
				yield("", err)
				return
			}
			for i := range entries {
				if !yield(entries[i].Name(), nil) {
					return
				}
			}
		}
	}, nil
}

func (fsc *Filesystem) IterNodes() iterator.Iterator[objects.MAC, *btree.Node[string, objects.MAC, objects.MAC]] {
	return fsc.tree.IterDFS()
}

func (fsc *Filesystem) XattrNodes() iterator.Iterator[objects.MAC, *btree.Node[string, objects.MAC, objects.MAC]] {
	return fsc.xattrs.IterDFS()
}

func (fsc *Filesystem) FileMacs() (iter.Seq2[objects.MAC, error], error) {
	iter, err := fsc.tree.ScanAll()
	if err != nil {
		return nil, err
	}

	return func(yield func(objects.MAC, error) bool) {
		for iter.Next() {
			_, csum := iter.Current()
			if err != nil {
				yield(objects.MAC{}, err)
				return
			}
			if !yield(csum, nil) {
				return
			}
		}
		if err := iter.Err(); err != nil {
			yield(objects.MAC{}, err)
			return
		}
	}, nil
}

// XXX
func (fsc *Filesystem) BTrees() (*btree.BTree[string, objects.MAC, objects.MAC], *btree.BTree[string, objects.MAC, objects.MAC], *btree.BTree[string, objects.MAC, objects.MAC]) {
	return fsc.tree, fsc.errors, fsc.xattrs
}
