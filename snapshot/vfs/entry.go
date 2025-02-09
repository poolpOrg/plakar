package vfs

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"iter"
	"path"
	"sort"
	"strings"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

const VFS_ENTRY_VERSION = "1.0.0"

func init() {
	versioning.Register(resources.RT_VFS_ENTRY, versioning.FromString(VFS_ENTRY_VERSION))
}

// Entry implements FSEntry and fs.DirEntry, as well as some other
// helper methods.
type Entry struct {
	Version    versioning.Version  `msgpack:"version" json:"version"`
	ParentPath string              `msgpack:"parent_path" json:"parent_path"`
	RecordType importer.RecordType `msgpack:"type" json:"type"`
	FileInfo   objects.FileInfo    `msgpack:"file_info" json:"file_info"`

	/* Directory specific fields */
	Summary *Summary `msgpack:"summary" json:"summary,omitempty"`

	/* File specific fields */
	SymlinkTarget  string           `msgpack:"symlinkTarget,omitempty" json:"symlink_target,omitempty"`
	Object         objects.Checksum `msgpack:"object,omitempty" json:"-"` // nil for !regular files
	ResolvedObject *objects.Object  `msgpack:"-" json:"object,omitempty"` // This the true object, resolved when opening the entry. Beware we serialize it as "Object" only for json to not break API compat'

	/* Windows specific fields */
	AlternateDataStreams []AlternateDataStream `msgpack:"alternate_data_streams,omitempty" json:"alternate_data_streams"`
	SecurityDescriptor   []byte                `msgpack:"security_descriptor,omitempty" json:"security_descriptor"`
	FileAttributes       uint32                `msgpack:"file_attributes,omitempty" json:"file_attributes"`

	/* Unix fields */
	ExtendedAttributes []ExtendedAttribute `msgpack:"extended_attributes,omitempty" json:"extended_attributes"`

	/* Custom metadata and tags */
	Classifications []Classification `msgpack:"classifications,omitempty" json:"classifications"`
	CustomMetadata  []CustomMetadata `msgpack:"custom_metadata,omitempty" json:"custom_metadata"`
	Tags            []string         `msgpack:"tags,omitempty" json:"tags"`
}

func (e *Entry) HasObject() bool {
	return e.Object != objects.Checksum{}
}

// Return empty lists for nil slices.
func (e *Entry) MarshalJSON() ([]byte, error) {
	// Create an alias to avoid recursive MarshalJSON calls
	type Alias Entry

	ret := (*Alias)(e)

	if ret.AlternateDataStreams == nil {
		ret.AlternateDataStreams = []AlternateDataStream{}
	}
	if ret.SecurityDescriptor == nil {
		ret.SecurityDescriptor = []byte{}
	}
	if ret.ExtendedAttributes == nil {
		ret.ExtendedAttributes = []ExtendedAttribute{}
	}
	if ret.Classifications == nil {
		ret.Classifications = []Classification{}
	}
	if ret.CustomMetadata == nil {
		ret.CustomMetadata = []CustomMetadata{}
	}
	if ret.Tags == nil {
		ret.Tags = []string{}
	}

	return json.Marshal(ret)
}

func NewEntry(parentPath string, record *importer.ScanRecord) *Entry {
	target := ""
	if record.Target != "" {
		target = record.Target
	}

	ExtendedAttributes := make([]ExtendedAttribute, 0, len(record.ExtendedAttributes))
	for name, value := range record.ExtendedAttributes {
		ExtendedAttributes = append(ExtendedAttributes, ExtendedAttribute{
			Name:  name,
			Value: value,
		})
	}

	sort.Slice(ExtendedAttributes, func(i, j int) bool {
		return ExtendedAttributes[i].Name < ExtendedAttributes[j].Name
	})

	entry := &Entry{
		Version:            versioning.FromString(VFS_ENTRY_VERSION),
		RecordType:         record.Type,
		FileInfo:           record.FileInfo,
		SymlinkTarget:      target,
		ExtendedAttributes: ExtendedAttributes,
		Tags:               []string{},
		ParentPath:         parentPath,
	}

	if record.Type == importer.RecordTypeDirectory {
		entry.Summary = &Summary{}
	}

	return entry
}

func EntryFromBytes(bytes []byte) (*Entry, error) {
	entry := Entry{}
	err := msgpack.Unmarshal(bytes, &entry)
	return &entry, err
}

func (e *Entry) ToBytes() ([]byte, error) {
	return msgpack.Marshal(e)
}

func (e *Entry) ContentType() string {
	if e.ResolvedObject == nil {
		return ""
	}
	return e.ResolvedObject.ContentType
}

func (e *Entry) Entropy() float64 {
	if e.ResolvedObject == nil {
		return 0
	}
	return e.ResolvedObject.Entropy
}

func (e *Entry) AddClassification(analyzer string, classes []string) {
	e.Classifications = append(e.Classifications, Classification{
		Analyzer: analyzer,
		Classes:  classes,
	})
}

func (e *Entry) Open(fs *Filesystem, path string) fs.File {
	if e.FileInfo.IsDir() {
		return &vdir{
			path:  path,
			entry: e,
			fs:    fs,
		}
	}

	return &vfile{
		path:  path,
		entry: e,
		repo:  fs.repo,
	}
}

func (e *Entry) Getdents(fsc *Filesystem) (iter.Seq2[*Entry, error], error) {
	path := path.Join(e.ParentPath, e.FileInfo.Name())

	prefix := path
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	iter, err := fsc.tree.ScanFrom(prefix)
	if err != nil {
		return nil, err
	}

	return func(yield func(*Entry, error) bool) {
		for iter.Next() {
			path, csum := iter.Current()
			if prefix == path {
				continue
			}
			if !isEntryBelow(prefix, path) {
				break
			}
			if !yield(fsc.resolveEntry(csum)) {
				return
			}
		}
		if err := iter.Err(); err != nil {
			yield(nil, err)
		}
	}, nil
}

func (e *Entry) Stat() *objects.FileInfo {
	return &e.FileInfo
}

func (e *Entry) Name() string {
	return e.FileInfo.Name()
}

func (e *Entry) Size() int64 {
	return e.FileInfo.Size()
}

func (e *Entry) Path() string {
	return path.Join(e.ParentPath, e.FileInfo.Name())
}

func (e *Entry) IsDir() bool {
	return e.FileInfo.IsDir()
}

func (e *Entry) Type() fs.FileMode {
	return e.Stat().Mode()
}

func (e *Entry) Info() (fs.FileInfo, error) {
	return e.FileInfo, nil
}

// FileEntry implements fs.File, FSEntry and ReadSeeker
type vfile struct {
	path   string
	entry  *Entry
	repo   *repository.Repository
	closed bool
	objoff int
	off    int64
	rd     io.ReadSeeker
}

func (vf *vfile) Stat() (fs.FileInfo, error) {
	if vf.closed {
		return nil, fs.ErrClosed
	}
	return vf.entry.FileInfo, nil
}

func (vf *vfile) Name() string {
	return vf.entry.FileInfo.Name()
}

func (vf *vfile) Size() int64 {
	return vf.entry.FileInfo.Size()
}

func (vf *vfile) Path() string {
	return vf.path
}

func (vf *vfile) Read(p []byte) (int, error) {
	if vf.closed {
		return 0, fs.ErrClosed
	}

	if vf.entry.ResolvedObject == nil {
		return 0, fs.ErrInvalid
	}

	for vf.objoff < len(vf.entry.ResolvedObject.Chunks) {
		if vf.rd == nil {
			rd, err := vf.repo.GetBlob(resources.RT_CHUNK,
				vf.entry.ResolvedObject.Chunks[vf.objoff].Checksum)
			if err != nil {
				return -1, err
			}
			vf.rd = rd
		}

		n, err := vf.rd.Read(p)
		if errors.Is(err, io.EOF) {
			vf.objoff++
			vf.rd = nil
			continue
		}
		vf.off += int64(n)
		return n, err
	}

	return 0, io.EOF
}

func (vf *vfile) Seek(offset int64, whence int) (int64, error) {
	if vf.closed {
		return 0, fs.ErrClosed
	}

	if vf.entry.ResolvedObject == nil {
		return 0, fs.ErrInvalid
	}

	chunks := vf.entry.ResolvedObject.Chunks

	switch whence {
	case io.SeekStart:
		vf.rd = nil
		vf.off = 0
		for vf.objoff = 0; vf.objoff < len(chunks); vf.objoff++ {
			clen := int64(chunks[vf.objoff].Length)
			if offset > clen {
				vf.off += clen
				offset -= clen
				continue
			}
			vf.off += offset
			rd, err := vf.repo.GetBlob(resources.RT_CHUNK,
				chunks[vf.objoff].Checksum)
			if err != nil {
				return 0, err
			}
			if _, err := rd.Seek(offset, whence); err != nil {
				return 0, err
			}
			vf.rd = rd
			break
		}

	case io.SeekEnd:
		vf.rd = nil
		vf.off = vf.Size()
		for vf.objoff = len(chunks) - 1; vf.objoff >= 0; vf.objoff-- {
			clen := int64(chunks[vf.objoff].Length)
			if offset > clen {
				vf.off -= clen
				offset -= clen
				continue
			}
			vf.off -= offset
			rd, err := vf.repo.GetBlob(resources.RT_CHUNK,
				chunks[vf.objoff].Checksum)
			if err != nil {
				return 0, err
			}
			if _, err := rd.Seek(offset, whence); err != nil {
				return 0, err
			}
			vf.rd = rd
			break
		}

	case io.SeekCurrent:
		if vf.rd != nil {
			n, err := vf.rd.Seek(offset, whence)
			if err != nil {
				return 0, err
			}
			diff := n - vf.off
			vf.off += diff
			offset -= diff
		}

		if offset == 0 {
			break
		}

		vf.objoff++
		for vf.objoff < len(chunks) {
			clen := int64(chunks[vf.objoff].Length)
			if offset > clen {
				vf.off += clen
				offset -= clen
			}
			vf.off += offset
			rd, err := vf.repo.GetBlob(resources.RT_CHUNK,
				chunks[vf.objoff].Checksum)
			if err != nil {
				return 0, err
			}
			if _, err := rd.Seek(offset, whence); err != nil {
				return 0, err
			}
			vf.rd = rd
		}
	}

	return vf.off, nil
}

func (vf *vfile) Close() error {
	if vf.closed {
		return fs.ErrClosed
	}
	vf.closed = true
	return nil
}

type vdir struct {
	path   string
	entry  *Entry
	fs     *Filesystem
	iter   btree.Iterator[string, objects.Checksum]
	closed bool
}

func (vf *vdir) Stat() (fs.FileInfo, error) {
	if vf.closed {
		return nil, fs.ErrClosed
	}
	return vf.entry.FileInfo, nil
}

func (vf *vdir) Read(p []byte) (int, error) {
	if vf.closed {
		return 0, fs.ErrClosed
	}
	return 0, fs.ErrInvalid
}

func (vf *vdir) Seek(offset int64, whence int) (int64, error) {
	if vf.closed {
		return 0, fs.ErrClosed
	}
	return 0, fs.ErrInvalid
}

func (vf *vdir) Close() error {
	if vf.closed {
		return fs.ErrClosed
	}
	vf.closed = true
	return nil
}

func (vf *vdir) ReadDir(n int) (entries []fs.DirEntry, err error) {
	if vf.closed {
		return entries, fs.ErrClosed
	}

	prefix := vf.path
	if prefix != "/" {
		prefix += "/"
	}

	if vf.iter == nil {
		vf.iter, err = vf.fs.tree.ScanFrom(prefix)
		if err != nil {
			return
		}
	}

	for vf.iter.Next() {
		if n == 0 {
			break
		}
		if n > 0 {
			n--
		}
		path, csum := vf.iter.Current()

		dirent, err := vf.fs.resolveEntry(csum)
		if err != nil {
			return nil, err
		}

		if path == prefix {
			continue
		}
		if !isEntryBelow(prefix, path) {
			break
		}

		entries = append(entries, &vdirent{dirent})
	}

	if len(entries) == 0 && n != -1 {
		err = io.EOF
	}
	if e := vf.iter.Err(); e != nil {
		err = e
	}
	return
}

type vdirent struct {
	*Entry
}

func (dirent *vdirent) Name() string {
	return dirent.FileInfo.Lname
}

func (dirent *vdirent) IsDir() bool {
	return dirent.FileInfo.IsDir()
}

func (dirent *vdirent) Type() fs.FileMode {
	return dirent.FileInfo.Lmode
}

func (dirent *vdirent) Info() (fs.FileInfo, error) {
	return dirent.FileInfo, nil
}
