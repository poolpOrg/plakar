package snapshot

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

func push(src *Snapshot, dst *Snapshot, mac objects.MAC, rtype resources.Type, data []byte) error {
	if dst.BlobExists(rtype, mac) {
		return nil
	}
	var err error
	if data == nil {
		data, err = src.GetBlob(rtype, mac)
		if err != nil {
			return err
		}
	}
	return dst.PutBlob(rtype, mac, data)
}

func persistObject(src, dst *Snapshot, object *objects.Object) (objects.MAC, error) {
	hasher := dst.Repository().GetMACHasher()
	newObject := *object
	newObject.Chunks = make([]objects.Chunk, 0, len(object.Chunks))

	for _, chunkRef := range object.Chunks {
		chunk, err := src.GetBlob(resources.RT_CHUNK, chunkRef.ContentMAC)
		if err != nil {
			return objects.MAC{}, err
		}

		hasher.Write(chunk)

		chunkMAC := dst.Repository().ComputeMAC(chunk)
		if !dst.BlobExists(resources.RT_CHUNK, chunkMAC) {
			err = dst.PutBlob(resources.RT_CHUNK, chunkMAC, chunk)
			if err != nil {
				return objects.MAC{}, err
			}
		}

		newObject.Chunks = append(newObject.Chunks, objects.Chunk{
			Version:    chunkRef.Version,
			ContentMAC: chunkMAC,
			Length:     chunkRef.Length,
			Entropy:    chunkRef.Entropy,
			Flags:      chunkRef.Flags,
		})
	}

	newObject.ContentMAC = objects.MAC(hasher.Sum(nil))
	serializedObject, err := newObject.Serialize()
	if err != nil {
		return objects.MAC{}, err
	}

	mac := dst.Repository().ComputeMAC(serializedObject)
	if !dst.BlobExists(resources.RT_OBJECT, mac) {
		err = dst.PutBlob(resources.RT_OBJECT, mac, serializedObject)
		if err != nil {
			return objects.MAC{}, err
		}
	}

	return mac, nil
}

func persistVFS(src *Snapshot, dst *Snapshot, fs *vfs.Filesystem) func(objects.MAC) (objects.MAC, error) {
	return func(mac objects.MAC) (objects.MAC, error) {
		entry, err := fs.ResolveEntry(mac)
		if err != nil {
			return objects.MAC{}, err
		}

		if entry.HasObject() {
		}

		serializedEntry, err := entry.ToBytes()
		if err != nil {
			return objects.MAC{}, err
		}

		entryMAC := dst.Repository().ComputeMAC(serializedEntry)
		if !dst.BlobExists(resources.RT_VFS_ENTRY, entryMAC) {
			err = dst.PutBlob(resources.RT_VFS_ENTRY, entryMAC, serializedEntry)
			if err != nil {
				return objects.MAC{}, err
			}
		}
		return entryMAC, nil
	}
}

func persistErrors(src *Snapshot, dst *Snapshot) func(objects.MAC) (objects.MAC, error) {
	return func(mac objects.MAC) (objects.MAC, error) {
		data, err := src.GetBlob(resources.RT_ERROR_ENTRY, mac)
		if err != nil {
			return objects.MAC{}, err
		}

		newmac := dst.Repository().ComputeMAC(data)
		if !dst.BlobExists(resources.RT_ERROR_ENTRY, newmac) {
			err = dst.PutBlob(resources.RT_ERROR_ENTRY, newmac, data)
		}
		return newmac, err
	}
}

func persistXattrs(src *Snapshot, dst *Snapshot, fs *vfs.Filesystem) func(objects.MAC) (objects.MAC, error) {
	return func(mac objects.MAC) (objects.MAC, error) {
		xattr, err := fs.ResolveXattr(mac)
		if err != nil {
			return objects.MAC{}, err
		}

		xattr.Object, err = persistObject(src, dst, xattr.ResolvedObject)
		serialized, err := xattr.ToBytes()
		if err != nil {
			return objects.MAC{}, err
		}

		newmac := dst.Repository().ComputeMAC(serialized)
		if !dst.BlobExists(resources.RT_XATTR_ENTRY, newmac) {
			err = dst.PutBlob(resources.RT_XATTR_ENTRY, newmac, serialized)
			if err != nil {
				return objects.MAC{}, err
			}
		}
		return newmac, nil
	}
}

func syncIndex(src *Snapshot, dst *Snapshot, index *header.Index) error {
	switch index.Name {
	case "content-type":
		serialized, err := src.GetBlob(resources.RT_BTREE_ROOT, index.Value)
		if err != nil {
			return err
		}
		if err := dst.PutBlob(resources.RT_BTREE_ROOT, index.Value, serialized); err != nil {
			return err
		}

		store := repository.NewRepositoryStore[string, objects.MAC](src.Repository(), resources.RT_BTREE_NODE)
		tree, err := btree.Deserialize(bytes.NewReader(serialized), store, strings.Compare)
		if err != nil {
			return err
		}

		it := tree.IterDFS()
		for it.Next() {
			mac, node := it.Current()

			bytes, err := msgpack.Marshal(node)
			if err != nil {
				return err
			}

			err = push(src, dst, mac, resources.RT_BTREE_NODE, bytes)
			if err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("don't know how to sync the index %s of type %s",
			index.Name, index.Type)
	}

	return nil
}

func (src *Snapshot) Synchronize(dst *Snapshot) error {
	if src.Header.Identity.Identifier != uuid.Nil {
		err := push(src, dst, src.Header.Identifier,
			resources.RT_SIGNATURE, nil)
		if err != nil {
			return err
		}
	}

	source := src.Header.GetSource(0)
	fs, err := src.Filesystem()
	if err != nil {
		return err
	}

	vfs, errors, xattrs := fs.BTrees()

	dst.Header.GetSource(0).VFS.Root, err = persistIndex(dst, vfs, resources.RT_VFS_BTREE,
		resources.RT_VFS_NODE, persistVFS(src, dst, fs))
	if err != nil {
		return err
	}

	dst.Header.GetSource(0).VFS.Errors, err = persistIndex(dst, errors, resources.RT_ERROR_BTREE,
		resources.RT_ERROR_NODE, persistErrors(src, dst))
	if err != nil {
		return err
	}

	dst.Header.GetSource(0).VFS.Xattrs, err = persistIndex(dst, xattrs, resources.RT_XATTR_BTREE,
		resources.RT_XATTR_NODE, persistXattrs(src, dst, fs))
	if err != nil {
		return err
	}

	for i := range source.Indexes {
		if err := syncIndex(src, dst, &source.Indexes[i]); err != nil {
			return err
		}
	}

	return nil
}
