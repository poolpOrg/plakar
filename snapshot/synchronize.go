package snapshot

import (
	"fmt"
	"strings"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/google/uuid"
)

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

func persistVFS(src *Snapshot, dst *Snapshot, fs *vfs.Filesystem, ctidx *btree.BTree[string, int, objects.MAC]) func(objects.MAC) (objects.MAC, error) {
	return func(mac objects.MAC) (objects.MAC, error) {
		entry, err := fs.ResolveEntry(mac)
		if err != nil {
			return objects.MAC{}, err
		}

		if entry.HasObject() {
			entry.Object, err = persistObject(src, dst, entry.ResolvedObject)
			if err != nil {
				return objects.MAC{}, nil
			}
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

		if entry.HasObject() {
			parts := strings.SplitN(entry.ResolvedObject.ContentType, ";", 2)
			mime := parts[0]
			k := fmt.Sprintf("/%s%s", mime, entry.Path())
			if err := ctidx.Insert(k, entryMAC); err != nil {
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

func (src *Snapshot) Synchronize(dst *Snapshot) error {
	if src.Header.Identity.Identifier != uuid.Nil {
		data, err := src.GetBlob(resources.RT_SIGNATURE, src.Header.Identifier)
		if err != nil {
			return err
		}

		newmac := dst.Repository().ComputeMAC(data)
		dst.Header.Identifier = newmac
		if dst.BlobExists(resources.RT_SIGNATURE, newmac) {
			err = dst.PutBlob(resources.RT_SIGNATURE, newmac, data)
			if err != nil {
				return err
			}
		}
	}

	fs, err := src.Filesystem()
	if err != nil {
		return err
	}

	vfs, errors, xattrs := fs.BTrees()

	ctidx, err := btree.New(&btree.InMemoryStore[string, objects.MAC]{}, strings.Compare, 50)

	dst.Header.GetSource(0).VFS.Root, err = persistIndex(dst, vfs, resources.RT_VFS_BTREE,
		resources.RT_VFS_NODE, persistVFS(src, dst, fs, ctidx))
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

	ctsum, err := persistIndex(dst, ctidx, resources.RT_BTREE_ROOT, resources.RT_BTREE_NODE, func(mac objects.MAC) (objects.MAC, error) {
		return mac, nil
	})
	dst.Header.GetSource(0).Indexes = []header.Index{
		{
			Name: "content-type",
			Type: "btree",
			Value: ctsum,
		},
	}

	return nil
}
