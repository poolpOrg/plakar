package caching

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type ScanCache struct {
	snapshotID [32]byte
	manager    *Manager
	db         *leveldb.DB
}

func newScanCache(cacheManager *Manager, snapshotID [32]byte) (*ScanCache, error) {
	cacheDir := filepath.Join(cacheManager.cacheDir, "scan", fmt.Sprintf("%x", snapshotID))

	db, err := leveldb.OpenFile(cacheDir, nil)
	if err != nil {
		return nil, err
	}

	return &ScanCache{
		snapshotID: snapshotID,
		manager:    cacheManager,
		db:         db,
	}, nil
}

func (c *ScanCache) Close() error {
	c.db.Close()
	return os.RemoveAll(filepath.Join(c.manager.cacheDir, "scan", fmt.Sprintf("%x", c.snapshotID)))
}

func (c *ScanCache) put(prefix string, key string, data []byte) error {
	return c.db.Put([]byte(fmt.Sprintf("%s:%s", prefix, key)), data, nil)
}

func (c *ScanCache) get(prefix, key string) ([]byte, error) {
	data, err := c.db.Get([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

func (c *ScanCache) has(prefix, key string) (bool, error) {
	return c.db.Has([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
}

func (c *ScanCache) delete(prefix, key string) error {
	return c.db.Delete([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
}

func (c *ScanCache) getObjects(keyPrefix string) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		iter := c.db.NewIterator(nil, nil)
		defer iter.Release()

		for iter.Seek([]byte(keyPrefix)); iter.Valid(); iter.Next() {
			if !strings.HasPrefix(string(iter.Key()), keyPrefix) {
				break
			}

			/* Extract the csum part of the key, this avoids decoding the full
			 * entry later on if that's the only thing we need */
			key := iter.Key()
			hex_csum := string(key[bytes.LastIndexByte(key, byte(':'))+1:])
			csum, _ := hex.DecodeString(hex_csum)

			if !yield(objects.MAC(csum), iter.Value()) {
				return
			}
		}
	}
}

func (c *ScanCache) PutFile(file string, data []byte) error {
	return c.put("__file__", file, data)
}

func (c *ScanCache) GetFile(file string) ([]byte, error) {
	return c.get("__file__", file)
}

func (c *ScanCache) PutXattr(xattr string, data []byte) error {
	return c.put("__xattr__", xattr, data)
}

func (c *ScanCache) GetXattr(xattr string) ([]byte, error) {
	return c.get("__xattr__", xattr)
}

func (c *ScanCache) PutDirectory(directory string, data []byte) error {
	return c.put("__directory__", directory, data)
}

func (c *ScanCache) GetDirectory(directory string) ([]byte, error) {
	return c.get("__directory__", directory)
}

func (c *ScanCache) PutMAC(pathname string, mac objects.MAC) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}
	return c.put("__mac__", pathname, mac[:])
}

func (c *ScanCache) GetMAC(pathname string) (objects.MAC, error) {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}

	data, err := c.get("__mac__", pathname)
	if err != nil {
		return objects.MAC{}, err
	}

	if len(data) != 32 {
		return objects.MAC{}, fmt.Errorf("invalid MAC length: %d", len(data))
	}

	return objects.MAC(data), nil
}

func (c *ScanCache) PutSummary(pathname string, data []byte) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}

	return c.put("__summary__", pathname, data)
}

func (c *ScanCache) GetSummary(pathname string) ([]byte, error) {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}

	return c.get("__summary__", pathname)
}

func (c *ScanCache) PutState(stateID objects.MAC, data []byte) error {
	return c.put("__state__", fmt.Sprintf("%x", stateID), data)
}

func (c *ScanCache) HasState(stateID objects.MAC) (bool, error) {
	panic("HasState should never be used on the ScanCache backend")
}

func (c *ScanCache) GetState(stateID objects.MAC) ([]byte, error) {
	panic("GetState should never be used on the ScanCache backend")
}

func (c *ScanCache) GetStates() (map[objects.MAC][]byte, error) {
	panic("GetStates should never be used on the ScanCache backend")
}

func (c *ScanCache) DelState(stateID objects.MAC) error {
	panic("DelStates should never be used on the ScanCache backend")
}

func (c *ScanCache) GetDelta(blobType resources.Type, blobCsum objects.MAC) ([]byte, error) {
	return c.get("__delta__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *ScanCache) HasDelta(blobType resources.Type, blobCsum objects.MAC) (bool, error) {
	return c.has("__delta__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *ScanCache) GetDeltaByCsum(blobCsum objects.MAC) ([]byte, error) {
	for typ := resources.RT_SNAPSHOT; typ <= resources.RT_ERROR_ENTRY; typ++ {
		ret, err := c.GetDelta(typ, blobCsum)

		if err != nil {
			return nil, err
		}

		if ret != nil {
			return ret, nil
		}
	}

	return nil, nil
}

func (c *ScanCache) PutDelta(blobType resources.Type, blobCsum objects.MAC, data []byte) error {
	return c.put("__delta__", fmt.Sprintf("%d:%x", blobType, blobCsum), data)
}

func (c *ScanCache) GetDeltasByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		iter := c.db.NewIterator(nil, nil)
		defer iter.Release()

		keyPrefix := fmt.Sprintf("__delta__:%d", blobType)
		for iter.Seek([]byte(keyPrefix)); iter.Valid(); iter.Next() {
			if !strings.HasPrefix(string(iter.Key()), keyPrefix) {
				break
			}

			/* Extract the csum part of the key, this avoids decoding the full
			 * entry later on if that's the only thing we need */
			key := iter.Key()
			hex_csum := string(key[bytes.LastIndexByte(key, byte(':'))+1:])
			csum, _ := hex.DecodeString(hex_csum)

			if !yield(objects.MAC(csum), iter.Value()) {
				return
			}
		}
	}
}

func (c *ScanCache) GetDeltas() iter.Seq2[objects.MAC, []byte] {
	return c.getObjects("__delta__:")
}

func (c *ScanCache) DelDelta(blobType resources.Type, blobCsum objects.MAC) error {
	return c.delete("__delta__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *ScanCache) PutDeleted(blobType resources.Type, blobCsum objects.MAC, data []byte) error {
	return c.put("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum), data)
}

func (c *ScanCache) HasDeleted(blobType resources.Type, blobCsum objects.MAC) (bool, error) {
	return c.has("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *ScanCache) GetDeleteds() iter.Seq2[objects.MAC, []byte] {
	return c.getObjects(fmt.Sprintf("__deleted__:"))
}
func (c *ScanCache) GetDeletedsByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return c.getObjects(fmt.Sprintf("__deleted__:%d", blobType))
}

func (c *ScanCache) PutPackfile(stateID, packfile objects.MAC, data []byte) error {
	return c.put("__packfile__", fmt.Sprintf("%x:%x", stateID, packfile), data)
}

func (c *ScanCache) GetPackfiles() iter.Seq2[objects.MAC, []byte] {
	return c.getObjects("__packfile__:")
}

func (c *ScanCache) GetPackfilesForState(stateID objects.MAC) iter.Seq2[objects.MAC, []byte] {
	return c.getObjects(fmt.Sprintf("__packfile__:%x", stateID))
}

func (c *ScanCache) EnumerateKeysWithPrefix(prefix string, reverse bool) iter.Seq2[string, []byte] {
	l := len(prefix)

	return func(yield func(string, []byte) bool) {
		// Use LevelDB's iterator
		iter := c.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
		defer iter.Release()

		if reverse {
			iter.Last()
		} else {
			iter.First()
		}

		for iter.Valid() {
			key := iter.Key()

			// Check if the key starts with the given prefix
			if !strings.HasPrefix(string(key), prefix) {
				if reverse {
					iter.Prev()
				} else {
					iter.Next()
				}
				continue
			}

			if !yield(string(key)[l:], iter.Value()) {
				return
			}

			if reverse {
				iter.Prev()
			} else {
				iter.Next()
			}
		}
	}
}
