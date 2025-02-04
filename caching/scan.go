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

func (c *ScanCache) getObjects(keyPrefix string) iter.Seq2[objects.Checksum, []byte] {
	return func(yield func(objects.Checksum, []byte) bool) {
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

			if !yield(objects.Checksum(csum), iter.Value()) {
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

func (c *ScanCache) PutDirectory(directory string, data []byte) error {
	return c.put("__directory__", directory, data)
}

func (c *ScanCache) GetDirectory(directory string) ([]byte, error) {
	return c.get("__directory__", directory)
}

func (c *ScanCache) PutChecksum(pathname string, checksum objects.Checksum) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}
	return c.put("__checksum__", pathname, checksum[:])
}

func (c *ScanCache) GetChecksum(pathname string) (objects.Checksum, error) {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}

	data, err := c.get("__checksum__", pathname)
	if err != nil {
		return objects.Checksum{}, err
	}

	if len(data) != 32 {
		return objects.Checksum{}, fmt.Errorf("invalid checksum length: %d", len(data))
	}

	return objects.Checksum(data), nil
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

func (c *ScanCache) PutState(stateID objects.Checksum, data []byte) error {
	return c.put("__state__", fmt.Sprintf("%x", stateID), data)
}

func (c *ScanCache) HasState(stateID objects.Checksum) (bool, error) {
	panic("HasState should never be used on the ScanCache backend")
}

func (c *ScanCache) GetState(stateID objects.Checksum) ([]byte, error) {
	panic("GetState should never be used on the ScanCache backend")
}

func (c *ScanCache) GetStates() (map[objects.Checksum][]byte, error) {
	panic("GetStates should never be used on the ScanCache backend")
}

func (c *ScanCache) DelState(stateID objects.Checksum) error {
	panic("DelStates should never be used on the ScanCache backend")
}

func (c *ScanCache) GetDelta(blobType resources.Type, blobCsum objects.Checksum) ([]byte, error) {
	return c.get("__delta__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *ScanCache) HasDelta(blobType resources.Type, blobCsum objects.Checksum) (bool, error) {
	return c.has("__delta__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *ScanCache) GetDeltaByCsum(blobCsum objects.Checksum) ([]byte, error) {
	for typ := resources.RT_SNAPSHOT; typ <= resources.RT_ERROR; typ++ {
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

func (c *ScanCache) PutDelta(blobType resources.Type, blobCsum objects.Checksum, data []byte) error {
	return c.put("__delta__", fmt.Sprintf("%d:%x", blobType, blobCsum), data)
}

func (c *ScanCache) GetDeltasByType(blobType resources.Type) iter.Seq2[objects.Checksum, []byte] {
	return func(yield func(objects.Checksum, []byte) bool) {
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

			if !yield(objects.Checksum(csum), iter.Value()) {
				return
			}
		}
	}
}

func (c *ScanCache) GetDeltas() iter.Seq2[objects.Checksum, []byte] {
	return c.getObjects("__delta__:")
}

func (c *ScanCache) PutDeleted(blobType resources.Type, blobCsum objects.Checksum, data []byte) error {
	return c.put("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum), data)
}

func (c *ScanCache) HasDeleted(blobType resources.Type, blobCsum objects.Checksum) (bool, error) {
	return c.has("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *ScanCache) GetDeleteds() iter.Seq2[objects.Checksum, []byte] {
	return c.getObjects("__deleted__:")
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
