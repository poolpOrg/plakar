package caching

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"iter"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
)

type _RepositoryCache struct {
	manager *Manager
	db      *leveldb.DB
}

func newRepositoryCache(cacheManager *Manager, repositoryID uuid.UUID) (*_RepositoryCache, error) {
	cacheDir := filepath.Join(cacheManager.cacheDir, "repository", repositoryID.String())

	db, err := leveldb.OpenFile(cacheDir, nil)
	if err != nil {
		return nil, err
	}

	return &_RepositoryCache{
		manager: cacheManager,
		db:      db,
	}, nil
}

func (c *_RepositoryCache) Close() error {
	return c.db.Close()
}

func (c *_RepositoryCache) put(prefix string, key string, data []byte) error {
	return c.db.Put([]byte(fmt.Sprintf("%s:%s", prefix, key)), data, nil)
}

func (c *_RepositoryCache) has(prefix, key string) (bool, error) {
	return c.db.Has([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
}

func (c *_RepositoryCache) get(prefix, key string) ([]byte, error) {
	data, err := c.db.Get([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

func (c *_RepositoryCache) getObjects(keyPrefix string) iter.Seq2[objects.Checksum, []byte] {
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

func (c *_RepositoryCache) delete(prefix, key string) error {
	return c.db.Delete([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
}

func (c *_RepositoryCache) PutState(stateID objects.Checksum, data []byte) error {
	return c.put("__state__", fmt.Sprintf("%x", stateID), data)
}

func (c *_RepositoryCache) HasState(stateID objects.Checksum) (bool, error) {
	return c.has("__state__", fmt.Sprintf("%x", stateID))
}

func (c *_RepositoryCache) GetState(stateID objects.Checksum) ([]byte, error) {
	return c.get("__state__", fmt.Sprintf("%x", stateID))
}

func (c *_RepositoryCache) DelState(stateID objects.Checksum) error {
	return c.delete("__state__", fmt.Sprintf("%x", stateID))
}

func (c *_RepositoryCache) GetStates() (map[objects.Checksum][]byte, error) {
	ret := make(map[objects.Checksum][]byte, 0)
	iter := c.db.NewIterator(nil, nil)
	defer iter.Release()

	keyPrefix := "__state__:"
	for iter.Seek([]byte(keyPrefix)); iter.Valid(); iter.Next() {
		if !strings.HasPrefix(string(iter.Key()), keyPrefix) {
			break
		}

		var stateID objects.Checksum
		_, err := hex.Decode(stateID[:], iter.Key()[len(keyPrefix):])
		if err != nil {
			fmt.Printf("Error decoding state ID: %v\n", err)
			return nil, err
		}
		ret[stateID] = iter.Value()
	}

	return ret, nil
}

func (c *_RepositoryCache) GetDelta(blobType resources.Type, blobCsum objects.Checksum) ([]byte, error) {
	return c.get("__delta__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *_RepositoryCache) HasDelta(blobType resources.Type, blobCsum objects.Checksum) (bool, error) {
	return c.has("__delta__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *_RepositoryCache) GetDeltaByCsum(blobCsum objects.Checksum) ([]byte, error) {
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

func (c *_RepositoryCache) PutDelta(blobType resources.Type, blobCsum objects.Checksum, data []byte) error {
	return c.put("__delta__", fmt.Sprintf("%d:%x", blobType, blobCsum), data)
}

func (c *_RepositoryCache) GetDeltasByType(blobType resources.Type) iter.Seq2[objects.Checksum, []byte] {
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

func (c *_RepositoryCache) GetDeltas() iter.Seq2[objects.Checksum, []byte] {
	return c.getObjects("__delta__:")
}

func (c *_RepositoryCache) PutDeleted(blobType resources.Type, blobCsum objects.Checksum, data []byte) error {
	return c.put("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum), data)
}

func (c *_RepositoryCache) HasDeleted(blobType resources.Type, blobCsum objects.Checksum) (bool, error) {
	return c.has("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *_RepositoryCache) GetDeleteds() iter.Seq2[objects.Checksum, []byte] {
	return c.getObjects("__deleted__:")
}

func (c *_RepositoryCache) PutPackfile(stateID, packfile objects.Checksum, data []byte) error {
	return c.put("__packfile__", fmt.Sprintf("%x:%x", stateID, packfile), data)
}

func (c *_RepositoryCache) GetPackfiles() iter.Seq2[objects.Checksum, []byte] {
	return c.getObjects("__packfile__:")
}

func (c *_RepositoryCache) GetPackfilesForState(stateID objects.Checksum) iter.Seq2[objects.Checksum, []byte] {
	return c.getObjects(fmt.Sprintf("__packfile__:%x", stateID))
}
