package caching

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"iter"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type MaintainanceCache struct {
	manager *Manager
	db      *leveldb.DB
}

func newMaintainanceCache(cacheManager *Manager, repositoryID uuid.UUID) (*MaintainanceCache, error) {
	cacheDir := filepath.Join(cacheManager.cacheDir, "maintainance", repositoryID.String())

	db, err := leveldb.OpenFile(cacheDir, nil)
	if err != nil {
		return nil, err
	}

	return &MaintainanceCache{
		manager: cacheManager,
		db:      db,
	}, nil
}

func (c *MaintainanceCache) Close() error {
	return c.db.Close()
}

func (c *MaintainanceCache) put(prefix string, pathname string, data []byte) error {
	return c.db.Put([]byte(fmt.Sprintf("%s:%s", prefix, pathname)), data, nil)
}

func (c *MaintainanceCache) has(prefix, key string) (bool, error) {
	return c.db.Has([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
}

func (c *MaintainanceCache) get(prefix, pathname string) ([]byte, error) {
	data, err := c.db.Get([]byte(fmt.Sprintf("%s:%s", prefix, pathname)), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

func (c *MaintainanceCache) delete(prefix, key string) error {
	return c.db.Delete([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
}

func (c *MaintainanceCache) PutSnapshot(snapshotID objects.MAC, data []byte) error {
	return c.put("__snapshot__", fmt.Sprintf("%x", snapshotID), data)
}

func (c *MaintainanceCache) HasSnapshot(snapshotID objects.MAC) (bool, error) {
	return c.has("__snapshot__", fmt.Sprintf("%x", snapshotID))
}

func (c *MaintainanceCache) DeleteSnapshot(snapshotID objects.MAC) error {
	return c.delete("__snapshot__", fmt.Sprintf("%x", snapshotID))
}

func (c *MaintainanceCache) PutPackfile(snapshotID, packfileMAC objects.MAC) error {
	return c.put("__packfile__", fmt.Sprintf("%x:%x", packfileMAC, snapshotID), packfileMAC[:])
}

func (c *MaintainanceCache) HasPackfile(packfileMAC objects.MAC) bool {
	keyPrefix := fmt.Sprintf("__packfile__:%x", packfileMAC)
	iter := c.db.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()

	for iter.Next() {
		return true
	}

	return false
}

func (c *MaintainanceCache) GetPackfiles(snapshotID objects.MAC) iter.Seq[objects.MAC] {
	return func(yield func(objects.MAC) bool) {
		iter := c.db.NewIterator(nil, nil)
		defer iter.Release()
		keyPrefix := "__packfile__:"

		for iter.Seek([]byte(keyPrefix)); iter.Valid(); iter.Next() {
			if !strings.HasPrefix(string(iter.Key()), keyPrefix) {
				break
			}

			if !yield(objects.MAC(iter.Value())) {
				return
			}
		}
	}
}

func (c *MaintainanceCache) DeleletePackfiles(snapshotID objects.MAC) error {
	iter := c.db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		hex_mac := string(key[bytes.LastIndexByte(key, byte(':'))+1:])
		mac, err := hex.DecodeString(hex_mac)
		if err != nil {
			return err
		}

		if objects.MAC(mac) == snapshotID {
			err := c.db.Delete(iter.Key(), nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
