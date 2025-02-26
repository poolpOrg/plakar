/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/vmihailenco/msgpack/v5"
)

const VERSION = "1.0.0"

func init() {
	versioning.Register(resources.RT_STATE, versioning.FromString(VERSION))
}

type EntryType uint8

const (
	ET_METADATA      EntryType = 1
	ET_LOCATIONS               = 2
	ET_DELETED                 = 3
	ET_PACKFILE                = 4
	ET_CONFIGURATION           = 5
)

type Metadata struct {
	Version   versioning.Version `msgpack:"version"`
	Timestamp time.Time          `msgpack:"timestamp"`
	Serial    uuid.UUID          `msgpack:"serial"`
}

type Location struct {
	Packfile objects.MAC
	Offset   uint64
	Length   uint32
}

const LocationSerializedSize = 32 + 8 + 4

type DeltaEntry struct {
	Type     resources.Type
	Version  versioning.Version
	Blob     objects.MAC
	Location Location
	Flags    uint32
}

const DeltaEntrySerializedSize = 1 + 4 + 32 + LocationSerializedSize + 4

type DeletedEntry struct {
	Type resources.Type
	Blob objects.MAC
	When time.Time
}

const DeletedEntrySerializedSize = 1 + 32 + 8

type PackfileEntry struct {
	Packfile  objects.MAC
	StateID   objects.MAC
	Timestamp time.Time
}

const PackfileEntrySerializedSize = 32 + 32 + 8

type ConfigurationEntry struct {
	Key       string
	Value     []byte
	CreatedAt time.Time
}

// A local version of the state, possibly aggregated, that uses on-disk storage.
//   - States are stored under a dedicated prefix key, with their data being the
//     state's metadata.
//   - Delta entries are stored under another dedicated prefix and are keyed by
//     their issuing state.
type LocalState struct {
	Metadata Metadata

	// Contains live configuration values (most up to date loaded from
	// repository state), or when in a derived State contains configurations
	// about to be pushed to the repository.
	configuration map[string]ConfigurationEntry

	// DeltaEntries are keyed by <EntryType>:<EntryCsum>:<StateID> in the cache.
	// This allows:
	//  - Grouping and iterating on them by Type.
	//  - Finding a particular Csum efficiently if you know the type.
	//  - Somewhat fast key retrieval if you only know the Csum (something we
	//    don't need right now).
	//  - StateID is there at the end because we don't need to query by it but
	//    we need it to avoid concurrent insert of the same entry by two
	//    different backup processes.
	cache caching.StateCache
}

func NewLocalState(cache caching.StateCache) *LocalState {
	return &LocalState{
		Metadata: Metadata{
			Version:   versioning.FromString(VERSION),
			Timestamp: time.Now(),
		},
		configuration: make(map[string]ConfigurationEntry),
		cache:         cache,
	}
}

func FromStream(version versioning.Version, rd io.Reader, cache caching.StateCache) (*LocalState, error) {
	st := &LocalState{cache: cache}
	if err := st.deserializeFromStream(rd); err != nil {
		return nil, err
	} else {
		return st, nil
	}
}

// Derive constructs a new state backed by *cache*, keeping the same serial as previous one.
// Mainly used to construct Delta states when backing up.
func (ls *LocalState) Derive(cache caching.StateCache) *LocalState {
	st := NewLocalState(cache)
	st.Metadata.Serial = ls.Metadata.Serial

	return st
}

// Finds the latest (current) serial in the aggregate state, and if none sets
// it to the provided one.
func (ls *LocalState) UpdateSerialOr(serial uuid.UUID) error {
	var latestID *objects.MAC = nil
	var latestMT *Metadata = nil

	states, err := ls.cache.GetStates()
	if err != nil {
		return err
	}

	for stateID, buf := range states {
		mt, err := MetadataFromBytes(buf)

		if err != nil {
			return err
		}

		if latestID == nil || latestMT.Timestamp.Before(mt.Timestamp) {
			latestID = &stateID
			latestMT = mt
		}
	}

	if latestMT != nil {
		ls.Metadata.Serial = latestMT.Serial
	} else {
		ls.Metadata.Serial = serial
	}

	return nil
}

/* Insert the state denotated by stateID and its associated delta entries read from rd */
func (ls *LocalState) InsertState(version versioning.Version, stateID objects.MAC, rd io.Reader) error {
	has, err := ls.HasState(stateID)
	if err != nil {
		return err
	}

	if has {
		return nil
	}

	err = ls.deserializeFromStream(rd)
	if err != nil {
		return err
	}

	/* We merged the state deltas, we can now publish it */
	mt, err := ls.Metadata.ToBytes()
	if err != nil {
		return err
	}

	err = ls.cache.PutState(stateID, mt)
	if err != nil {
		return err
	}

	return nil
}

/* On disk format is <EntryType><EntryLength><Entry>...N<header>
 * Counting keys would mean iterating twice so we reverse the format and add a
 * type.
 */
func (ls *LocalState) SerializeToStream(w io.Writer) error {
	writeUint64 := func(value uint64) error {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, value)
		_, err := w.Write(buf)
		return err
	}

	writeUint32 := func(value uint32) error {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, value)
		_, err := w.Write(buf)
		return err
	}

	for _, entry := range ls.cache.GetDeltas() {
		if _, err := w.Write([]byte{byte(ET_LOCATIONS)}); err != nil {
			return fmt.Errorf("failed to write delta entry type: %w", err)
		}

		if err := writeUint32(DeltaEntrySerializedSize); err != nil {
			return fmt.Errorf("failed to write delta entry length: %w", err)
		}

		if _, err := w.Write(entry); err != nil {
			return fmt.Errorf("failed to write delta entry: %w", err)
		}
	}

	for _, entry := range ls.cache.GetDeleteds() {
		if _, err := w.Write([]byte{byte(ET_DELETED)}); err != nil {
			return fmt.Errorf("failed to write deleted entry type: %w", err)
		}

		if err := writeUint32(DeletedEntrySerializedSize); err != nil {
			return fmt.Errorf("failed to write deleted entry length: %w", err)
		}

		if _, err := w.Write(entry); err != nil {
			return fmt.Errorf("failed to write deleted entry: %w", err)
		}
	}

	for _, entry := range ls.cache.GetPackfiles() {
		if _, err := w.Write([]byte{byte(ET_PACKFILE)}); err != nil {
			return fmt.Errorf("failed to write packfile entry type: %w", err)
		}

		if err := writeUint32(PackfileEntrySerializedSize); err != nil {
			return fmt.Errorf("failed to write packfile entry length: %w", err)
		}

		if _, err := w.Write(entry); err != nil {
			return fmt.Errorf("failed to write packfile entry: %w", err)
		}
	}

	for entry := range ls.cache.GetConfigurations() {
		if _, err := w.Write([]byte{byte(ET_CONFIGURATION)}); err != nil {
			return fmt.Errorf("failed to write configuration entry type: %w", err)
		}

		if err := writeUint32(uint32(len(entry))); err != nil {
			return fmt.Errorf("failed to write configuration entry length: %w", err)
		}

		if _, err := w.Write(entry); err != nil {
			return fmt.Errorf("failed to write configuration entry: %w", err)
		}
	}

	/* Finally we serialize the Metadata */
	if _, err := w.Write([]byte{byte(ET_METADATA)}); err != nil {
		return fmt.Errorf("failed to write metadata type %w", err)
	}
	if err := writeUint32(uint32(ls.Metadata.Version)); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}
	timestamp := ls.Metadata.Timestamp.UnixNano()
	if err := writeUint64(uint64(timestamp)); err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}
	if _, err := w.Write(ls.Metadata.Serial[:]); err != nil {
		return fmt.Errorf("failed to write serial flag: %w", err)
	}

	return nil

}

func DeltaEntryFromBytes(buf []byte) (de DeltaEntry, err error) {
	bbuf := bytes.NewBuffer(buf)

	typ, err := bbuf.ReadByte()
	if err != nil {
		return
	}

	de.Type = resources.Type(typ)
	de.Version = versioning.Version(binary.LittleEndian.Uint32(bbuf.Next(4)))

	n, err := bbuf.Read(de.Blob[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return de, fmt.Errorf("Short read while deserializing delta entry")
	}

	n, err = bbuf.Read(de.Location.Packfile[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return de, fmt.Errorf("Short read while deserializing delta entry")
	}

	de.Location.Offset = binary.LittleEndian.Uint64(bbuf.Next(8))
	de.Location.Length = binary.LittleEndian.Uint32(bbuf.Next(4))
	de.Flags = binary.LittleEndian.Uint32(bbuf.Next(4))

	return
}

func (de *DeltaEntry) _toBytes(buf []byte) {
	pos := 0
	buf[pos] = byte(de.Type)
	pos++
	binary.LittleEndian.PutUint32(buf[pos:], uint32(de.Version))
	pos += 4

	pos += copy(buf[pos:], de.Blob[:])
	pos += copy(buf[pos:], de.Location.Packfile[:])
	binary.LittleEndian.PutUint64(buf[pos:], de.Location.Offset)
	pos += 8
	binary.LittleEndian.PutUint32(buf[pos:], de.Location.Length)
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:], de.Flags)
}

func (de *DeltaEntry) ToBytes() (ret []byte) {
	ret = make([]byte, DeltaEntrySerializedSize)
	de._toBytes(ret)
	return
}

func PackfileEntryFromBytes(buf []byte) (pe PackfileEntry, err error) {
	bbuf := bytes.NewBuffer(buf)

	n, err := bbuf.Read(pe.Packfile[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return pe, fmt.Errorf("Short read while deserializing packfile entry")
	}

	n, err = bbuf.Read(pe.StateID[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return pe, fmt.Errorf("Short read while deserializing packfile entry")
	}

	timestamp := binary.LittleEndian.Uint64(bbuf.Next(8))
	pe.Timestamp = time.Unix(0, int64(timestamp))

	return
}

func (pe *PackfileEntry) _toBytes(buf []byte) {
	pos := 0
	pos += copy(buf[pos:], pe.Packfile[:])
	pos += copy(buf[pos:], pe.StateID[:])
	binary.LittleEndian.PutUint64(buf[pos:], uint64(pe.Timestamp.UnixNano()))
}

func (pe *PackfileEntry) ToBytes() (ret []byte) {
	ret = make([]byte, PackfileEntrySerializedSize)
	pe._toBytes(ret)
	return
}

func DeletedEntryFromBytes(buf []byte) (de DeletedEntry, err error) {
	bbuf := bytes.NewBuffer(buf)

	typ, err := bbuf.ReadByte()
	if err != nil {
		return
	}

	de.Type = resources.Type(typ)

	n, err := bbuf.Read(de.Blob[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return de, fmt.Errorf("Short read while deserializing deleted entry")
	}

	timestamp := binary.LittleEndian.Uint64(bbuf.Next(8))
	de.When = time.Unix(0, int64(timestamp))

	return
}

func (de *DeletedEntry) _toBytes(buf []byte) {
	pos := 0
	buf[pos] = byte(de.Type)
	pos++

	pos += copy(buf[pos:], de.Blob[:])
	binary.LittleEndian.PutUint64(buf[pos:], uint64(de.When.UnixNano()))
}

func (de *DeletedEntry) ToBytes() (ret []byte) {
	ret = make([]byte, DeletedEntrySerializedSize)
	de._toBytes(ret)
	return
}

// Because it's a variable sized struct we encode it this way:
// - keyLen uint8
// - key [keylen]byte
// - valueLen uint16
// - value [valueLen]byte
// - createdAt uint64
func ConfigurationEntryFromBytes(buf []byte) (ce ConfigurationEntry, err error) {
	bbuf := bytes.NewBuffer(buf)

	keyLen, err := bbuf.ReadByte()
	if err != nil {
		return ce, fmt.Errorf("Short read while deserializing keyLen ConfigurationEntry")
	}
	ce.Key = string(bbuf.Next(int(keyLen)))

	valueLen := binary.LittleEndian.Uint16(bbuf.Next(2))
	ce.Value = bbuf.Next(int(valueLen))

	timestamp := binary.LittleEndian.Uint64(bbuf.Next(8))
	ce.CreatedAt = time.Unix(0, int64(timestamp))

	return
}

func (ce *ConfigurationEntry) ToBytes() []byte {
	buf := make([]byte, 1+len(ce.Key)+2+len(ce.Value)+8)
	pos := 0

	buf[pos] = byte(len(ce.Key))
	pos += 1
	pos += copy(buf[pos:], ce.Key)

	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(ce.Value)))
	pos += 2
	pos += copy(buf[pos:], ce.Value)

	binary.LittleEndian.PutUint64(buf[pos:], uint64(ce.CreatedAt.UnixNano()))

	return buf
}

func (ls *LocalState) deserializeFromStream(r io.Reader) error {
	readUint64 := func() (uint64, error) {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(buf), nil
	}

	readUint32 := func() (uint32, error) {
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(buf), nil
	}

	/* Deserialize LOCATIONS */
	et_buf := make([]byte, 1)
	de_buf := make([]byte, DeltaEntrySerializedSize)
	deleted_buf := make([]byte, DeletedEntrySerializedSize)
	pe_buf := make([]byte, PackfileEntrySerializedSize)
	for {
		n, err := r.Read(et_buf)
		if err != nil || n != len(et_buf) {
			return fmt.Errorf("failed to read entry type %w", err)
		}

		entryType := EntryType(et_buf[0])
		if entryType == ET_METADATA {
			break
		}

		length, err := readUint32()
		if err != nil {
			return fmt.Errorf("failed to read entry length %w", err)
		}

		//XXX: This is screaming refactorization, but is a bit subtil.
		switch entryType {
		case ET_LOCATIONS:
			if length != DeltaEntrySerializedSize {
				return fmt.Errorf("failed to read delta entry wrong length got(%d)/expected(%d)", length, DeltaEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, de_buf); err != nil {
				return fmt.Errorf("failed to read delta entry %w, read(%d)/expected(%d)", err, n, length)
			}

			// We need to decode just to make the key, but we can reuse the buffer
			// to put inside the data part of the cache.
			delta, err := DeltaEntryFromBytes(de_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize delta entry %w", err)
			}

			ls.cache.PutDelta(delta.Type, delta.Blob, delta.Location.Packfile, de_buf)
		case ET_DELETED:
			if length != DeletedEntrySerializedSize {
				return fmt.Errorf("failed to read deleted entry wrong length got(%d)/expected(%d)", length, DeletedEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, deleted_buf); err != nil {
				return fmt.Errorf("failed to read deleted entry %w, read(%d)/expected(%d)", err, n, length)
			}

			deleted, err := DeletedEntryFromBytes(deleted_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize deleted entry %w", err)
			}

			ls.cache.PutDeleted(deleted.Type, deleted.Blob, deleted_buf)
		case ET_PACKFILE:
			if length != PackfileEntrySerializedSize {
				return fmt.Errorf("failed to read packfile entry wrong length got(%d)/expected(%d)", length, PackfileEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, pe_buf); err != nil {
				return fmt.Errorf("failed to read packfile entry %w, read(%d)/expected(%d)", err, n, length)
			}

			pe, err := PackfileEntryFromBytes(pe_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize packfile entry %w", err)
			}

			ls.cache.PutPackfile(pe.Packfile, pe_buf)

		case ET_CONFIGURATION:
			ce_buf := make([]byte, length)

			if n, err := io.ReadFull(r, ce_buf); err != nil {
				return fmt.Errorf("failed to read configuration entry %w, read(%d)/expected(%d)", err, n, length)
			}

			ce, err := ConfigurationEntryFromBytes(ce_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize configuration entry %w", err)
			}

			err = ls.insertOrUpdateConfiguration(ce)
			if err != nil {
				return fmt.Errorf("failed to insert/update configuration entry %w", err)
			}
		default:
			// Our version doesn't know this entry type, just skip it.
			io.CopyN(io.Discard, r, int64(length))
		}

	}

	/* Deserialize Metadata */
	version, err := readUint32()
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	ls.Metadata.Version = versioning.Version(version)

	timestamp, err := readUint64()
	if err != nil {
		return fmt.Errorf("failed to read timestamp: %w", err)
	}
	ls.Metadata.Timestamp = time.Unix(0, int64(timestamp))

	serial := make([]byte, len(uuid.UUID{}))
	if _, err := io.ReadFull(r, serial); err != nil {
		return fmt.Errorf("failed to read serial: %w", err)
	}
	ls.Metadata.Serial = uuid.UUID(serial)

	return nil
}

func (ls *LocalState) HasState(stateID objects.MAC) (bool, error) {
	return ls.cache.HasState(stateID)
}

func (ls *LocalState) DelState(stateID objects.MAC) error {
	return ls.cache.DelState(stateID)
}

func (ls *LocalState) PutDelta(de DeltaEntry) error {
	return ls.cache.PutDelta(de.Type, de.Blob, de.Location.Packfile, de.ToBytes())
}

func (ls *LocalState) DelDelta(Type resources.Type, blobMAC, packfileMAC objects.MAC) error {
	return ls.cache.DelDelta(Type, blobMAC, packfileMAC)
}

func (ls *LocalState) BlobExists(Type resources.Type, blobMAC objects.MAC) bool {
	for _, buf := range ls.cache.GetDelta(Type, blobMAC) {
		de, err := DeltaEntryFromBytes(buf)

		if err != nil {
			continue
		}

		ok, err := ls.cache.HasPackfile(de.Location.Packfile)
		if err != nil {
			continue
		}

		deleted, _ := ls.HasDeletedResource(resources.RT_PACKFILE, de.Location.Packfile)
		if ok && !deleted {
			return true
		}
	}

	return false
}

func (ls *LocalState) GetSubpartForBlob(Type resources.Type, blobMAC objects.MAC) (Location, bool, error) {
	var delta *DeltaEntry
	for _, buf := range ls.cache.GetDelta(Type, blobMAC) {
		de, err := DeltaEntryFromBytes(buf)

		if err != nil {
			return Location{}, false, err
		}

		ok, err := ls.cache.HasPackfile(de.Location.Packfile)
		if err != nil {
			return Location{}, false, err
		}

		if ok {
			delta = &de
			break
		}
	}

	if delta == nil {
		return Location{}, false, nil
	} else {
		return delta.Location, true, nil
	}
}

func (ls *LocalState) PutPackfile(stateId, packfile objects.MAC) error {
	pe := PackfileEntry{
		StateID:   stateId,
		Packfile:  packfile,
		Timestamp: time.Now(),
	}

	return ls.cache.PutPackfile(pe.Packfile, pe.ToBytes())
}

func (ls *LocalState) DelPackfile(packfile objects.MAC) error {
	return ls.cache.DelPackfile(packfile)
}

func (ls *LocalState) ListPackfiles() iter.Seq[objects.MAC] {
	return func(yield func(objects.MAC) bool) {
		for st, _ := range ls.cache.GetPackfiles() {
			if !yield(st) {
				return
			}
		}
	}
}

func (ls *LocalState) ListSnapshots() iter.Seq[objects.MAC] {
	return func(yield func(objects.MAC) bool) {
		for _, buf := range ls.cache.GetDeltasByType(resources.RT_SNAPSHOT) {
			de, _ := DeltaEntryFromBytes(buf)

			ok, err := ls.cache.HasPackfile(de.Location.Packfile)
			if err != nil || !ok {
				continue
			}

			if has, _ := ls.cache.HasDeleted(resources.RT_SNAPSHOT, de.Blob); has {
				continue
			}

			if !yield(de.Blob) {
				return
			}
		}
	}
}

func (ls *LocalState) ListObjectsOfType(Type resources.Type) iter.Seq2[DeltaEntry, error] {
	return func(yield func(DeltaEntry, error) bool) {
		for _, buf := range ls.cache.GetDeltasByType(Type) {
			de, err := DeltaEntryFromBytes(buf)
			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}
			}

			ok, err := ls.cache.HasPackfile(de.Location.Packfile)
			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}
			}

			if !ok {
				continue
			}

			if !yield(de, err) {
				return
			}
		}
	}
}

func (ls *LocalState) ListOrphanDeltas() iter.Seq2[DeltaEntry, error] {
	return func(yield func(DeltaEntry, error) bool) {
		for _, buf := range ls.cache.GetDeltas() {
			de, err := DeltaEntryFromBytes(buf)

			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}
			}

			ok, err := ls.cache.HasPackfile(de.Location.Packfile)
			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}
			}

			if !ok {
				if !yield(de, nil) {
					return
				}
			}
		}
	}
}

func (ls *LocalState) DeleteResource(rtype resources.Type, resource objects.MAC) error {
	de := DeletedEntry{
		Type: rtype,
		Blob: resource,
		When: time.Now(),
	}
	return ls.cache.PutDeleted(de.Type, de.Blob, de.ToBytes())
}

func (ls *LocalState) HasDeletedResource(rtype resources.Type, resource objects.MAC) (bool, error) {
	return ls.cache.HasDeleted(rtype, resource)
}

// Public function to insert a new configuration, beware this is to be
// serialized and pushed to repository, in order to do so most of the time you
// want to do it on a Derive'd State (in order to not repush existing
// configuration entries)
func (ls *LocalState) SetConfiguration(key string, value []byte) error {
	ce := ConfigurationEntry{
		Key:       key,
		Value:     value,
		CreatedAt: time.Now(),
	}

	return ls.insertOrUpdateConfiguration(ce)
}

// Internal function used by deserialization that only updates our local on
// disk state if the provided configuration is more recent than the stored one
func (ls *LocalState) insertOrUpdateConfiguration(ce ConfigurationEntry) error {
	value, err := ls.cache.GetConfiguration(ce.Key)

	if err != nil && err != leveldb.ErrNotFound {
		return err
	}

	if err != nil && err == leveldb.ErrNotFound {
		oldCe, err := ConfigurationEntryFromBytes(value)
		if err != nil {
			return err
		}

		if oldCe.CreatedAt.Before(ce.CreatedAt) {
			if err := ls.cache.PutConfiguration(ce.Key, ce.ToBytes()); err != nil {
				return err
			}
		}
	} else {
		if err := ls.cache.PutConfiguration(ce.Key, ce.ToBytes()); err != nil {
			return err
		}
	}

	return nil
}

func (ls *LocalState) ListDeletedResources(rtype resources.Type) iter.Seq2[DeletedEntry, error] {
	return func(yield func(DeletedEntry, error) bool) {
		for _, buf := range ls.cache.GetDeletedsByType(rtype) {
			de, err := DeletedEntryFromBytes(buf)

			if !yield(de, err) {
				return
			}
		}
	}
}

func (ls *LocalState) DelDeletedResource(rtype resources.Type, resourceMAC objects.MAC) error {
	return ls.cache.DelDeleted(rtype, resourceMAC)
}

func (mt *Metadata) ToBytes() ([]byte, error) {
	return msgpack.Marshal(mt)
}

func MetadataFromBytes(data []byte) (*Metadata, error) {
	var mt Metadata
	if err := msgpack.Unmarshal(data, &mt); err != nil {
		return nil, err
	}
	return &mt, nil
}
