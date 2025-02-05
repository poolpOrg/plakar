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
	"github.com/vmihailenco/msgpack/v5"
)

const VERSION = "1.0.0"

func init() {
	versioning.Register(resources.RT_STATE, versioning.FromString(VERSION))
}

type EntryType uint8

const (
	ET_METADATA  EntryType = 1
	ET_LOCATIONS           = 2
	ET_TIMESTAMP           = 3
)

type Metadata struct {
	Version   versioning.Version `msgpack:"version"`
	Timestamp time.Time          `msgpack:"timestamp"`
	Serial    uuid.UUID          `msgpack:"serial"`
}

type Location struct {
	Packfile objects.Checksum
	Offset   uint32
	Length   uint32
}

type DeltaEntry struct {
	Type     resources.Type
	Blob     objects.Checksum
	Location Location
}

/* /!\ Always keep those in sync with the serialized format on disk.
 * We are not using reflect.SizeOf because we might have padding in those structs
 */
const LocationSerializedSize = 32 + 4 + 4
const DeltaEntrySerializedSize = 1 + 32 + LocationSerializedSize

/*
 * A local version of the state, possibly aggregated, that uses on-disk storage.
 * - States are stored under a dedicated prefix key, with their data being the
 * state's metadata.
 * - Delta entries are stored under another dedicated prefix and are keyed by
 * their issuing state.
 */
type LocalState struct {
	Metadata Metadata

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
		cache: cache,
	}
}

func FromStream(rd io.Reader, cache caching.StateCache) (*LocalState, error) {
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
	var latestID *objects.Checksum = nil
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
func (ls *LocalState) InsertState(stateID objects.Checksum, rd io.Reader) error {
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

/* On disk format is <EntryType><Entry>...N<header>
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

	/* First we serialize all the LOCATIONS type entries */
	for _, entry := range ls.cache.GetDeltas() {
		w.Write([]byte{byte(ET_LOCATIONS)})
		w.Write(entry)
	}

	/* Finally we serialize the Metadata */
	w.Write([]byte{byte(ET_METADATA)})
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

	n, err := bbuf.Read(de.Blob[:])
	if err != nil {
		return
	}
	if n < len(objects.Checksum{}) {
		return de, fmt.Errorf("Short read while deserializing delta entry")
	}

	n, err = bbuf.Read(de.Location.Packfile[:])
	if err != nil {
		return
	}
	if n < len(objects.Checksum{}) {
		return de, fmt.Errorf("Short read while deserializing delta entry")
	}

	de.Location.Offset = binary.LittleEndian.Uint32(bbuf.Next(4))
	de.Location.Length = binary.LittleEndian.Uint32(bbuf.Next(4))

	return
}

func (de *DeltaEntry) _toBytes(buf []byte) {
	pos := 0
	buf[pos] = byte(de.Type)
	pos++

	pos += copy(buf[pos:], de.Blob[:])
	pos += copy(buf[pos:], de.Location.Packfile[:])
	binary.LittleEndian.PutUint32(buf[pos:], de.Location.Offset)
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:], de.Location.Length)
}

func (de *DeltaEntry) ToBytes() (ret []byte) {
	ret = make([]byte, DeltaEntrySerializedSize)
	de._toBytes(ret)
	return
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
	for {
		n, err := r.Read(et_buf)
		if err != nil || n != len(et_buf) {
			return fmt.Errorf("failed to read entry type %w", err)
		}

		if EntryType(et_buf[0]) == ET_METADATA {
			break
		}

		if n, err := io.ReadFull(r, de_buf); err != nil {
			return fmt.Errorf("failed to read delta entry %w, read(%d)/expected(%d)", err, n, DeltaEntrySerializedSize)
		}

		// We need to decode just to make the key, but we can reuse the buffer
		// to put inside the data part of the cache.
		delta, err := DeltaEntryFromBytes(de_buf)
		if err != nil {
			return fmt.Errorf("failed to deserialize delta entry %w", err)
		}

		ls.cache.PutDelta(delta.Type, delta.Blob, de_buf)
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

	aggregate := make([]byte, 1)
	if _, err := io.ReadFull(r, aggregate); err != nil {
		return fmt.Errorf("failed to read aggregate flag: %w", err)
	}

	serial := make([]byte, len(uuid.UUID{}))
	if _, err := io.ReadFull(r, serial); err != nil {
		return fmt.Errorf("failed to read serial: %w", err)
	}
	ls.Metadata.Serial = uuid.UUID(serial)

	return nil
}

func (ls *LocalState) HasState(stateID objects.Checksum) (bool, error) {
	return ls.cache.HasState(stateID)
}

func (ls *LocalState) DelState(stateID objects.Checksum) error {
	return ls.cache.DelState(stateID)
}

func (ls *LocalState) PutDelta(de DeltaEntry) error {
	return ls.cache.PutDelta(de.Type, de.Blob, de.ToBytes())
}

// XXX: Keeping those to minimize the diff, but this should get refactored into using PutDelta.
func (ls *LocalState) SetPackfileForBlob(Type resources.Type, packfileChecksum objects.Checksum, blobChecksum objects.Checksum, packfileOffset uint32, chunkLength uint32) {
	de := DeltaEntry{
		Type: Type,
		Blob: blobChecksum,
		Location: Location{
			Packfile: packfileChecksum,
			Offset:   packfileOffset,
			Length:   chunkLength,
		},
	}

	ls.PutDelta(de)
}

func (ls *LocalState) BlobExists(Type resources.Type, blobChecksum objects.Checksum) bool {
	has, _ := ls.cache.HasDelta(Type, blobChecksum)
	return has
}

func (ls *LocalState) GetSubpartForBlob(Type resources.Type, blobChecksum objects.Checksum) (objects.Checksum, uint32, uint32, bool) {
	/* XXX: We treat an error as missing data. Checking calling code I assume it's safe .. */
	delta, _ := ls.cache.GetDelta(Type, blobChecksum)
	if delta == nil {
		return objects.Checksum{}, 0, 0, false
	} else {
		de, _ := DeltaEntryFromBytes(delta)
		return de.Location.Packfile, de.Location.Offset, de.Location.Length, true
	}
}

func (ls *LocalState) ListSnapshots() iter.Seq[objects.Checksum] {
	return func(yield func(objects.Checksum) bool) {
		for csum, _ := range ls.cache.GetDeltasByType(resources.RT_SNAPSHOT) {
			// TODO: handling of deleted snaps.
			//st.muDeletedSnapshots.Lock()
			//_, deleted := st.DeletedSnapshots[k]
			//st.muDeletedSnapshots.Unlock()
			//if !deleted {
			if !yield(csum) {
				return
			}
			//}
		}
	}
}

func (ls *LocalState) ListObjectsOfType(Type resources.Type) iter.Seq2[DeltaEntry, error] {
	return func(yield func(DeltaEntry, error) bool) {
		for _, buf := range ls.cache.GetDeltasByType(Type) {
			de, err := DeltaEntryFromBytes(buf)

			if !yield(de, err) {
				return
			}
		}
	}

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
