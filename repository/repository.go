package repository

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"iter"
	"strings"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/repository/state"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/google/uuid"
)

var (
	ErrPackfileNotFound = errors.New("packfile not found")
	ErrBlobNotFound     = errors.New("blob not found")
)

type Repository struct {
	store         storage.Store
	state         *state.LocalState
	configuration storage.Configuration

	appContext *appcontext.AppContext
}

func Inexistant(ctx *appcontext.AppContext, repositoryPath string) (*Repository, error) {
	st, err := storage.New(map[string]string{"location": repositoryPath})
	if err != nil {
		return nil, err
	}
	defer st.Close()

	return &Repository{
		store:         st,
		configuration: *storage.NewConfiguration(),
		appContext:    ctx,
	}, nil
}

func New(ctx *appcontext.AppContext, store storage.Store, config []byte) (*Repository, error) {
	t0 := time.Now()
	defer func() {
		ctx.GetLogger().Trace("repository", "New(store=%p): %s", store, time.Since(t0))
	}()

	var hasher hash.Hash
	if ctx.GetSecret() != nil {
		hasher = hashing.GetMACHasher(storage.DEFAULT_HASHING_ALGORITHM, ctx.GetSecret())
	} else {
		hasher = hashing.GetHasher(storage.DEFAULT_HASHING_ALGORITHM)
	}

	version, unwrappedConfigRd, err := storage.Deserialize(hasher, resources.RT_CONFIG, bytes.NewReader(config))
	if err != nil {
		return nil, err
	}

	unwrappedConfig, err := io.ReadAll(unwrappedConfigRd)
	if err != nil {
		return nil, err
	}

	configInstance, err := storage.NewConfigurationFromBytes(version, unwrappedConfig)
	if err != nil {
		return nil, err
	}

	r := &Repository{
		store:         store,
		configuration: *configInstance,
		appContext:    ctx,
	}

	if err := r.RebuildState(); err != nil {
		return nil, err
	}

	return r, nil
}

func NewNoRebuild(ctx *appcontext.AppContext, store storage.Store, config []byte) (*Repository, error) {
	t0 := time.Now()
	defer func() {
		ctx.GetLogger().Trace("repository", "NewNoRebuild(store=%p): %s", store, time.Since(t0))
	}()

	var hasher hash.Hash
	if ctx.GetSecret() != nil {
		hasher = hashing.GetMACHasher(storage.DEFAULT_HASHING_ALGORITHM, ctx.GetSecret())
	} else {
		hasher = hashing.GetHasher(storage.DEFAULT_HASHING_ALGORITHM)
	}

	version, unwrappedConfigRd, err := storage.Deserialize(hasher, resources.RT_CONFIG, bytes.NewReader(config))
	if err != nil {
		return nil, err
	}

	unwrappedConfig, err := io.ReadAll(unwrappedConfigRd)
	if err != nil {
		return nil, err
	}

	configInstance, err := storage.NewConfigurationFromBytes(version, unwrappedConfig)
	if err != nil {
		return nil, err
	}

	r := &Repository{
		store:         store,
		configuration: *configInstance,
		appContext:    ctx,
	}

	return r, nil
}

func (r *Repository) RebuildState() error {
	cacheInstance, err := r.AppContext().GetCache().Repository(r.Configuration().RepositoryID)
	if err != nil {
		return err
	}

	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "rebuildState(): %s", time.Since(t0))
	}()

	/* Use on-disk local state, and merge it with repository's own state */
	aggregatedState := state.NewLocalState(cacheInstance)

	// identify local states
	localStates, err := cacheInstance.GetStates()
	if err != nil {
		return err
	}

	// identify remote states
	remoteStates, err := r.GetStates()
	if err != nil {
		return err
	}

	remoteStatesMap := make(map[objects.MAC]struct{})
	for _, stateID := range remoteStates {
		remoteStatesMap[stateID] = struct{}{}
	}

	// build delta of local and remote states
	localStatesMap := make(map[objects.MAC]struct{})
	outdatedStates := make([]objects.MAC, 0)
	for stateID := range localStates {
		localStatesMap[stateID] = struct{}{}

		if _, exists := remoteStatesMap[stateID]; !exists {
			outdatedStates = append(outdatedStates, stateID)
		}
	}

	missingStates := make([]objects.MAC, 0)
	for _, stateID := range remoteStates {
		if _, exists := localStatesMap[stateID]; !exists {
			missingStates = append(missingStates, stateID)
		}
	}

	for _, stateID := range missingStates {
		version, remoteStateRd, err := r.GetState(stateID)
		if err != nil {
			return err
		}

		if err := aggregatedState.InsertState(version, stateID, remoteStateRd); err != nil {
			return err
		}
	}

	// delete local states that are not present in remote
	for _, stateID := range outdatedStates {
		if err := aggregatedState.DelState(stateID); err != nil {
			return err
		}
	}

	r.state = aggregatedState

	// The first Serial id is our repository ID, this allows us to deal
	// naturally with concurrent first backups.
	r.state.UpdateSerialOr(r.configuration.RepositoryID)

	return nil
}

func (r *Repository) AppContext() *appcontext.AppContext {
	return r.appContext
}

func (r *Repository) Store() storage.Store {
	return r.store
}

func (r *Repository) Close() error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "Close(): %s", time.Since(t0))
	}()
	return nil
}

func (r *Repository) Decode(input io.Reader) (io.Reader, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "Decode: %s", time.Since(t0))
	}()

	stream := input
	if r.AppContext().GetSecret() != nil {
		tmp, err := encryption.DecryptStream(r.configuration.Encryption, r.AppContext().GetSecret(), stream)
		if err != nil {
			return nil, err
		}
		stream = tmp
	}

	if r.configuration.Compression != nil {
		tmp, err := compression.InflateStream(r.configuration.Compression.Algorithm, stream)
		if err != nil {
			return nil, err
		}
		stream = tmp
	}

	return stream, nil
}

func (r *Repository) Encode(input io.Reader) (io.Reader, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "Encode: %s", time.Since(t0))
	}()

	stream := input
	if r.configuration.Compression != nil {
		tmp, err := compression.DeflateStream(r.configuration.Compression.Algorithm, stream)
		if err != nil {
			return nil, err
		}
		stream = tmp
	}

	if r.AppContext().GetSecret() != nil {
		tmp, err := encryption.EncryptStream(r.configuration.Encryption, r.AppContext().GetSecret(), stream)
		if err != nil {
			return nil, err
		}
		stream = tmp
	}

	return stream, nil
}

func (r *Repository) DecodeBuffer(buffer []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "Decode(%d bytes): %s", len(buffer), time.Since(t0))
	}()

	rd, err := r.Decode(bytes.NewBuffer(buffer))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rd)
}

func (r *Repository) EncodeBuffer(buffer []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "Encode(%d): %s", len(buffer), time.Since(t0))
	}()

	rd, err := r.Encode(bytes.NewBuffer(buffer))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rd)
}

func (r *Repository) GetMACHasher() hash.Hash {
	secret := r.AppContext().GetSecret()
	if secret == nil {
		// unencrypted repo, derive 32-bytes "secret" from RepositoryID
		// so ComputeMAC can be used similarly to encrypted repos
		hasher := hashing.GetHasher(r.Configuration().Hashing.Algorithm)
		hasher.Write(r.configuration.RepositoryID[:])
		secret = hasher.Sum(nil)
	}
	return hashing.GetMACHasher(r.Configuration().Hashing.Algorithm, secret)
}

func (r *Repository) ComputeMAC(data []byte) objects.MAC {
	hasher := r.GetMACHasher()
	hasher.Write(data)
	result := hasher.Sum(nil)

	if len(result) != 32 {
		panic("hasher returned invalid length")
	}

	var mac objects.MAC
	copy(mac[:], result)

	return mac
}

func (r *Repository) Chunker(rd io.ReadCloser) (*chunkers.Chunker, error) {
	chunkingAlgorithm := r.configuration.Chunking.Algorithm
	chunkingMinSize := r.configuration.Chunking.MinSize
	chunkingNormalSize := r.configuration.Chunking.NormalSize
	chunkingMaxSize := r.configuration.Chunking.MaxSize

	return chunkers.NewChunker(strings.ToLower(chunkingAlgorithm), rd, &chunkers.ChunkerOpts{
		MinSize:    int(chunkingMinSize),
		NormalSize: int(chunkingNormalSize),
		MaxSize:    int(chunkingMaxSize),
	})
}

func (r *Repository) NewStateDelta(cache *caching.ScanCache) *state.LocalState {
	return r.state.Derive(cache)
}

func (r *Repository) Location() string {
	return r.store.Location()
}

func (r *Repository) Configuration() storage.Configuration {
	return r.configuration
}

func (r *Repository) GetSnapshots() ([]objects.MAC, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetSnapshots(): %s", time.Since(t0))
	}()

	ret := make([]objects.MAC, 0)
	for snapshotID := range r.state.ListSnapshots() {
		ret = append(ret, snapshotID)
	}
	return ret, nil
}

func (r *Repository) DeleteSnapshot(snapshotID objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "DeleteSnapshot(%x): %s", snapshotID, time.Since(t0))
	}()

	var identifier objects.MAC
	n, err := rand.Read(identifier[:])
	if err != nil {
		return err
	}
	if n != len(identifier) {
		return io.ErrShortWrite
	}

	sc, err := r.AppContext().GetCache().Scan(identifier)
	if err != nil {
		return err
	}
	deltaState := r.state.Derive(sc)

	ret := deltaState.DeleteResource(resources.RT_SNAPSHOT, snapshotID)
	if ret != nil {
		return ret
	}

	buffer := &bytes.Buffer{}
	err = deltaState.SerializeToStream(buffer)
	if err != nil {
		return err
	}

	mac := r.ComputeMAC(buffer.Bytes())
	if err := r.PutState(mac, buffer); err != nil {
		return err
	}

	return nil
}

func (r *Repository) GetStates() ([]objects.MAC, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetStates(): %s", time.Since(t0))
	}()

	return r.store.GetStates()
}

func (r *Repository) GetState(mac objects.MAC) (versioning.Version, io.Reader, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetState(%x): %s", mac, time.Since(t0))
	}()

	rd, err := r.store.GetState(mac)
	if err != nil {
		return versioning.Version(0), nil, err
	}

	version, rd, err := storage.Deserialize(r.GetMACHasher(), resources.RT_STATE, rd)
	if err != nil {
		return versioning.Version(0), nil, err
	}

	rd, err = r.Decode(rd)
	if err != nil {
		return versioning.Version(0), nil, err
	}
	return version, rd, err
}

func (r *Repository) PutState(mac objects.MAC, rd io.Reader) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "PutState(%x, ...): %s", mac, time.Since(t0))
	}()

	rd, err := r.Encode(rd)
	if err != nil {
		return err
	}

	rd, err = storage.Serialize(r.GetMACHasher(), resources.RT_STATE, versioning.GetCurrentVersion(resources.RT_STATE), rd)
	if err != nil {
		return err
	}

	return r.store.PutState(mac, rd)
}

func (r *Repository) DeleteState(mac objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "DeleteState(%x, ...): %s", mac, time.Since(t0))
	}()

	return r.store.DeleteState(mac)
}

func (r *Repository) GetPackfiles() ([]objects.MAC, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetPackfiles(): %s", time.Since(t0))
	}()

	return r.store.GetPackfiles()
}

func (r *Repository) GetPackfile(mac objects.MAC) (*packfile.PackFile, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetPackfile(%x, ...): %s", mac, time.Since(t0))
	}()

	hasher := r.GetMACHasher()

	rd, err := r.store.GetPackfile(mac)
	if err != nil {
		return nil, err
	}

	packfileVersion, rd, err := storage.Deserialize(hasher, resources.RT_PACKFILE, rd)
	if err != nil {
		return nil, err
	}

	rawPackfile, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	footerBufLength := binary.LittleEndian.Uint32(rawPackfile[len(rawPackfile)-4:])
	rawPackfile = rawPackfile[:len(rawPackfile)-4]

	footerbuf := rawPackfile[len(rawPackfile)-int(footerBufLength):]
	rawPackfile = rawPackfile[:len(rawPackfile)-int(footerBufLength)]

	footerbuf, err = r.DecodeBuffer(footerbuf)
	if err != nil {
		return nil, err
	}

	footer, err := packfile.NewFooterFromBytes(packfileVersion, footerbuf)
	if err != nil {
		return nil, err
	}

	indexbuf := rawPackfile[int(footer.IndexOffset):]
	rawPackfile = rawPackfile[:int(footer.IndexOffset)]

	indexbuf, err = r.DecodeBuffer(indexbuf)
	if err != nil {
		return nil, err
	}

	hasher.Reset()
	hasher.Write(indexbuf)

	if !bytes.Equal(hasher.Sum(nil), footer.IndexMAC[:]) {
		return nil, fmt.Errorf("packfile: index MAC mismatch")
	}

	rawPackfile = append(rawPackfile, indexbuf...)
	rawPackfile = append(rawPackfile, footerbuf...)

	hasher.Reset()
	p, err := packfile.NewFromBytes(hasher, packfileVersion, rawPackfile)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (r *Repository) GetPackfileBlob(loc state.Location) (io.ReadSeeker, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetPackfileBlob(%x, %d, %d): %s", loc.Packfile, loc.Offset, loc.Length, time.Since(t0))
	}()

	rd, err := r.store.GetPackfileBlob(loc.Packfile, loc.Offset+uint64(storage.STORAGE_HEADER_SIZE), loc.Length)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	decoded, err := r.DecodeBuffer(data)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(decoded), nil
}

func (r *Repository) PutPackfile(mac objects.MAC, rd io.Reader) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "PutPackfile(%x, ...): %s", mac, time.Since(t0))
	}()

	rd, err := storage.Serialize(r.GetMACHasher(), resources.RT_PACKFILE, versioning.GetCurrentVersion(resources.RT_PACKFILE), rd)
	if err != nil {
		return err
	}
	return r.store.PutPackfile(mac, rd)
}

// Deletes a packfile from the store. Warning this is a true delete and is unrecoverable.
func (r *Repository) DeletePackfile(mac objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "DeletePackfile(%x): %s", mac, time.Since(t0))
	}()

	return r.store.DeletePackfile(mac)
}

// Removes the packfile from the state, making it unreachable.
func (r *Repository) RemovePackfile(packfileMAC objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "RemovePackfile(%x): %s", packfileMAC, time.Since(t0))
	}()
	return r.state.DelPackfile(packfileMAC)
}

func (r *Repository) HasDeletedPackfile(mac objects.MAC) (bool, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "HasDeletedPackfile(%x): %s", mac, time.Since(t0))
	}()

	return r.state.HasDeletedResource(resources.RT_PACKFILE, mac)
}

func (r *Repository) ListDeletedPackfiles() iter.Seq2[objects.MAC, time.Time] {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "ListDeletedPackfiles(): %s", time.Since(t0))
	}()

	return func(yield func(objects.MAC, time.Time) bool) {
		for snap, err := range r.state.ListDeletedResources(resources.RT_PACKFILE) {

			if err != nil {
				r.Logger().Error("Failed to fetch deleted packfile %s", err)
			}

			if !yield(snap.Blob, snap.When) {
				return
			}
		}
	}
}

func (r *Repository) ListDeletedSnapShots() iter.Seq2[objects.MAC, time.Time] {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "ListDeletedSnapShots(): %s", time.Since(t0))
	}()

	return func(yield func(objects.MAC, time.Time) bool) {
		for snap, err := range r.state.ListDeletedResources(resources.RT_SNAPSHOT) {

			if err != nil {
				r.Logger().Error("Failed to fetch deleted snapshot %s", err)
			}

			if !yield(snap.Blob, snap.When) {
				return
			}
		}
	}
}

// Removes the deleted packfile entry from the state.
func (r *Repository) RemoveDeletedPackfile(packfileMAC objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "RemoveDeletedPackfile(%x): %s", packfileMAC, time.Since(t0))
	}()

	return r.state.DelDeletedResource(resources.RT_PACKFILE, packfileMAC)
}

func (r *Repository) GetPackfileForBlob(Type resources.Type, mac objects.MAC) (objects.MAC, bool, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetPackfileForBlob(%x): %s", mac, time.Since(t0))
	}()

	packfile, exists, err := r.state.GetSubpartForBlob(Type, mac)

	return packfile.Packfile, exists, err
}

func (r *Repository) GetBlob(Type resources.Type, mac objects.MAC) (io.ReadSeeker, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetBlob(%s, %x): %s", Type, mac, time.Since(t0))
	}()

	loc, exists, err := r.state.GetSubpartForBlob(Type, mac)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, ErrPackfileNotFound
	}

	rd, err := r.GetPackfileBlob(loc)
	if err != nil {
		return nil, err
	}

	return rd, nil
}

func (r *Repository) BlobExists(Type resources.Type, mac objects.MAC) bool {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "BlobExists(%s, %x): %s", Type, mac, time.Since(t0))
	}()
	return r.state.BlobExists(Type, mac)
}

// Removes the provided blob from our state, making it unreachable
func (r *Repository) RemoveBlob(Type resources.Type, mac, packfileMAC objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "DeleteBlob(%s, %x, %x): %s", Type, mac, packfileMAC, time.Since(t0))
	}()
	return r.state.DelDelta(Type, mac, packfileMAC)
}

func (r *Repository) ListOrphanBlobs() iter.Seq2[state.DeltaEntry, error] {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "ListSnapshots(): %s", time.Since(t0))
	}()
	return r.state.ListOrphanDeltas()
}

func (r *Repository) ListSnapshots() iter.Seq[objects.MAC] {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "ListSnapshots(): %s", time.Since(t0))
	}()
	return r.state.ListSnapshots()
}

func (r *Repository) ListPackfiles() iter.Seq[objects.MAC] {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "ListPackfiles(): %s", time.Since(t0))
	}()
	return r.state.ListPackfiles()
}

// Saves the full aggregated state to the repository, might be heavy handed use
// with care.
func (r *Repository) PutCurrentState() error {
	pr, pw := io.Pipe()

	/* By using a pipe and a goroutine we bound the max size in memory. */
	go func() {
		defer pw.Close()
		if err := r.state.SerializeToStream(pw); err != nil {
			pw.CloseWithError(err)
		}
	}()

	newSerial := uuid.New()
	r.state.Metadata.Serial = newSerial
	id := r.ComputeMAC(newSerial[:])

	return r.PutState(id, pr)
}

func (r *Repository) Logger() *logging.Logger {
	return r.AppContext().GetLogger()
}
