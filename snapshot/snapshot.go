package snapshot

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"iter"
	"runtime"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/repository/state"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/google/uuid"
)

var (
	ErrNotFound = errors.New("snapshot not found")
)

type Snapshot struct {
	repository *repository.Repository
	scanCache  *caching.ScanCache

	deltaState *state.LocalState

	filesystem *vfs.Filesystem

	SkipDirs []string

	Header *header.Header

	packerChan     chan interface{}
	packerChanDone chan bool
}

func New(repo *repository.Repository) (*Snapshot, error) {
	var identifier objects.Checksum

	n, err := rand.Read(identifier[:])
	if err != nil {
		return nil, err
	}
	if n != len(identifier) {
		return nil, io.ErrShortWrite
	}

	scanCache, err := repo.AppContext().GetCache().Scan(identifier)
	if err != nil {
		return nil, err
	}

	snap := &Snapshot{
		repository: repo,
		scanCache:  scanCache,

		Header: header.NewHeader("default", identifier),

		packerChan:     make(chan interface{}, runtime.NumCPU()*2+1),
		packerChanDone: make(chan bool),
	}

	snap.deltaState = repo.NewStateDelta(scanCache)

	if snap.AppContext().Identity != uuid.Nil {
		snap.Header.Identity.Identifier = snap.AppContext().Identity
		snap.Header.Identity.PublicKey = snap.AppContext().Keypair.PublicKey
	}

	snap.Header.SetContext("Hostname", snap.AppContext().Hostname)
	snap.Header.SetContext("Username", snap.AppContext().Username)
	snap.Header.SetContext("OperatingSystem", snap.AppContext().OperatingSystem)
	snap.Header.SetContext("MachineID", snap.AppContext().MachineID)
	snap.Header.SetContext("CommandLine", snap.AppContext().CommandLine)
	snap.Header.SetContext("ProcessID", fmt.Sprintf("%d", snap.AppContext().ProcessID))
	snap.Header.SetContext("Architecture", snap.AppContext().Architecture)
	snap.Header.SetContext("NumCPU", fmt.Sprintf("%d", runtime.NumCPU()))
	snap.Header.SetContext("MaxProcs", fmt.Sprintf("%d", runtime.GOMAXPROCS(0)))
	snap.Header.SetContext("Client", snap.AppContext().Client)

	go packerJob(snap)

	repo.Logger().Trace("snapshot", "%x: New()", snap.Header.GetIndexShortID())
	return snap, nil
}

func Load(repo *repository.Repository, Identifier objects.Checksum) (*Snapshot, error) {
	hdr, _, err := GetSnapshot(repo, Identifier)
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{}
	snapshot.repository = repo
	snapshot.Header = hdr

	repo.Logger().Trace("snapshot", "%x: Load()", snapshot.Header.GetIndexShortID())
	return snapshot, nil
}

func Clone(repo *repository.Repository, Identifier objects.Checksum) (*Snapshot, error) {
	snap, err := Load(repo, Identifier)
	if err != nil {
		return nil, err
	}
	snap.Header.Timestamp = time.Now()

	uuidBytes, err := uuid.Must(uuid.NewRandom()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	snap.Header.Identifier = repo.Checksum(uuidBytes[:])
	snap.packerChan = make(chan interface{}, runtime.NumCPU()*2+1)
	snap.packerChanDone = make(chan bool)
	go packerJob(snap)

	repo.Logger().Trace("snapshot", "%x: Clone(): %s", snap.Header.Identifier, snap.Header.GetIndexShortID())
	return snap, nil
}

func Fork(repo *repository.Repository, Identifier objects.Checksum) (*Snapshot, error) {
	var identifier objects.Checksum

	n, err := rand.Read(identifier[:])
	if err != nil {
		return nil, err
	}
	if n != len(identifier) {
		return nil, io.ErrShortWrite
	}

	snap, err := Clone(repo, Identifier)
	if err != nil {
		return nil, err
	}

	snap.Header.Identifier = identifier

	snap.Logger().Trace("snapshot", "%x: Fork(): %x", snap.Header.Identifier, snap.Header.GetIndexShortID())
	return snap, nil
}

func (snap *Snapshot) Close() error {
	snap.Logger().Trace("snapshot", "%x: Close(): %x", snap.Header.Identifier, snap.Header.GetIndexShortID())

	if snap.scanCache != nil {
		return snap.scanCache.Close()
	}

	return nil
}

func (snap *Snapshot) AppContext() *appcontext.AppContext {
	return snap.Repository().AppContext()
}

func (snap *Snapshot) Event(evt events.Event) {
	snap.AppContext().Events().Send(evt)
}

func GetSnapshot(repo *repository.Repository, Identifier objects.Checksum) (*header.Header, bool, error) {
	repo.Logger().Trace("snapshot", "repository.GetSnapshot(%x)", Identifier)

	rd, err := repo.GetBlob(resources.RT_SNAPSHOT, Identifier)
	if err != nil {
		if errors.Is(err, repository.ErrBlobNotFound) {
			err = ErrNotFound
		}
		return nil, false, err
	}

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return nil, false, err
	}

	hdr, err := header.NewFromBytes(buffer)
	if err != nil {
		return nil, false, err
	}

	return hdr, false, nil
}

func (snap *Snapshot) Repository() *repository.Repository {
	return snap.repository
}

func (snap *Snapshot) LookupObject(checksum objects.Checksum) (*objects.Object, error) {
	buffer, err := snap.GetBlob(resources.RT_OBJECT, checksum)
	if err != nil {
		return nil, err
	}
	return objects.NewObjectFromBytes(buffer)
}

func (snap *Snapshot) ListChunks() (iter.Seq2[objects.Checksum, error], error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	return func(yield func(objects.Checksum, error) bool) {
		for filename, err := range fs.Files() {
			if err != nil {
				yield(objects.Checksum{}, err)
				return
			}
			fsentry, err := fs.GetEntry(filename)
			if err != nil {
				yield(objects.Checksum{}, err)
				return
			}
			if fsentry.ResolvedObject == nil {
				continue
			}
			for _, chunk := range fsentry.ResolvedObject.Chunks {
				if !yield(chunk.Checksum, nil) {
					return
				}
			}
		}
	}, nil
}

func (snap *Snapshot) ListObjects() (iter.Seq2[objects.Checksum, error], error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	return func(yield func(objects.Checksum, error) bool) {
		for filename, err := range fs.Files() {
			if err != nil {
				yield(objects.Checksum{}, err)
				return
			}
			fsentry, err := fs.GetEntry(filename)
			if err != nil {
				yield(objects.Checksum{}, err)
				return
			}
			if fsentry.ResolvedObject == nil {
				continue
			}
			if !yield(fsentry.Object, nil) {
				return
			}
		}
	}, nil
}

func (snap *Snapshot) Logger() *logging.Logger {
	return snap.AppContext().GetLogger()
}
