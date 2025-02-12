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
	var identifier objects.MAC

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

func Load(repo *repository.Repository, Identifier objects.MAC) (*Snapshot, error) {
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

func Clone(repo *repository.Repository, Identifier objects.MAC) (*Snapshot, error) {
	snap, err := Load(repo, Identifier)
	if err != nil {
		return nil, err
	}
	snap.Header.Timestamp = time.Now()

	uuidBytes, err := uuid.Must(uuid.NewRandom()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	snap.Header.Identifier = repo.ComputeMAC(uuidBytes[:])
	snap.packerChan = make(chan interface{}, runtime.NumCPU()*2+1)
	snap.packerChanDone = make(chan bool)
	go packerJob(snap)

	repo.Logger().Trace("snapshot", "%x: Clone(): %s", snap.Header.Identifier, snap.Header.GetIndexShortID())
	return snap, nil
}

func Fork(repo *repository.Repository, Identifier objects.MAC) (*Snapshot, error) {
	var identifier objects.MAC

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

func GetSnapshot(repo *repository.Repository, Identifier objects.MAC) (*header.Header, bool, error) {
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

func (snap *Snapshot) LookupObject(mac objects.MAC) (*objects.Object, error) {
	buffer, err := snap.GetBlob(resources.RT_OBJECT, mac)
	if err != nil {
		return nil, err
	}
	return objects.NewObjectFromBytes(buffer)
}

func (snap *Snapshot) ListChunks() (iter.Seq2[objects.MAC, error], error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	return func(yield func(objects.MAC, error) bool) {
		for filename, err := range fs.Files() {
			if err != nil {
				yield(objects.MAC{}, err)
				return
			}
			fsentry, err := fs.GetEntry(filename)
			if err != nil {
				yield(objects.MAC{}, err)
				return
			}
			if fsentry.ResolvedObject == nil {
				continue
			}
			for _, chunk := range fsentry.ResolvedObject.Chunks {
				if !yield(chunk.MAC, nil) {
					return
				}
			}
		}
	}, nil
}

func (snap *Snapshot) ListObjects() (iter.Seq2[objects.MAC, error], error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	return func(yield func(objects.MAC, error) bool) {
		for filename, err := range fs.Files() {
			if err != nil {
				yield(objects.MAC{}, err)
				return
			}
			fsentry, err := fs.GetEntry(filename)
			if err != nil {
				yield(objects.MAC{}, err)
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

func (snap *Snapshot) ListPackfiles() (iter.Seq2[objects.MAC, error], error) {
	pvfs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}

	return func(yield func(objects.MAC, error) bool) {
		packfile, exists := snap.repository.GetPackfileForBlob(resources.RT_SNAPSHOT, snap.Header.Identifier)
		if !exists {
			if !yield(objects.MAC{}, fmt.Errorf("snapshot packfile not found")) {
				return
			}
		}
		if !yield(packfile, nil) {
			return
		}

		if snap.Header.Identity.Identifier != uuid.Nil {
			packfile, exists := snap.repository.GetPackfileForBlob(resources.RT_SIGNATURE, snap.Header.Identifier)
			if !exists {
				if !yield(objects.MAC{}, fmt.Errorf("snapshot packfile not found")) {
					return
				}
			}
			if !yield(packfile, nil) {
				return
			}
		}

		packfile, exists = snap.repository.GetPackfileForBlob(resources.RT_VFS_BTREE, snap.Header.Sources[0].VFS.Root)
		if !exists {
			if !yield(objects.MAC{}, fmt.Errorf("snapshot packfile not found")) {
				return
			}
		}
		if !yield(packfile, nil) {
			return
		}

		/* Iterate over all the VFS, resolving both Nodes and actual VFS entries. */
		fsIter := pvfs.IterNodes()
		for fsIter.Next() {
			macNode, node := fsIter.Current()
			packfile, exists := snap.repository.GetPackfileForBlob(resources.RT_VFS_BTREE, macNode)
			if !exists {
				if !yield(objects.MAC{}, fmt.Errorf("snapshot packfile not found")) {
					return
				}
			}
			if !yield(packfile, nil) {
				return
			}

			for _, entry := range node.Values {
				packfile, exists := snap.repository.GetPackfileForBlob(resources.RT_VFS_ENTRY, entry)
				if !exists {
					if !yield(objects.MAC{}, fmt.Errorf("snapshot packfile not found")) {
						return
					}
				}
				if !yield(packfile, nil) {
					return
				}

				vfsEntry, err := pvfs.ResolveEntry(entry)
				if err != nil {
					if !yield(objects.MAC{}, fmt.Errorf("Failed to resolve entry %x", entry)) {
						return
					}
				}

				if vfsEntry.HasObject() {
					packfile, exists := snap.repository.GetPackfileForBlob(resources.RT_OBJECT, vfsEntry.Object)
					if !exists {
						if !yield(objects.MAC{}, fmt.Errorf("snapshot packfile not found")) {
							return
						}
					}
					if !yield(packfile, nil) {
						return
					}

					for _, chunk := range vfsEntry.ResolvedObject.Chunks {
						packfile, exists := snap.repository.GetPackfileForBlob(resources.RT_CHUNK, chunk.MAC)
						if !exists {
							if !yield(objects.MAC{}, fmt.Errorf("snapshot packfile not found")) {
								return
							}
						}
						if !yield(packfile, nil) {
							return
						}
					}

				}

			}

		}

		/* Finally iterate over all errors */
		errIter, err := pvfs.IterErrorNodes()
		if err != nil {
			if !yield(objects.MAC{}, fmt.Errorf("Could not load errors btree: %s", err)) {
				return
			}
		}

		for errIter.Next() {
			macNode, node := errIter.Current()
			packfile, exists := snap.repository.GetPackfileForBlob(resources.RT_ERROR_BTREE, macNode)
			if !exists {
				if !yield(objects.MAC{}, fmt.Errorf("snapshot packfile not found")) {
					return
				}
			}
			if !yield(packfile, nil) {
				return
			}

			for _, error := range node.Values {
				packfile, exists := snap.repository.GetPackfileForBlob(resources.RT_ERROR_ENTRY, error)
				if !exists {
					if !yield(objects.MAC{}, fmt.Errorf("snapshot packfile not found")) {
						return
					}
				}
				if !yield(packfile, nil) {
					return
				}
			}
		}

	}, nil
}

func (snap *Snapshot) Logger() *logging.Logger {
	return snap.AppContext().GetLogger()
}
