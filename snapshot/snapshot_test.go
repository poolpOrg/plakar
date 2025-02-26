package snapshot

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/encryption/keypair"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot/importer/fs"
	"github.com/PlakarKorp/plakar/storage"
	bfs "github.com/PlakarKorp/plakar/storage/backends/fs"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func generateSnapshot(t *testing.T, keyPair *keypair.KeyPair) *Snapshot {
	// init temporary directories
	tmpRepoDirRoot, err := os.MkdirTemp("", "tmp_repo")
	require.NoError(t, err)
	tmpRepoDir := fmt.Sprintf("%s/repo", tmpRepoDirRoot)
	tmpCacheDir, err := os.MkdirTemp("", "tmp_cache")
	require.NoError(t, err)
	tmpBackupDir, err := os.MkdirTemp("", "tmp_to_backup")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpRepoDir)
		os.RemoveAll(tmpCacheDir)
		os.RemoveAll(tmpBackupDir)
		os.RemoveAll(tmpRepoDirRoot)
	})
	// create a temporary file to backup later
	err = os.WriteFile(tmpBackupDir+"/dummy.txt", []byte("hello"), 0644)
	require.NoError(t, err)

	// create a storage
	r, err := bfs.NewRepository(map[string]string{"location": "fs://" + tmpRepoDir})
	require.NotNil(t, r)
	require.NoError(t, err)
	config := storage.NewConfiguration()
	serialized, err := config.ToBytes()
	require.NoError(t, err)

	hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)
	wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serialized))
	require.NoError(t, err)

	wrappedConfig, err := io.ReadAll(wrappedConfigRd)
	require.NoError(t, err)

	err = r.Create(wrappedConfig)
	require.NoError(t, err)

	// open the storage to load the configuration
	r, serializedConfig, err := storage.Open(map[string]string{"location": "fs://" + tmpRepoDir})
	require.NoError(t, err)

	// create a repository
	ctx := appcontext.NewAppContext()
	cache := caching.NewManager(tmpCacheDir)
	ctx.SetCache(cache)
	if keyPair != nil {
		ctx.Identity = uuid.New()
		ctx.Keypair = keyPair
	}
	logger := logging.NewLogger(os.Stdout, os.Stderr)
	//logger.EnableTrace("all")
	ctx.SetLogger(logger)
	repo, err := repository.New(ctx, r, serializedConfig)
	require.NoError(t, err, "creating repository")

	// create a snapshot
	snap, err := New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap)

	imp, err := fs.NewFSImporter(tmpBackupDir)
	require.NoError(t, err)
	snap.Backup(tmpBackupDir, imp, &BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	return snap
}

func TestSnapshot(t *testing.T) {
	snap := generateSnapshot(t, nil)
	defer snap.Close()

	appCtx := snap.AppContext()
	require.NotNil(t, appCtx)
	require.NotNil(t, appCtx.GetCache())
	defer appCtx.GetCache().Close()

	events := appCtx.Events()
	require.NotNil(t, events)

	err := snap.repository.RebuildState()
	require.NoError(t, err)

	snapFs, err := snap.Filesystem()
	require.NoError(t, err)
	require.NotNil(t, snapFs)

	snap2, err := Load(snap.repository, snap.Header.Identifier)
	require.NoError(t, err)
	require.NotNil(t, snap2)

	require.Equal(t, snap.Header.Identifier, snap2.Header.Identifier)
	require.Equal(t, snap.Header.Timestamp.Truncate(time.Nanosecond), snap2.Header.Timestamp.Truncate(time.Nanosecond))

	snap3, err := Clone(snap.repository, snap.Header.Identifier)
	require.NoError(t, err)
	require.NotNil(t, snap3)

	require.NotEqual(t, snap.Header.Identifier, snap3.Header.Identifier)

	snap4, err := Fork(snap.repository, snap.Header.Identifier)
	require.NoError(t, err)
	require.NotNil(t, snap4)

	require.NotEqual(t, snap.Header.Identifier, snap4.Header.Identifier)
}
