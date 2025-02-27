package ls

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/encryption/keypair"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/importer/fs"
	"github.com/PlakarKorp/plakar/storage"
	bfs "github.com/PlakarKorp/plakar/storage/backends/fs"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("TZ", "UTC")
}

func generateSnapshot(t *testing.T, keyPair *keypair.KeyPair) *snapshot.Snapshot {
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
	err = os.MkdirAll(tmpBackupDir+"/subdir", 0755)
	require.NoError(t, err)
	err = os.WriteFile(tmpBackupDir+"/subdir/dummy.txt", []byte("hello"), 0644)
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
	logger.EnableInfo()
	// logger.EnableTrace("all")
	ctx.SetLogger(logger)
	repo, err := repository.New(ctx, r, serializedConfig)
	require.NoError(t, err, "creating repository")

	// create a snapshot
	snap, err := snapshot.New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap)

	imp, err := fs.NewFSImporter(map[string]string{"location": tmpBackupDir})
	require.NoError(t, err)
	snap.Backup(imp, &snapshot.BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	err = snap.Repository().RebuildState()
	require.NoError(t, err)

	return snap
}

func TestExecuteCmdLsDefault(t *testing.T) {
	// Create a pipe to capture stdout
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	snap := generateSnapshot(t, nil)
	defer snap.Close()

	ctx := snap.AppContext()
	ctx.MaxConcurrency = 1
	repo := snap.Repository()
	args := []string{}

	subcommand, err := parse_cmd_ls(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)
	require.Equal(t, "ls", subcommand.(*Ls).Name())

	status, err := subcommand.Execute(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, 0, status)

	// Close the write end of the pipe and restore stdout
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()
	lines := strings.Split(strings.Trim(output, "\n"), "\n")
	require.Equal(t, 1, len(lines))
	fields := strings.Fields(lines[0])
	require.Equal(t, 6, len(fields))
	require.Equal(t, snap.Header.Timestamp.Local().Format(time.RFC3339), fields[0])
	require.Equal(t, hex.EncodeToString(snap.Header.GetIndexShortID()), fields[1])
	require.Equal(t, snap.Header.GetSource(0).Importer.Directory, fields[len(fields)-1])
}

func TestExecuteCmdLsFilterByIDAndRecursive(t *testing.T) {
	// Create a pipe to capture stdout
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	snap := generateSnapshot(t, nil)
	defer snap.Close()

	ctx := snap.AppContext()
	ctx.MaxConcurrency = 1
	repo := snap.Repository()
	args := []string{"-recursive", hex.EncodeToString(snap.Header.GetIndexShortID())}

	subcommand, err := parse_cmd_ls(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)

	status, err := subcommand.Execute(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, 0, status)

	// Close the write end of the pipe and restore stdout
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()
	lines := strings.Split(strings.Trim(output, "\n"), "\n")
	require.Equal(t, 2, len(lines))
	// last line should have the filename we backed up
	lastline := lines[len(lines)-1]
	fields := strings.Fields(lastline)
	require.Equal(t, 7, len(fields))
	require.Equal(t, snap.Header.Timestamp.Local().Format(time.RFC3339), fields[0])
	require.Equal(t, fmt.Sprintf("%s/subdir/dummy.txt", snap.Header.GetSource(0).Importer.Directory), fields[len(fields)-1])
}

func TestExecuteCmdLsFilterUuid(t *testing.T) {
	// Create a pipe to capture stdout
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	snap := generateSnapshot(t, nil)
	defer snap.Close()

	ctx := snap.AppContext()
	ctx.MaxConcurrency = 1
	repo := snap.Repository()
	args := []string{"-uuid"}

	subcommand, err := parse_cmd_ls(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)

	status, err := subcommand.Execute(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, 0, status)

	// Close the write end of the pipe and restore stdout
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()
	lines := strings.Split(strings.Trim(output, "\n"), "\n")
	require.Equal(t, 1, len(lines))
	fields := strings.Fields(lines[0])
	require.Equal(t, 6, len(fields))
	require.Equal(t, snap.Header.Timestamp.Local().Format(time.RFC3339), fields[0])
	indexId := snap.Header.GetIndexID()
	require.Equal(t, hex.EncodeToString(indexId[:]), fields[1])
	require.Equal(t, snap.Header.GetSource(0).Importer.Directory, fields[len(fields)-1])
}
