package restore

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot"
	_ "github.com/PlakarKorp/plakar/snapshot/exporter/fs"
	"github.com/PlakarKorp/plakar/snapshot/importer/fs"
	"github.com/PlakarKorp/plakar/storage"
	bfs "github.com/PlakarKorp/plakar/storage/backends/fs"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("TZ", "UTC")
}

func generateSnapshot(t *testing.T, bufOut *bytes.Buffer, bufErr *bytes.Buffer) *snapshot.Snapshot {
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
	// create temporary files to backup
	err = os.MkdirAll(tmpBackupDir+"/subdir", 0755)
	require.NoError(t, err)
	err = os.MkdirAll(tmpBackupDir+"/another_subdir", 0755)
	require.NoError(t, err)
	err = os.WriteFile(tmpBackupDir+"/subdir/dummy.txt", []byte("hello dummy"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(tmpBackupDir+"/subdir/foo.txt", []byte("hello foo"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(tmpBackupDir+"/subdir/to_exclude", []byte("*/subdir/to_exclude\n"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(tmpBackupDir+"/another_subdir/bar", []byte("hello bar"), 0644)
	require.NoError(t, err)

	// create a storage
	r := bfs.NewRepository("fs://" + tmpRepoDir)
	require.NotNil(t, r)
	config := storage.NewConfiguration()
	serialized, err := config.ToBytes()
	require.NoError(t, err)

	hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)
	wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serialized))
	require.NoError(t, err)

	wrappedConfig, err := io.ReadAll(wrappedConfigRd)
	require.NoError(t, err)

	err = r.Create("fs://"+tmpRepoDir, wrappedConfig)
	require.NoError(t, err)

	// open the storage to load the configuration
	r, serializedConfig, err := storage.Open("fs://" + tmpRepoDir)
	require.NoError(t, err)

	// create a repository
	ctx := appcontext.NewAppContext()
	ctx.Stdout = bufOut
	ctx.Stderr = bufErr
	cache := caching.NewManager(tmpCacheDir)
	ctx.SetCache(cache)

	// Create a new logger
	logger := logging.NewLogger(bufOut, bufErr)
	logger.EnableInfo()
	ctx.SetLogger(logger)
	repo, err := repository.New(ctx, r, serializedConfig)
	require.NoError(t, err, "creating repository")

	// create a snapshot
	snap, err := snapshot.New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap)

	imp, err := fs.NewFSImporter(tmpBackupDir)
	require.NoError(t, err)
	snap.Backup(tmpBackupDir, imp, &snapshot.BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	err = snap.Repository().RebuildState()
	require.NoError(t, err)

	return snap
}

func TestExecuteCmdRestoreDefault(t *testing.T) {
	bufOut := bytes.NewBuffer(nil)
	bufErr := bytes.NewBuffer(nil)

	snap := generateSnapshot(t, bufOut, bufErr)
	defer snap.Close()

	ctx := snap.AppContext()
	ctx.MaxConcurrency = 1
	repo := snap.Repository()
	// override the homedir to avoid having test overwriting existing home configuration
	ctx.HomeDir = repo.Location()

	tmpToRestoreDir, err := os.MkdirTemp("", "tmp_to_restore")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpToRestoreDir)
	})

	ctx.CWD = tmpToRestoreDir
	args := []string{}
	// args := []string{tmpBackupDir + "/subdir/dummy.txt"}

	subcommand, err := parse_cmd_restore(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)
	require.Equal(t, "restore", subcommand.(*Restore).Name())

	status, err := subcommand.Execute(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, 0, status)

	// output should be something like:
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/another_subdir/bar
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/another_subdir
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/subdir/dummy.txt
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/subdir/foo.txt
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/subdir/to_exclude
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/subdir
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /
	// 2025-02-25T21:35:42Z info: restore: restoration of 31b0d219:/ at /tmp/tmp_to_restore3971085618/plakar-2025-02-25T21:35:42Z completed successfully

	output := bufOut.String()
	lines := strings.Split(strings.Trim(output, "\n"), "\n")
	require.Equal(t, 10, len(lines))
	// last line should have the summary
	lastline := lines[len(lines)-1]
	require.Contains(t, lastline, "info: restore: restoration of")
}

func TestExecuteCmdRestoreSpecificSnapshot(t *testing.T) {
	bufOut := bytes.NewBuffer(nil)
	bufErr := bytes.NewBuffer(nil)

	// create one snapshot
	snap := generateSnapshot(t, bufOut, bufErr)
	defer snap.Close()

	ctx := snap.AppContext()
	ctx.MaxConcurrency = 1
	repo := snap.Repository()
	// override the homedir to avoid having test overwriting existing home configuration
	ctx.HomeDir = repo.Location()

	tmpToRestoreDir, err := os.MkdirTemp("", "tmp_to_restore")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpToRestoreDir)
	})

	ctx.CWD = tmpToRestoreDir
	indexId := snap.Header.GetIndexID()
	args := []string{fmt.Sprintf("%s", hex.EncodeToString(indexId[:]))}
	subcommand, err := parse_cmd_restore(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)
	require.Equal(t, "restore", subcommand.(*Restore).Name())

	status, err := subcommand.Execute(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, 0, status)

	// output should be something like:
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/another_subdir/bar
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/another_subdir
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/subdir/dummy.txt
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/subdir/foo.txt
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/subdir/to_exclude
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797/subdir
	// 2025-02-25T21:35:42Z info: 31b0d219: OK ✓ /tmp/tmp_to_backup1287588797
	// 2025-02-25T21:35:42Z info: restore: restoration of 31b0d219:/ at /tmp/tmp_to_restore3971085618/plakar-2025-02-25T21:35:42Z completed successfully

	output := bufOut.String()
	lines := strings.Split(strings.Trim(output, "\n"), "\n")
	require.Equal(t, 8, len(lines))
	// last line should have the summary
	lastline := lines[len(lines)-1]
	require.Contains(t, lastline, "info: restore: restoration of")
}
