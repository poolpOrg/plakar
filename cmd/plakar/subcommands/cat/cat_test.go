package cat

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/importer/fs"
	"github.com/PlakarKorp/plakar/storage"
	bfs "github.com/PlakarKorp/plakar/storage/backends/fs"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("TZ", "UTC")
}

func generateFixtures(t *testing.T, bufOut *bytes.Buffer, bufErr *bytes.Buffer) (*repository.Repository, string) {
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

	return repo, tmpBackupDir
}

func TestExecuteCmdCatDefault(t *testing.T) {
	bufOut := bytes.NewBuffer(nil)
	bufErr := bytes.NewBuffer(nil)

	repo, tmpBackupDir := generateFixtures(t, bufOut, bufErr)

	// create a snapshot
	snap, err := snapshot.New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap)

	imp, err := fs.NewFSImporter(tmpBackupDir)
	require.NoError(t, err)
	snap.Backup(tmpBackupDir, imp, &snapshot.BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	err = snap.Repository().RebuildState()
	require.NoError(t, err)

	ctx := repo.AppContext()
	ctx.MaxConcurrency = 1
	// override the homedir to avoid having test overwriting existing home configuration
	ctx.HomeDir = repo.Location()
	args := []string{tmpBackupDir + "/subdir/dummy.txt"}

	subcommand, err := parse_cmd_cat(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)

	status, err := subcommand.Execute(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, 0, status)

	output := bufOut.String()
	require.Equal(t, "hello dummy", output)
}

func TestExecuteCmdCatErrorAmbiguous(t *testing.T) {
	bufOut := bytes.NewBuffer(nil)
	bufErr := bytes.NewBuffer(nil)

	repo, tmpBackupDir := generateFixtures(t, bufOut, bufErr)

	// create one snapshot
	snap, err := snapshot.New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap)

	imp, err := fs.NewFSImporter(tmpBackupDir)
	require.NoError(t, err)
	snap.Backup(tmpBackupDir, imp, &snapshot.BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	err = snap.Repository().RebuildState()
	require.NoError(t, err)

	// create second snapshot
	snap2, err := snapshot.New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap2)

	snap2.Backup(tmpBackupDir, imp, &snapshot.BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	err = snap2.Repository().RebuildState()
	require.NoError(t, err)

	ctx := repo.AppContext()
	ctx.MaxConcurrency = 1
	// override the homedir to avoid having test overwriting existing home configuration
	ctx.HomeDir = repo.Location()
	args := []string{tmpBackupDir + "/subdir/dummy.txt"}

	subcommand, err := parse_cmd_cat(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)

	status, err := subcommand.Execute(ctx, repo)
	require.Error(t, err, "errors occurred")
	require.Equal(t, 1, status)

	outputErr := bufErr.String()
	require.Contains(t, outputErr, "snapshot ID is ambiguous:  (matches 2 snapshots)")
}

func TestExecuteCmdCatErrorNotRegularFile(t *testing.T) {
	bufOut := bytes.NewBuffer(nil)
	bufErr := bytes.NewBuffer(nil)

	repo, tmpBackupDir := generateFixtures(t, bufOut, bufErr)

	// create one snapshot
	snap, err := snapshot.New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap)

	imp, err := fs.NewFSImporter(tmpBackupDir)
	require.NoError(t, err)
	snap.Backup(tmpBackupDir, imp, &snapshot.BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	err = snap.Repository().RebuildState()
	require.NoError(t, err)

	// create second snapshot
	snap2, err := snapshot.New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap2)

	snap2.Backup(tmpBackupDir, imp, &snapshot.BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	err = snap2.Repository().RebuildState()
	require.NoError(t, err)

	ctx := repo.AppContext()
	ctx.MaxConcurrency = 1
	// override the homedir to avoid having test overwriting existing home configuration
	ctx.HomeDir = repo.Location()
	args := []string{fmt.Sprintf("%s:/", hex.EncodeToString(snap2.Header.GetIndexShortID()))}

	subcommand, err := parse_cmd_cat(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)

	status, err := subcommand.Execute(ctx, repo)
	require.Error(t, err, "errors occurred")
	require.Equal(t, 1, status)

	outputErr := bufErr.String()
	require.Contains(t, outputErr, "not a regular file")
}

func TestExecuteCmdCatErrorUnknownFile(t *testing.T) {
	bufOut := bytes.NewBuffer(nil)
	bufErr := bytes.NewBuffer(nil)

	repo, tmpBackupDir := generateFixtures(t, bufOut, bufErr)

	// create one snapshot
	snap, err := snapshot.New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap)

	imp, err := fs.NewFSImporter(tmpBackupDir)
	require.NoError(t, err)
	snap.Backup(tmpBackupDir, imp, &snapshot.BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	err = snap.Repository().RebuildState()
	require.NoError(t, err)

	// create second snapshot
	snap2, err := snapshot.New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap2)

	snap2.Backup(tmpBackupDir, imp, &snapshot.BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	err = snap2.Repository().RebuildState()
	require.NoError(t, err)

	ctx := repo.AppContext()
	ctx.MaxConcurrency = 1
	// override the homedir to avoid having test overwriting existing home configuration
	ctx.HomeDir = repo.Location()
	args := []string{fmt.Sprintf("%s:/unknown", hex.EncodeToString(snap2.Header.GetIndexShortID()))}

	subcommand, err := parse_cmd_cat(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)

	status, err := subcommand.Execute(ctx, repo)
	require.Error(t, err, "errors occurred")
	require.Equal(t, 1, status)

	outputErr := bufErr.String()
	require.Contains(t, outputErr, "cat: /unknown: no such file")
}

func TestExecuteCmdCatHighlight(t *testing.T) {
	bufOut := bytes.NewBuffer(nil)
	bufErr := bytes.NewBuffer(nil)

	repo, tmpBackupDir := generateFixtures(t, bufOut, bufErr)

	// create a snapshot
	snap, err := snapshot.New(repo)
	require.NoError(t, err)
	require.NotNil(t, snap)

	imp, err := fs.NewFSImporter(tmpBackupDir)
	require.NoError(t, err)
	snap.Backup(tmpBackupDir, imp, &snapshot.BackupOptions{Name: "test_backup", MaxConcurrency: 1})

	err = snap.Repository().RebuildState()
	require.NoError(t, err)

	ctx := repo.AppContext()
	ctx.MaxConcurrency = 1
	// override the homedir to avoid having test overwriting existing home configuration
	ctx.HomeDir = repo.Location()
	args := []string{"--highlight", tmpBackupDir + "/subdir/dummy.txt"}

	subcommand, err := parse_cmd_cat(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)

	status, err := subcommand.Execute(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, 0, status)

	output := bufOut.String()
	require.Equal(t, "\x1b[1m\x1b[37mhello dummy\x1b[0m", output)
}
