package backup

import (
	"bytes"
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
	_ "github.com/PlakarKorp/plakar/snapshot/importer/fs"
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
	cache := caching.NewManager(tmpCacheDir)
	ctx.SetCache(cache)

	// Create a new logger
	logger := logging.NewLogger(bufOut, bufErr)
	// logger := logging.NewLogger(os.Stdout, os.Stderr)
	logger.EnableInfo()
	// logger.EnableTrace("all")
	ctx.SetLogger(logger)
	repo, err := repository.New(ctx, r, serializedConfig)
	require.NoError(t, err, "creating repository")

	return repo, tmpBackupDir
}

func TestExecuteCmdCreateDefault(t *testing.T) {
	bufOut := bytes.NewBuffer(nil)
	bufErr := bytes.NewBuffer(nil)

	repo, tmpBackupDir := generateFixtures(t, bufOut, bufErr)

	ctx := repo.AppContext()
	ctx.MaxConcurrency = 1
	// override the homedir to avoid having test overwriting existing home configuration
	ctx.HomeDir = repo.Location()
	args := []string{tmpBackupDir}

	subcommand, err := parse_cmd_backup(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)

	status, err := subcommand.Execute(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, 0, status)

	// output should be something like:
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/subdir/foo.txt
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/another_subdir/bar
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/subdir/dummy.txt
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/subdir/to_exclude
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/subdir
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/another_subdir
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254
	// info: 9a383818: OK ✓ /tmp
	// info: 9a383818: OK ✓ /
	// info: created unsigned snapshot 9a383818 with root PoRwWDCajeHqDG0vkZu13jOAWo3U/Wr9e/Hecg4IJoU of size 29 B in 10.961071ms

	output := bufOut.String()
	lines := strings.Split(strings.Trim(output, "\n"), "\n")
	require.Equal(t, 10, len(lines))
	// last line should have the summary
	lastline := lines[len(lines)-1]
	require.Contains(t, lastline, "created unsigned snapshot")
}

func TestExecuteCmdCreateDefaultWithExcludes(t *testing.T) {
	bufOut := bytes.NewBuffer(nil)
	bufErr := bytes.NewBuffer(nil)

	repo, tmpBackupDir := generateFixtures(t, bufOut, bufErr)

	ctx := repo.AppContext()
	ctx.MaxConcurrency = 1
	// override the homedir to avoid having test overwriting existing home configuration
	ctx.HomeDir = repo.Location()
	args := []string{"-excludes", tmpBackupDir + "/subdir/to_exclude", tmpBackupDir}

	subcommand, err := parse_cmd_backup(ctx, repo, args)
	require.NoError(t, err)
	require.NotNil(t, subcommand)

	status, err := subcommand.Execute(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, 0, status)

	// output should be something like:
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/subdir/foo.txt
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/another_subdir/bar
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/subdir/dummy.txt
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/subdir
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254/another_subdir
	// info: 9a383818: OK ✓ /tmp/tmp_to_backup2103009254
	// info: 9a383818: OK ✓ /tmp
	// info: 9a383818: OK ✓ /
	// info: created unsigned snapshot 9a383818 with root PoRwWDCajeHqDG0vkZu13jOAWo3U/Wr9e/Hecg4IJoU of size 29 B in 10.961071ms

	output := bufOut.String()
	lines := strings.Split(strings.Trim(output, "\n"), "\n")
	require.Equal(t, 9, len(lines))
	// last line should have the summary
	lastline := lines[len(lines)-1]
	require.Contains(t, lastline, "created unsigned snapshot")
}
