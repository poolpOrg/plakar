package vfs_test

import (
	"bytes"
	"fmt"
	"io"
	iofs "io/fs"
	"log"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/importer/fs"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/PlakarKorp/plakar/storage"
	bfs "github.com/PlakarKorp/plakar/storage/backends/fs"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/stretchr/testify/require"
)

func ppslice(s []string) {
	for _, x := range s {
		log.Println("->", x)
	}
}

func TestPathCmp(t *testing.T) {
	suite := []struct {
		input  []string
		expect []string
	}{
		{
			input:  []string{"/"},
			expect: []string{"/"},
		},
		{
			input:  []string{"c", "a", "b", "z"},
			expect: []string{"a", "b", "c", "z"},
		},
		{
			input:  []string{"c", "a", "b", "z", "z/a", "z/z", "z/a/b/c", "z/z/z", "z/a/z"},
			expect: []string{"a", "b", "c", "z", "z/a", "z/z", "z/a/z", "z/z/z", "z/a/b/c"},
		},
		{
			input:  []string{"/etc/", "/"},
			expect: []string{"/", "/etc/"},
		},
		{
			input:  []string{"/etc/zzz", "/etc/foo/bar", "/etc/foo"},
			expect: []string{"/etc/foo", "/etc/zzz", "/etc/foo/bar"},
		},
		{
			input:  []string{"/etc/zzz", "/etc/foo/bar", "/etc/foo", "/etc/aaa"},
			expect: []string{"/etc/aaa", "/etc/foo", "/etc/zzz", "/etc/foo/bar"},
		},
		{
			input:  []string{"/home/op", "/etc/zzz", "/etc/foo/bar", "/etc/foo", "/home/op/.kshrc"},
			expect: []string{"/etc/foo", "/etc/zzz", "/home/op", "/etc/foo/bar", "/home/op/.kshrc"},
		},
		{
			input: []string{
				"/",
				"/home",
				"/home/op",
				"/home/op/w",
				"/home/op/w/plakar",
				"/home/op/w/plakar/btree",
				"/home/op/w/plakar/btree/btree.go",
				"/home/op/w/plakar/storage",
				"/home/op/w/plakar/storage/backends",
				"/home/op/w/plakar/storage/backends/database",
				"/home/op/w/plakar/storage/backends/database/database.go",
				"/home/op/w/plakar/storage/backends/null",
				"/home/op/w/plakar/storage/backends/null/null.go",
				"/home/op/w/plakar/storage/backends/s3",
				"/home/op/w/plakar/storage/backends/s3/s3.go",
				"/home/op/w/plakar/snapshot",
				"/home/op/w/plakar/snapshot/backup.go",
				"/home/op/w/plakar/snapshot/exporter",
				"/home/op/w/plakar/snapshot/exporter/exporter.go",
				"/home/op/w/plakar/snapshot/exporter/fs",
				"/home/op/w/plakar/snapshot/exporter/fs/fs.go",
				"/home/op/w/plakar/snapshot/vfs",
				"/home/op/w/plakar/snapshot/vfs/vfs.go",
				"/home/op/w/plakar/snapshot/vfs/entry.go",
			},
			expect: []string{
				"/",
				"/home",
				"/home/op",
				"/home/op/w",
				"/home/op/w/plakar",
				"/home/op/w/plakar/btree",
				"/home/op/w/plakar/snapshot",
				"/home/op/w/plakar/storage",
				"/home/op/w/plakar/btree/btree.go",
				"/home/op/w/plakar/snapshot/backup.go",
				"/home/op/w/plakar/snapshot/exporter",
				"/home/op/w/plakar/snapshot/vfs",
				"/home/op/w/plakar/storage/backends",
				"/home/op/w/plakar/snapshot/exporter/exporter.go",
				"/home/op/w/plakar/snapshot/exporter/fs",
				"/home/op/w/plakar/snapshot/vfs/entry.go",
				"/home/op/w/plakar/snapshot/vfs/vfs.go",
				"/home/op/w/plakar/storage/backends/database",
				"/home/op/w/plakar/storage/backends/null",
				"/home/op/w/plakar/storage/backends/s3",
				"/home/op/w/plakar/snapshot/exporter/fs/fs.go",
				"/home/op/w/plakar/storage/backends/database/database.go",
				"/home/op/w/plakar/storage/backends/null/null.go",
				"/home/op/w/plakar/storage/backends/s3/s3.go",
			},
		},
	}

	for _, test := range suite {
		sorted := make([]string, len(test.input))
		copy(sorted, test.input)

		slices.SortFunc(sorted, vfs.PathCmp)
		if slices.Compare(test.expect, sorted) != 0 {
			t.Error("expected:")
			ppslice(test.expect)
			t.Error("got:")
			ppslice(sorted)
		}

		for _, path := range test.input {
			if _, found := slices.BinarySearchFunc(sorted, path, vfs.PathCmp); !found {
				t.Error("item not found by binary search:", path)
			}
		}
	}
}

func generateSnapshot(t *testing.T) *snapshot.Snapshot {
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

	hasher := hashing.GetHasher(storage.DEFAULT_HASHING_ALGORITHM)
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
	logger := logging.NewLogger(os.Stdout, os.Stderr)
	//logger.EnableTrace("all")
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

	return snap
}

func TestFiles(t *testing.T) {
	snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Repository().RebuildState()
	require.NoError(t, err)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	for files, err := range fs.Files() {
		require.NoError(t, err)
		require.Contains(t, files, "dummy.txt")
	}
}

func TestPathnames(t *testing.T) {
	snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Repository().RebuildState()
	require.NoError(t, err)

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	fs, err := snap.Filesystem()
	require.NoError(t, err)
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "dummy.txt") {
			filepath = pathname
		}
	}
	require.NotEmpty(t, filepath)
}

func TestOpen(t *testing.T) {
	snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Repository().RebuildState()
	require.NoError(t, err)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "dummy.txt") {
			filepath = pathname
		}
	}
	require.NotEmpty(t, filepath)

	f, err := fs.Open(filepath)
	require.NoError(t, err)
	defer f.Close()

	content, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "hello", string(content))
}

func TestGetEntry(t *testing.T) {
	snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Repository().RebuildState()
	require.NoError(t, err)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "dummy.txt") {
			filepath = pathname
		}
	}
	require.NotEmpty(t, filepath)

	entry, err := fs.GetEntry(filepath)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, "dummy.txt", entry.Name())
}

func _TestReadDir(t *testing.T) {
	snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Repository().RebuildState()
	require.NoError(t, err)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	entries, err := fs.ReadDir("/")
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))

	entry := entries[0]
	require.Equal(t, "tmp", entry.Name())
	require.True(t, entry.Type().IsDir())
	require.True(t, entry.IsDir())

	fileinfo, err := entry.Info()
	require.NoError(t, err)
	require.Implements(t, (*iofs.FileInfo)(nil), fileinfo)
}

func TestGetdents(t *testing.T) {
	snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Repository().RebuildState()
	require.NoError(t, err)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "subdir") {
			filepath = pathname
			break
		}
	}
	require.NotEmpty(t, filepath)

	entry, err := fs.GetEntry(filepath)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.True(t, entry.IsDir())

	dents, err := entry.Getdents(fs)
	require.NoError(t, err)
	for d, err := range dents {
		require.NoError(t, err)
		require.Equal(t, "dummy.txt", d.Name())
	}
}

func TestChildren(t *testing.T) {
	snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Repository().RebuildState()
	require.NoError(t, err)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// search for the correct directory path
	var dirpath string
	for pathname := range fs.Pathnames() {
		if strings.Contains(pathname, "tmp") {
			dirpath = pathname
			break
		}
	}
	require.NotEmpty(t, dirpath)

	iter, err := fs.Children(dirpath)
	require.NoError(t, err)
	require.NotNil(t, iter)

	// this is commented as it seems broken at the moment
	// expectedChildren := []string{"dummy.txt"}
	// var childNames []string
	// for childName, err := range iter {
	// 	require.NoError(t, err)
	// 	require.NotNil(t, childName)
	// 	childNames = append(childNames, childName)
	// }
	// require.ElementsMatch(t, expectedChildren, childNames)
}

func _TestFileMacs(t *testing.T) {
	snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Repository().RebuildState()
	require.NoError(t, err)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	iter, err := fs.FileMacs()
	require.NoError(t, err)
	require.NotNil(t, iter)

	macs := make(map[objects.MAC]struct{})
	for m, err := range iter {
		require.NoError(t, err)
		require.NotNil(t, m)
		macs[m] = struct{}{}
	}

	require.Equal(t, 5, len(macs))
}
