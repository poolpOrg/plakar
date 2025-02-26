package snapshot

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/plakar/snapshot/exporter"
	_ "github.com/PlakarKorp/plakar/snapshot/exporter/fs"
	"github.com/stretchr/testify/require"
)

func TestRestore(t *testing.T) {
	snap := generateSnapshot(t, nil)
	defer snap.Close()

	err := snap.repository.RebuildState()
	require.NoError(t, err)

	tmpRestoreDir, err := os.MkdirTemp("", "tmp_to_restore")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpRestoreDir)
	})
	var exporterInstance exporter.Exporter
	exporterInstance, err = exporter.NewExporter(map[string]string{"location": tmpRestoreDir})
	require.NoError(t, err)
	defer exporterInstance.Close()

	opts := &RestoreOptions{
		MaxConcurrency: 1,
		Strip:          snap.Header.GetSource(0).Importer.Directory,
	}

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

	err = snap.Restore(exporterInstance, exporterInstance.Root(), filepath, opts)
	require.NoError(t, err)

	files, err := os.ReadDir(exporterInstance.Root())
	fmt.Println(files)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	contents, err := os.ReadFile(fmt.Sprintf("%s/dummy.txt", exporterInstance.Root()))
	require.NoError(t, err)
	require.Equal(t, "hello", string(contents))
}
