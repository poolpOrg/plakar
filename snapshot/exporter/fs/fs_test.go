package fs

import (
	"io"
	"os"
	"testing"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/exporter"
	"github.com/stretchr/testify/require"
)

func TestExporter(t *testing.T) {
	tmpExportDir, err := os.MkdirTemp("/tmp", "tmp_export*")
	require.NoError(t, err)
	tmpOriginDir, err := os.MkdirTemp("/tmp", "tmp_origin*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpExportDir)
		os.RemoveAll(tmpOriginDir)
	})

	var exporterInstance exporter.Exporter
	exporterInstance, err = exporter.NewExporter(map[string]string{"location": tmpExportDir})
	require.NoError(t, err)
	defer exporterInstance.Close()

	require.Equal(t, tmpExportDir, exporterInstance.Root())

	err = os.WriteFile(tmpOriginDir+"/dummy.txt", []byte("test exporter fs"), 0644)
	require.NoError(t, err)

	fpOrigin, err := os.Open(tmpOriginDir + "/dummy.txt")
	require.NoError(t, err)
	defer fpOrigin.Close()

	err = exporterInstance.StoreFile(tmpExportDir+"/dummy.txt", fpOrigin)
	require.NoError(t, err)

	fpExported, err := os.Open(tmpExportDir + "/dummy.txt")
	require.NoError(t, err)
	defer fpExported.Close()

	newContent, err := io.ReadAll(fpExported)
	require.NoError(t, err)

	require.Equal(t, "test exporter fs", string(newContent))

	err = exporterInstance.CreateDirectory(tmpExportDir + "/subdir")
	require.NoError(t, err)

	err = exporterInstance.SetPermissions(tmpExportDir+"/dummy.txt", &objects.FileInfo{Lmode: 0644})
	require.NoError(t, err)
}
