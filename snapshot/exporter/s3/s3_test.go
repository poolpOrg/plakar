package s3

import (
	"net/http/httptest"
	"os"
	"testing"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/exporter"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/require"
)

func TestExporter(t *testing.T) {
	tmpOriginDir, err := os.MkdirTemp("/tmp", "tmp_origin*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpOriginDir)
	})

	// Start the fake S3 server
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	tmpExportBucket := "s3://" + ts.Listener.Addr().String() + "/bucket"

	var exporterInstance exporter.Exporter
	exporterInstance, err = exporter.NewExporter(map[string]string{"location": tmpExportBucket, "access_key": "", "secret_access_key": "", "use_ssl": "false"})
	require.NoError(t, err)
	defer exporterInstance.Close()

	require.Equal(t, "/bucket", exporterInstance.Root())

	// create a temporary file to backup later
	err = os.WriteFile(tmpOriginDir+"/dummy.txt", []byte("test exporter s3"), 0644)
	require.NoError(t, err)

	fpOrigin, err := os.Open(tmpOriginDir + "/dummy.txt")
	require.NoError(t, err)
	defer fpOrigin.Close()

	err = exporterInstance.StoreFile("dummy.txt", fpOrigin)
	require.NoError(t, err)

	err = exporterInstance.CreateDirectory("/bucket/subdir")
	require.NoError(t, err)

	err = exporterInstance.SetPermissions("bucket/subdir", &objects.FileInfo{Lmode: 0644})
	require.NoError(t, err)
}
