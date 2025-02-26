package s3

import (
	"net/http/httptest"
	"os"
	"sort"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/require"
)

func TestS3Importer(t *testing.T) {
	tmpImportDir, err := os.MkdirTemp("/tmp", "tmp_import*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpImportDir)
	})

	err = os.WriteFile(tmpImportDir+"/dummy.txt", []byte("test importer fs"), 0644)
	require.NoError(t, err)

	fpOrigin, err := os.Open(tmpImportDir + "/dummy.txt")
	require.NoError(t, err)
	defer fpOrigin.Close()

	// Start the fake S3 server
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	tmpImportBucket := "s3://" + ts.Listener.Addr().String() + "/bucket"

	backend.CreateBucket("bucket")
	_, err = backend.PutObject("bucket", "dummy.txt", nil, fpOrigin, 16)
	require.NoError(t, err)

	importer, err := NewS3Importer(map[string]string{"location": tmpImportBucket, "access_key": "", "secret_access_key": ""})
	require.NoError(t, err)
	require.NotNil(t, importer)

	origin := importer.Origin()
	require.NotEmpty(t, origin)

	root := importer.Root()
	require.NoError(t, err)
	require.Equal(t, "/", root)

	typ := importer.Type()
	require.Equal(t, "s3", typ)

	scanChan, err := importer.Scan()
	require.NoError(t, err)
	require.NotNil(t, scanChan)

	paths := []string{}
	for record := range scanChan {
		require.Nil(t, record.Error)
		paths = append(paths, record.Record.Pathname)
	}
	expected := []string{"/", "/dummy.txt"}
	sort.Strings(paths)
	require.Equal(t, expected, paths)

	reader, err := importer.NewReader("bucket/dummy.txt")
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	_, err = importer.NewExtendedAttributeReader("/bucket/dummy.txt", "user.plakar.test")
	require.EqualError(t, err, "extended attributes are not supported on S3")

	_, err = importer.GetExtendedAttributes("/bucket/dummy.txt")
	require.EqualError(t, err, "extended attributes are not supported on S3")

	err = importer.Close()
	require.NoError(t, err)
}
