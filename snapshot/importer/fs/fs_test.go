package fs

import (
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFSImporter(t *testing.T) {
	tmpImportDir, err := os.MkdirTemp("/tmp", "tmp_import*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpImportDir)
	})

	err = os.WriteFile(tmpImportDir+"/dummy.txt", []byte("test importer fs"), 0644)
	require.NoError(t, err)

	importer, err := NewFSImporter(map[string]string{"location": tmpImportDir})
	require.NoError(t, err)
	require.NotNil(t, importer)

	origin := importer.Origin()
	require.NotEmpty(t, origin)

	root := importer.Root()
	require.NoError(t, err)
	require.Equal(t, tmpImportDir, root)

	typ := importer.Type()
	require.Equal(t, "fs", typ)

	scanChan, err := importer.Scan()
	require.NoError(t, err)
	require.NotNil(t, scanChan)

	paths := []string{}
	for record := range scanChan {
		require.Nil(t, record.Error)
		if record.Record.IsXattr {
			continue
		}
		paths = append(paths, record.Record.Pathname)
	}
	expected := []string{"/", "/tmp", tmpImportDir, tmpImportDir + "/dummy.txt"}
	sort.Strings(paths)
	require.Equal(t, expected, paths)

	// cannot test this that as filesystem does not necessarly have the xattr enabled
	// err = xattr.Set(tmpImportDir+"/dummy.txt", "user.plakar.test", []byte("random.value"))
	// require.NoError(t, err)
	// extendedAttrReader, err := importer.NewExtendedAttributeReader(tmpImportDir+"/dummy.txt", "user.plakar.test")
	// require.NoError(t, err)
	// require.NotNil(t, extendedAttrReader)
	// defer extendedAttrReader.Close()

	reader, err := importer.NewReader(tmpImportDir + "/dummy.txt")
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	attributes, err := importer.GetExtendedAttributes(tmpImportDir + "/dummy.txt")
	require.NoError(t, err)
	require.NotNil(t, attributes)

	err = importer.Close()
	require.NoError(t, err)
}
