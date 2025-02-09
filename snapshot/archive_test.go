package snapshot

import (
	"bytes"
	"strings"
	"testing"

	_ "github.com/PlakarKorp/plakar/snapshot/exporter/fs"
	"github.com/stretchr/testify/require"
)

func TestArchive(t *testing.T) {
	snap := generateSnapshot(t, nil)
	defer snap.Close()

	err := snap.repository.RebuildState()
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

	for _, format := range []ArchiveFormat{ArchiveTar, ArchiveTarball, ArchiveZip} {
		bufOut := bytes.NewBuffer(nil)
		err = snap.Archive(bufOut, format, []string{filepath}, true)
		require.NoError(t, err)
	}
}
