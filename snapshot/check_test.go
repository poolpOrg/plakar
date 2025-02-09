package snapshot

import (
	"strings"
	"testing"

	_ "github.com/PlakarKorp/plakar/snapshot/exporter/fs"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
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

	checked, err := snap.Check(filepath, &CheckOptions{
		MaxConcurrency: 1,
	})
	require.NoError(t, err)
	require.True(t, checked)
}
