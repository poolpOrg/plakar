package null

import (
	"bytes"
	"io"
	"testing"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/stretchr/testify/require"
)

func TestNullBackend(t *testing.T) {
	// create a repository
	repo := NewRepository("/test/location")
	if repo == nil {
		t.Fatal("error creating repository")
	}

	location := repo.Location()
	require.Equal(t, "/test/location", location)

	err := repo.Create(location, storage.Configuration{})
	require.NoError(t, err)

	err = repo.Open(location)
	require.NoError(t, err)
	// only test one field
	require.Equal(t, repo.Configuration().Version, storage.VERSION)

	err = repo.Close()
	require.NoError(t, err)

	// snapshots
	r, ok := repo.(*Repository)
	require.True(t, ok)
	snaps, err := r.GetSnapshots()
	require.NoError(t, err)
	require.Equal(t, snaps, []objects.Checksum{})

	checksum := objects.Checksum{0x10}
	err = r.PutSnapshot(checksum, []byte("test"))
	require.NoError(t, err)

	retrievedSnapshot, err := r.GetSnapshot(checksum)
	require.NoError(t, err)
	require.Equal(t, []byte(""), retrievedSnapshot)

	err = r.DeleteSnapshot(checksum)
	require.NoError(t, err)

	// states
	checksums, err := repo.GetStates()
	require.NoError(t, err)
	require.Equal(t, checksums, []objects.Checksum{})

	err = repo.PutState(checksum, bytes.NewReader([]byte("test")))
	require.NoError(t, err)

	rd, err := repo.GetState(checksum)
	require.NoError(t, err)
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, rd)
	require.NoError(t, err)
	require.Equal(t, "", buf.String())

	err = repo.DeleteState(checksum)
	require.NoError(t, err)

	// packfiles
	checksums, err = repo.GetPackfiles()
	require.NoError(t, err)
	require.Equal(t, checksums, []objects.Checksum{})

	err = repo.PutPackfile(checksum, bytes.NewReader([]byte("test")))
	require.NoError(t, err)

	rd, err = repo.GetPackfile(checksum)
	buf = new(bytes.Buffer)
	_, err = io.Copy(buf, rd)
	require.NoError(t, err)
	require.Equal(t, "", buf.String())

	rd, err = repo.GetPackfileBlob(checksum, 0, 0)
	buf = new(bytes.Buffer)
	_, err = io.Copy(buf, rd)
	require.NoError(t, err)
	require.Equal(t, "", buf.String())

	err = repo.DeletePackfile(checksum)
	require.NoError(t, err)
}
