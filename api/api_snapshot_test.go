package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	ptesting "github.com/PlakarKorp/plakar/testing"
	"github.com/stretchr/testify/require"
)

func TestSnapshotHeaderErrors(t *testing.T) {
	testCases := []struct {
		name       string
		params     string
		location   string
		snapshotId string
		expected   string
		status     int
	}{
		{
			name:       "wrong snapshot id format",
			location:   "/test/location",
			snapshotId: "abc",
			status:     http.StatusBadRequest,
		},
		{
			name:       "snapshot id valid but not found",
			location:   "/test/location",
			snapshotId: "7e0e6e24a6e29faf11d022dca77826fe8b8a000aff5ea27e16650d03acefc93c",
			status:     http.StatusNotFound,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			config := ptesting.NewConfiguration()
			lstore, err := storage.Create(c.location, *config)
			require.NoError(t, err, "creating storage")

			ctx := appcontext.NewAppContext()
			cache := caching.NewManager("/tmp/test_plakar")
			defer cache.Close()
			ctx.SetCache(cache)
			ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
			repo, err := repository.New(ctx, lstore, nil)
			require.NoError(t, err, "creating repository")

			var noToken string
			mux := http.NewServeMux()
			SetupRoutes(mux, repo, noToken)

			req, err := http.NewRequest("GET", fmt.Sprintf("/api/snapshot/%s", c.snapshotId), nil)
			require.NoError(t, err, "creating request")

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			require.Equal(t, c.status, w.Code, fmt.Sprintf("expected status code %d", c.status))
		})
	}
}
