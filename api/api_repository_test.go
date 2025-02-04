package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func init() {
	os.Setenv("TZ", "UTC")
}

func Test_RepositoryStateErrors(t *testing.T) {
	testCases := []struct {
		name     string
		params   string
		location string
		stateId  string
		expected string
		status   int
	}{
		{
			name:     "wrong state id format",
			location: "/test/location",
			stateId:  "abc",
			status:   http.StatusBadRequest,
		},
		{
			name:     "wrong state",
			location: "/test/location?behavior=brokenGetState",
			stateId:  "0100000000000000000000000000000000000000000000000000000000000000",
			status:   http.StatusInternalServerError,
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

			req, err := http.NewRequest("GET", fmt.Sprintf("/api/repository/state/%s", c.stateId), nil)
			require.NoError(t, err, "creating request")

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			require.Equal(t, c.status, w.Code, fmt.Sprintf("expected status code %d", c.status))
		})
	}
}

func Test_RepositoryPackfile(t *testing.T) {

	testCases := []struct {
		name       string
		config     *storage.Configuration
		location   string
		packfileId string
		expected   string
	}{
		{
			name:       "default packfile",
			location:   "/test/location",
			config:     ptesting.NewConfiguration(ptesting.WithConfigurationCompression(nil)),
			packfileId: "0400000000000000000000000000000000000000000000000000000000000000",
			expected:   `{"test": "data"}`,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			lstore, err := storage.Create(c.location, *c.config)
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

			req, err := http.NewRequest("GET", fmt.Sprintf("/api/repository/packfile/%s", c.packfileId), nil)
			require.NoError(t, err, "creating request")

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code, fmt.Sprintf("expected status code %d", http.StatusOK))

			response := w.Result()
			defer func(Body io.ReadCloser) {
				err := Body.Close()
				require.NoError(t, err, "closing body")
			}(response.Body)

			rawBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)

			require.JSONEq(t, c.expected, string(rawBody))
		})
	}
}

func Test_RepositoryPackfileErrors(t *testing.T) {
	testCases := []struct {
		name       string
		params     string
		location   string
		packfileId string
		expected   string
		status     int
	}{
		{
			name:       "wrong packfile id format",
			location:   "/test/location",
			packfileId: "abc",
			status:     http.StatusBadRequest,
		},
		{
			name:       "wrong offset",
			location:   "/test/location",
			params:     url.Values{"offset": []string{"abc"}}.Encode(),
			packfileId: "0100000000000000000000000000000000000000000000000000000000000000",
			status:     http.StatusInternalServerError,
		},
		{
			name:       "wrong length",
			location:   "/test/location",
			params:     url.Values{"length": []string{"abc"}}.Encode(),
			packfileId: "0100000000000000000000000000000000000000000000000000000000000000",
			status:     http.StatusInternalServerError,
		},
		{
			name:       "length but no offset",
			location:   "/test/location",
			params:     url.Values{"length": []string{"1"}}.Encode(),
			packfileId: "0100000000000000000000000000000000000000000000000000000000000000",
			status:     http.StatusBadRequest,
		},
		{
			name:       "wrong packfile",
			location:   "/test/location?behavior=brokenGetPackfile",
			packfileId: "0100000000000000000000000000000000000000000000000000000000000000",
			status:     http.StatusInternalServerError,
		},
		{
			name:       "length and offset but error",
			location:   "/test/location?behavior=brokenGetPackfileBlob",
			params:     url.Values{"length": []string{"1"}, "offset": []string{"1"}}.Encode(),
			packfileId: "0100000000000000000000000000000000000000000000000000000000000000",
			status:     http.StatusInternalServerError,
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

			req, err := http.NewRequest("GET", fmt.Sprintf("/api/repository/packfile/%s?%s", c.packfileId, c.params), nil)

			require.NoError(t, err, "creating request")

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			require.Equal(t, c.status, w.Code, fmt.Sprintf("expected status code %d", c.status))
		})
	}
}
