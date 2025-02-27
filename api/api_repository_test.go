package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/storage"
	ptesting "github.com/PlakarKorp/plakar/testing"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("TZ", "UTC")
}

// XXX: re-add once we move to non-mocked state object.
func _Test_RepositoryConfiguration(t *testing.T) {

	config := ptesting.NewConfiguration()
	serializedConfig, err := config.ToBytes()
	require.NoError(t, err)

	hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)
	wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serializedConfig))
	require.NoError(t, err)
	wrappedConfig, err := io.ReadAll(wrappedConfigRd)
	require.NoError(t, err)
	lstore, err := storage.Create(map[string]string{"location": "/test/location"}, wrappedConfig)
	require.NoError(t, err, "creating storage")

	ctx := appcontext.NewAppContext()
	cache := caching.NewManager("/tmp/test_plakar")
	defer cache.Close()
	ctx.SetCache(cache)
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	repo, err := repository.New(ctx, lstore, wrappedConfig)
	require.NoError(t, err, "creating repository")

	var noToken string
	mux := http.NewServeMux()
	SetupRoutes(mux, repo, noToken)

	req, err := http.NewRequest("GET", "/api/repository/configuration", nil)
	require.NoError(t, err, "creating request")

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "expected status code 200")

	response := w.Result()
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		require.NoError(t, err, "closing body")
	}(response.Body)

	rawBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)

	expected := `{
		"Version": "0.6.0",
		"Timestamp": "2025-01-01T00:00:00Z",
		"RepositoryID": "00ff0000-0000-4000-a000-000000000001",
		"Packfile": { "MaxSize": 20971520 },
		"Chunking": {
			"Algorithm": "FASTCDC",
			"MinSize": 65536,
			"NormalSize": 1048576,
			"MaxSize": 4194304
		},
		"Hashing": { "Algorithm": "SHA256", "Bits": 256 },
		"Compression": {
			"Algorithm": "LZ4",
			"Level": 131072,
			"WindowSize": -1,
			"ChunkSize": -1,
			"BlockSize": -1,
			"EnableCRC": false
		},
		"Encryption": { "Algorithm": "AES256-GCM", "Key": "" }
	}`
	require.JSONEq(t, expected, string(rawBody))
}

// XXX: re-add once we move to non-mocked state object.
func _Test_RepositorySnapshots(t *testing.T) {
	testCases := []struct {
		name     string
		config   *storage.Configuration
		location string
		expected string
	}{
		{
			name:     "no snapshots",
			location: "/test/location",
			config:   ptesting.NewConfiguration(),
			expected: `{"items": [], "total": 0}`,
		},
		{
			name:     "one snapshot with compression",
			location: "/test/location?behavior=oneSnapshot",
			config:   ptesting.NewConfiguration(),
			expected: `{
						"total": 1,
						"items": [
							{
							"identifier": "0100000000000000000000000000000000000000000000000000000000000000",
							"version": "",
							"timestamp": "2025-01-02T00:00:00Z",
							"duration": 0,
							"identity": {
								"identifier": "00000000-0000-0000-0000-000000000000",
								"public_key": null
							},
							"name": "",
							"category": "",
							"environment": "",
							"perimeter": "",
							"classifications": null,
							"tags": null,
							"context": null,
							"importer": { "type": "", "origin": "", "directory": "" },
							"root": "0000000000000000000000000000000000000000000000000000000000000000",
							"errors": "0000000000000000000000000000000000000000000000000000000000000000",
							"index": "0000000000000000000000000000000000000000000000000000000000000000",
							"metadata": "0000000000000000000000000000000000000000000000000000000000000000",
							"statistics": "0000000000000000000000000000000000000000000000000000000000000000",
							"summary": {
								"directory": {
								"directories": 0,
								"files": 0,
								"symlinks": 0,
								"devices": 0,
								"pipes": 0,
								"sockets": 0,
								"children": 0,
								"setuid": 0,
								"setgid": 0,
								"sticky": 0,
								"objects": 0,
								"chunks": 0,
								"min_size": 0,
								"max_size": 0,
								"avg_size": 0,
								"size": 0,
								"min_mod_time": 0,
								"max_mod_time": 0,
								"min_entropy": 0,
								"max_entropy": 0,
								"sum_entropy": 0,
								"avg_entropy": 0,
								"hi_entropy": 0,
								"lo_entropy": 0,
								"MIME_audio": 0,
								"MIME_video": 0,
								"MIME_image": 0,
								"MIME_text": 0,
								"MIME_application": 0,
								"MIME_other": 0,
								"errors": 0
								},
								"below": {
								"directories": 0,
								"files": 0,
								"symlinks": 0,
								"devices": 0,
								"pipes": 0,
								"sockets": 0,
								"children": 0,
								"setuid": 0,
								"setgid": 0,
								"sticky": 0,
								"objects": 0,
								"chunks": 0,
								"min_size": 0,
								"max_size": 0,
								"size": 0,
								"min_mod_time": 0,
								"max_mod_time": 0,
								"min_entropy": 0,
								"max_entropy": 0,
								"hi_entropy": 0,
								"lo_entropy": 0,
								"MIME_audio": 0,
								"MIME_video": 0,
								"MIME_image": 0,
								"MIME_text": 0,
								"MIME_application": 0,
								"MIME_other": 0,
								"errors": 0
								}
							}
							}
						]}`,
		},
		{
			name:     "one snapshot without compression",
			location: "/test/location?behavior=oneSnapshot",
			config:   ptesting.NewConfiguration(ptesting.WithConfigurationCompression(nil)),
			expected: `{
						"total": 1,
						"items": [
							{
							"identifier": "0100000000000000000000000000000000000000000000000000000000000000",
							"version": "",
							"timestamp": "2025-01-02T00:00:00Z",
							"duration": 0,
							"identity": {
								"identifier": "00000000-0000-0000-0000-000000000000",
								"public_key": null
							},
							"name": "",
							"category": "",
							"environment": "",
							"perimeter": "",
							"classifications": null,
							"tags": null,
							"context": null,
							"importer": { "type": "", "origin": "", "directory": "" },
							"root": "0000000000000000000000000000000000000000000000000000000000000000",
							"errors": "0000000000000000000000000000000000000000000000000000000000000000",
							"index": "0000000000000000000000000000000000000000000000000000000000000000",
							"metadata": "0000000000000000000000000000000000000000000000000000000000000000",
							"statistics": "0000000000000000000000000000000000000000000000000000000000000000",
							"summary": {
								"directory": {
								"directories": 0,
								"files": 0,
								"symlinks": 0,
								"devices": 0,
								"pipes": 0,
								"sockets": 0,
								"children": 0,
								"setuid": 0,
								"setgid": 0,
								"sticky": 0,
								"objects": 0,
								"chunks": 0,
								"min_size": 0,
								"max_size": 0,
								"avg_size": 0,
								"size": 0,
								"min_mod_time": 0,
								"max_mod_time": 0,
								"min_entropy": 0,
								"max_entropy": 0,
								"sum_entropy": 0,
								"avg_entropy": 0,
								"hi_entropy": 0,
								"lo_entropy": 0,
								"MIME_audio": 0,
								"MIME_video": 0,
								"MIME_image": 0,
								"MIME_text": 0,
								"MIME_application": 0,
								"MIME_other": 0,
								"errors": 0
								},
								"below": {
								"directories": 0,
								"files": 0,
								"symlinks": 0,
								"devices": 0,
								"pipes": 0,
								"sockets": 0,
								"children": 0,
								"setuid": 0,
								"setgid": 0,
								"sticky": 0,
								"objects": 0,
								"chunks": 0,
								"min_size": 0,
								"max_size": 0,
								"size": 0,
								"min_mod_time": 0,
								"max_mod_time": 0,
								"min_entropy": 0,
								"max_entropy": 0,
								"hi_entropy": 0,
								"lo_entropy": 0,
								"MIME_audio": 0,
								"MIME_video": 0,
								"MIME_image": 0,
								"MIME_text": 0,
								"MIME_application": 0,
								"MIME_other": 0,
								"errors": 0
								}
							}
							}
						]}`,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {

			serializedConfig, err := c.config.ToBytes()
			require.NoError(t, err)

			hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)
			wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serializedConfig))
			require.NoError(t, err)

			wrappedConfig, err := io.ReadAll(wrappedConfigRd)
			require.NoError(t, err)

			lstore, err := storage.Create(map[string]string{"location": c.location}, wrappedConfig)
			require.NoError(t, err, "creating storage")

			ctx := appcontext.NewAppContext()
			cache := caching.NewManager("/tmp/test_plakar")
			defer cache.Close()
			ctx.SetCache(cache)
			ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
			repo, err := repository.New(ctx, lstore, wrappedConfig)
			require.NoError(t, err, "creating repository")

			var noToken string
			mux := http.NewServeMux()
			SetupRoutes(mux, repo, noToken)

			req, err := http.NewRequest("GET", "/api/repository/snapshots", nil)
			require.NoError(t, err, "creating request")

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code, "expected status code 200")

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

// XXX: re-add once we move to non-mocked state object.
func _Test_RepositorySnapshotsErrors(t *testing.T) {
	testCases := []struct {
		name     string
		params   string
		location string
		expected string
		status   int
	}{
		{
			name:     "wrong offset",
			location: "/test/location",
			params:   url.Values{"offset": []string{"abc"}}.Encode(),
			status:   http.StatusInternalServerError,
		},
		{
			name:     "offset too big",
			location: "/test/location",
			params:   url.Values{"offset": []string{"5"}}.Encode(),
			status:   http.StatusOK,
			expected: `{"items": [], "total": 0}`,
		},
		{
			name:     "offset + limit too big",
			location: "/test/location?behavior=oneSnapshot",
			params:   url.Values{"offset": []string{"1"}, "limit": []string{"1"}}.Encode(),
			status:   http.StatusOK,
			expected: `{"items": [], "total": 1}`,
		},
		{
			name:     "wrong packfile",
			location: "/test/location?behavior=nopackfile",
			status:   http.StatusInternalServerError,
		},
		{
			name:     "wrong limit",
			location: "/test/location",
			params:   url.Values{"limit": []string{"abc"}}.Encode(),
			status:   http.StatusInternalServerError,
		},
		{
			name:     "wrong sort",
			location: "/test/location",
			params:   url.Values{"sort": []string{"abc"}}.Encode(),
			expected: `{"error":{"code":"invalid_params","message":"Invalid parameter","params":{"sort":{"code":"invalid_argument","message":"invalid sort key: abc"}}}}`,
			status:   http.StatusBadRequest,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			config := ptesting.NewConfiguration()

			serializedConfig, err := config.ToBytes()
			require.NoError(t, err)

			hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)
			wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serializedConfig))
			require.NoError(t, err)

			wrappedConfig, err := io.ReadAll(wrappedConfigRd)
			require.NoError(t, err)

			lstore, err := storage.Create(map[string]string{"location": c.location}, wrappedConfig)
			require.NoError(t, err, "creating storage")

			ctx := appcontext.NewAppContext()
			cache := caching.NewManager("/tmp/test_plakar")
			defer cache.Close()
			ctx.SetCache(cache)
			ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
			repo, err := repository.New(ctx, lstore, wrappedConfig)
			require.NoError(t, err, "creating repository")

			var noToken string
			mux := http.NewServeMux()
			SetupRoutes(mux, repo, noToken)

			req, err := http.NewRequest("GET", fmt.Sprintf("/api/repository/snapshots?%s", c.params), nil)
			require.NoError(t, err, "creating request")

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			require.Equal(t, c.status, w.Code, fmt.Sprintf("expected status code %d", c.status))

			if c.expected != "" {
				response := w.Result()
				defer func(Body io.ReadCloser) {
					err := Body.Close()
					require.NoError(t, err, "closing body")
				}(response.Body)

				rawBody, err := io.ReadAll(response.Body)
				require.NoError(t, err)

				require.JSONEq(t, c.expected, string(rawBody))
			}
		})
	}
}

// XXX: re-add once we move to non-mocked state object.
func _Test_RepositoryStates(t *testing.T) {

	testCases := []struct {
		name     string
		config   *storage.Configuration
		location string
		expected string
	}{
		{
			name:     "no states",
			location: "/test/location",
			config:   ptesting.NewConfiguration(),
			expected: `{"items": [], "total": 0}`,
		},
		{
			name:     "with states",
			location: "/test/location?behavior=oneSnapshot",
			config:   ptesting.NewConfiguration(),
			expected: `{"total":3,"items":["0100000000000000000000000000000000000000000000000000000000000000","0200000000000000000000000000000000000000000000000000000000000000","0300000000000000000000000000000000000000000000000000000000000000"]}`,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {

			serializedConfig, err := c.config.ToBytes()
			require.NoError(t, err)

			hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)
			wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serializedConfig))
			require.NoError(t, err)

			wrappedConfig, err := io.ReadAll(wrappedConfigRd)
			require.NoError(t, err)

			lstore, err := storage.Create(map[string]string{"location": c.location}, wrappedConfig)
			require.NoError(t, err, "creating storage")

			ctx := appcontext.NewAppContext()
			cache := caching.NewManager("/tmp/test_plakar")
			defer cache.Close()
			ctx.SetCache(cache)
			ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
			repo, err := repository.New(ctx, lstore, wrappedConfig)
			require.NoError(t, err, "creating repository")

			var noToken string
			mux := http.NewServeMux()
			SetupRoutes(mux, repo, noToken)

			req, err := http.NewRequest("GET", "/api/repository/states", nil)
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

// XXX: re-add once we move to non-mocked state object.
func _Test_RepositoryState(t *testing.T) {

	testCases := []struct {
		name     string
		config   *storage.Configuration
		location string
		stateId  string
		expected string
	}{
		{
			name:     "default state",
			location: "/test/location",
			config:   ptesting.NewConfiguration(),
			stateId:  "0100000000000000000000000000000000000000000000000000000000000000",
			expected: `{"test": "data"}`,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {

			serializedConfig, err := c.config.ToBytes()
			require.NoError(t, err)

			hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)
			wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serializedConfig))
			require.NoError(t, err)

			wrappedConfig, err := io.ReadAll(wrappedConfigRd)
			require.NoError(t, err)

			lstore, err := storage.Create(map[string]string{"location": c.location}, wrappedConfig)
			require.NoError(t, err, "creating storage")

			ctx := appcontext.NewAppContext()
			cache := caching.NewManager("/tmp/test_plakar")
			defer cache.Close()
			ctx.SetCache(cache)
			ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
			repo, err := repository.New(ctx, lstore, wrappedConfig)
			require.NoError(t, err, "creating repository")

			var noToken string
			mux := http.NewServeMux()
			SetupRoutes(mux, repo, noToken)

			req, err := http.NewRequest("GET", fmt.Sprintf("/api/repository/state/%s", c.stateId), nil)
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

			serializedConfig, err := config.ToBytes()
			require.NoError(t, err)

			hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)
			wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serializedConfig))
			require.NoError(t, err)

			wrappedConfig, err := io.ReadAll(wrappedConfigRd)
			require.NoError(t, err)

			lstore, err := storage.Create(map[string]string{"location": c.location}, wrappedConfig)
			require.NoError(t, err, "creating storage")

			ctx := appcontext.NewAppContext()
			cache := caching.NewManager("/tmp/test_plakar")
			defer cache.Close()
			ctx.SetCache(cache)
			ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
			repo, err := repository.New(ctx, lstore, wrappedConfig)
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
