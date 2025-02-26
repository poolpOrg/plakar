package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

// XXX: re-add once we move to non-mocked state object.
func _TestSnapshotHeader(t *testing.T) {
	testCases := []struct {
		name       string
		params     string
		location   string
		snapshotId string
		expected   string
		status     int
	}{
		{
			name:       "snapshot id valid",
			location:   "/test/location?behavior=oneState",
			snapshotId: "0100000000000000000000000000000000000000000000000000000000000000",
			status:     http.StatusOK,
			expected: `{
			"item": {
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
				"root": "0100000000000000000000000000000000000000000000000000000000000000",
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
			}`,
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

			req, err := http.NewRequest("GET", fmt.Sprintf("/api/snapshot/%s", c.snapshotId), nil)
			require.NoError(t, err, "creating request")

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			require.Equal(t, c.status, w.Code, fmt.Sprintf("expected status code %d", c.status))

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

			req, err := http.NewRequest("GET", fmt.Sprintf("/api/snapshot/%s", c.snapshotId), nil)
			require.NoError(t, err, "creating request")

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			require.Equal(t, c.status, w.Code, fmt.Sprintf("expected status code %d", c.status))
		})
	}
}

// XXX: re-add once we move to non-mocked state object.
func _TestSnapshotSign(t *testing.T) {
	testCases := []struct {
		name         string
		params       string
		location     string
		snapshotPath string
		expected     string
		status       int
	}{
		{
			name:         "working",
			location:     "/test/location?behavior=oneState",
			snapshotPath: "0100000000000000000000000000000000000000000000000000000000000000:/dummy",
			status:       http.StatusOK,
			expected:     `{}`,
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

			token := "test-token"
			mux := http.NewServeMux()
			SetupRoutes(mux, repo, token)

			// retrieve a valid jwt token before calling the read
			req, err := http.NewRequest("POST", fmt.Sprintf("/api/snapshot/reader-sign-url/%s", c.snapshotPath), nil)
			req.SetPathValue("snapshot_path", c.snapshotPath)
			require.NoError(t, err, "creating request")

			w := httptest.NewRecorder()
			urlSigner := NewSnapshotReaderURLSigner(token)
			urlSigner.Sign(w, req)

			response := w.Result()
			defer func(Body io.ReadCloser) {
				err := Body.Close()
				require.NoError(t, err, "closing body")
			}(response.Body)

			rawBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)

			type SignatureResponse struct {
				Item struct {
					Signature string `json:"signature"`
				} `json:"item"`
			}

			var resp SignatureResponse
			err = json.Unmarshal(rawBody, &resp)
			require.NoError(t, err, "unmarshaling jwt signature")
			signature := resp.Item.Signature

			require.Equal(t, 283, len(signature), "signature should be 32 bytes")
		})
	}
}
