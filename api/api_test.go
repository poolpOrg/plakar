package api

import (
	"bytes"
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

func TestNewRouter(t *testing.T) {
	repo := &repository.Repository{}
	token := "test-token"
	mux := http.NewServeMux()
	// Make sure SetupRoutes doesn't panic, which happens when invalid routes
	// are registered
	SetupRoutes(mux, repo, token)
}

func TestAuthMiddleware(t *testing.T) {
	config := ptesting.NewConfiguration()

	serializedConfig, err := config.ToBytes()
	require.NoError(t, err)

	hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)
	wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serializedConfig))
	require.NoError(t, err)

	wrappedConfig, err := io.ReadAll(wrappedConfigRd)
	require.NoError(t, err)

	lstore, err := storage.Create(map[string]string{"location": "/test/location"}, wrappedConfig)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	ctx := appcontext.NewAppContext()
	cache := caching.NewManager("/tmp/test_plakar")
	defer cache.Close()
	ctx.SetCache(cache)
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	repo, err := repository.New(ctx, lstore, wrappedConfig)
	if err != nil {
		t.Fatal(err)
	}
	token := "test-token"
	mux := http.NewServeMux()
	SetupRoutes(mux, repo, token)

	req, err := http.NewRequest("GET", "/api/storage/configuration", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Invalid Token")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status code 401, got %d", w.Code)
	}

	req.Header.Set("Authorization", "")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req)
	if w2.Code != http.StatusUnauthorized {
		t.Errorf("Expected status code 401, got %d", w2.Code)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, req)

	if w3.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", w3.Code)
	}
}

func Test_UnknownEndpoint(t *testing.T) {
	config := ptesting.NewConfiguration()

	serializedConfig, err := config.ToBytes()
	require.NoError(t, err)

	hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)
	wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serializedConfig))
	require.NoError(t, err)

	wrappedConfig, err := io.ReadAll(wrappedConfigRd)
	require.NoError(t, err)

	lstore, err := storage.Create(map[string]string{"location": "/test/location"}, wrappedConfig)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	ctx := appcontext.NewAppContext()
	cache := caching.NewManager("/tmp/test_plakar")
	defer cache.Close()
	ctx.SetCache(cache)
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	repo, err := repository.New(ctx, lstore, wrappedConfig)
	if err != nil {
		t.Fatal(err)
	}
	token := ""
	mux := http.NewServeMux()
	SetupRoutes(mux, repo, token)

	req, err := http.NewRequest("GET", "/api/unknown_endpoint", nil)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status code 200, got %d", w.Code)
	}
}
