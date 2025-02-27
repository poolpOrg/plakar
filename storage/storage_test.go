package storage_test

import (
	"os"
	"runtime"
	"testing"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/storage"
	ptesting "github.com/PlakarKorp/plakar/testing"
)

func TestNewStore(t *testing.T) {
	ctx := appcontext.NewAppContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

	store, err := storage.NewStore("fs", map[string]string{"location": "/test/location"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if store.Location() != "/test/location" {
		t.Errorf("expected location to be '/test/location', got %v", store.Location())
	}

	// should return an error as the backend does not exist
	_, err = storage.NewStore("unknown", map[string]string{"location": "/test/location"})
	if err.Error() != "backend 'unknown' does not exist" {
		t.Fatalf("Expected %s but got %v", "backend 'unknown' does not exist", err)
	}
}

func TestCreateStore(t *testing.T) {
	ctx := appcontext.NewAppContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

	config := storage.NewConfiguration()
	serializedConfig, err := config.ToBytes()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	_, err = storage.Create(map[string]string{"location": "/test/location"}, serializedConfig)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// should return an error as the backend Create will return an error
	_, err = storage.Create(map[string]string{"location": "/test/location/musterror"}, serializedConfig)
	if err.Error() != "creating error" {
		t.Fatalf("Expected %s but got %v", "opening error", err)
	}

	// should return an error as the backend does not exist
	_, err = storage.Create(map[string]string{"location": "unknown://dummy"}, serializedConfig)
	if err.Error() != "unsupported plakar protocol" {
		t.Fatalf("Expected %s but got %v", "unsupported plakar protocol", err)
	}
}

func TestOpenStore(t *testing.T) {
	ctx := appcontext.NewAppContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

	store, _, err := storage.Open(map[string]string{"location": "/test/location"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if store.Location() != "/test/location" {
		t.Errorf("expected location to be '/test/location', got %v", store.Location())
	}

	// should return an error as the backend Open will return an error
	_, _, err = storage.Open(map[string]string{"location": "/test/location/musterror"})
	if err.Error() != "opening error" {
		t.Fatalf("Expected %s but got %v", "opening error", err)
	}

	// should return an error as the backend does not exist
	_, _, err = storage.Open(map[string]string{"location": "unknown://dummy"})
	if err.Error() != "unsupported plakar protocol" {
		t.Fatalf("Expected %s but got %v", "unsupported plakar protocol", err)
	}
}

func TestBackends(t *testing.T) {
	ctx := appcontext.NewAppContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

	storage.Register("test", func(storeConfig map[string]string) (storage.Store, error) { return &ptesting.MockBackend{}, nil })

	expected := []string{"fs", "test"}
	actual := storage.Backends()
	if len(expected) != len(actual) {
		t.Errorf("expected %d backends, got %d", len(expected), len(actual))
	}
}

func TestNew(t *testing.T) {
	locations := []struct {
		name     string
		location string
	}{
		{"fs2", "fs://test/location"},
		{"http", "http://test/location"},
		{"database", "sqlite:///test/location"},
		{"s3", "s3://test/location"},
		{"null", "null://test/location"},
		{"sftp", "sftp://test/location"},
	}

	for _, l := range locations {
		t.Run(l.name, func(t *testing.T) {
			ctx := appcontext.NewAppContext()
			ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
			ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

			storage.Register(l.name, func(storeConfig map[string]string) (storage.Store, error) {
				return ptesting.NewMockBackend(storeConfig), nil
			})

			store, err := storage.New(map[string]string{"location": l.location})
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if store.Location() != l.location {
				t.Errorf("expected location to be '%s', got %v", l.location, store.Location())
			}
		})
	}

	t.Run("unknown backend", func(t *testing.T) {
		ctx := appcontext.NewAppContext()
		ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
		ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

		// storage.Register("unknown", func(location string) storage.Store { return ptesting.NewMockBackend(location) })
		_, err := storage.New(map[string]string{"location": "unknown://dummy"})
		if err.Error() != "unsupported plakar protocol" {
			t.Fatalf("Expected %s but got %v", "unsupported plakar protocol", err)
		}
	})

	t.Run("absolute fs path", func(t *testing.T) {
		ctx := appcontext.NewAppContext()
		ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
		ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

		// storage.Register("unknown", func(location string) storage.Store { return ptesting.NewMockBackend(location) })
		store, err := storage.New(map[string]string{"location": "dummy"})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if store.Location() != "dummy" {
			t.Errorf("expected location to be '%s', got %v", "dummy", store.Location())
		}
	})
}
