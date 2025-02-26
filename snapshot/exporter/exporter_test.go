package exporter

import (
	"io"
	"testing"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/stretchr/testify/require"
)

type MockedExporter struct{}

func (m MockedExporter) Root() string {
	return ""
}

func (m MockedExporter) CreateDirectory(pathname string) error {
	return nil
}

func (m MockedExporter) StoreFile(pathname string, fp io.Reader) error {
	return nil
}

func (m MockedExporter) SetPermissions(pathname string, fileinfo *objects.FileInfo) error {
	return nil
}

func (m MockedExporter) Close() error {
	return nil
}

func TestBackends(t *testing.T) {
	// Setup: Register some backends
	Register("fs1", func(config map[string]string) (Exporter, error) {
		return nil, nil
	})
	Register("s33", func(config map[string]string) (Exporter, error) {
		return nil, nil
	})

	// Test: Retrieve the list of registered backends
	expectedBackends := []string{"fs1", "s33"}
	actualBackends := Backends()

	// Assert: Check if the actual backends match the expected
	require.ElementsMatch(t, expectedBackends, actualBackends)
}

func TestNewExporter(t *testing.T) {
	// Setup: Register some backends
	Register("fs", func(config map[string]string) (Exporter, error) {
		return MockedExporter{}, nil
	})
	Register("s3", func(config map[string]string) (Exporter, error) {
		return MockedExporter{}, nil
	})

	tests := []struct {
		location        string
		expectedError   string
		expectedBackend string
	}{
		{location: "/", expectedError: "", expectedBackend: "fs"},
		{location: "fs://some/path", expectedError: "", expectedBackend: "fs"},
		{location: "s3://bucket/path", expectedError: "", expectedBackend: "s3"},
		{location: "http://unsupported", expectedError: "unsupported importer protocol", expectedBackend: ""},
	}

	for _, test := range tests {
		t.Run(test.location, func(t *testing.T) {
			exporter, err := NewExporter(map[string]string{"location": test.location})

			if test.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.expectedError)
			} else {
				require.NoError(t, err)
				require.NotNil(t, exporter)
			}
		})
	}
}
