package importer

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/stretchr/testify/require"
)

type MockedImporter struct{}

func (m MockedImporter) Origin() string {
	return ""
}

func (m MockedImporter) Type() string {
	return ""
}

func (m MockedImporter) Root() string {
	return ""
}

func (m MockedImporter) Scan() (<-chan *ScanResult, error) {
	return nil, nil
}

func (m MockedImporter) NewReader(string) (io.ReadCloser, error) {
	return nil, nil
}

func (m MockedImporter) NewExtendedAttributeReader(string, string) (io.ReadCloser, error) {
	return nil, nil
}

func (m MockedImporter) GetExtendedAttributes(string) ([]ExtendedAttributes, error) {
	return nil, nil
}

func (m MockedImporter) Close() error {
	return nil
}

func TestBackends(t *testing.T) {
	// Setup: Register some backends
	Register("local1", func(config map[string]string) (Importer, error) {
		return nil, nil
	})
	Register("remote1", func(config map[string]string) (Importer, error) {
		return nil, nil
	})

	// Test: Retrieve the list of registered backends
	expectedBackends := []string{"local1", "remote1"}
	actualBackends := Backends()

	// Assert: Check if the actual backends match the expected
	require.ElementsMatch(t, expectedBackends, actualBackends)
}

func TestNewImporter(t *testing.T) {
	// Setup: Register some backends
	Register("fs", func(config map[string]string) (Importer, error) {
		return MockedImporter{}, nil
	})
	Register("s3", func(config map[string]string) (Importer, error) {
		return MockedImporter{}, nil
	})
	Register("ftp", func(config map[string]string) (Importer, error) {
		return MockedImporter{}, nil
	})

	tests := []struct {
		location        string
		expectedError   string
		expectedBackend string
	}{
		{location: "/", expectedError: "", expectedBackend: "fs"},
		{location: "fs://some/path", expectedError: "", expectedBackend: "fs"},
		{location: "s3://bucket/path", expectedError: "", expectedBackend: "s3"},
		{location: "ftp://some/path", expectedError: "", expectedBackend: "ftp"},
		{location: "http://unsupported", expectedError: "unsupported importer protocol", expectedBackend: ""},
	}

	for _, test := range tests {
		t.Run(test.location, func(t *testing.T) {
			importer, err := NewImporter(map[string]string{"location": test.location})

			if test.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.expectedError)
			} else {
				require.NoError(t, err)
				require.NotNil(t, importer)
			}
		})
	}
}

func TestNewScanRecord(t *testing.T) {
	pathname := "/path/to/file"
	target := "target"
	now := time.Now().Local()

	fileinfo := objects.NewFileInfo("file", 300000, 0644, now, 1, 2, 3, 4, 5)
	xattr := []string{"attr1", "attr2"}

	record := NewScanRecord(pathname, target, fileinfo, xattr)

	require.Equal(t, pathname, record.Record.Pathname)
	require.Equal(t, target, record.Record.Target)
	require.Equal(t, fileinfo, record.Record.FileInfo)
	require.ElementsMatch(t, xattr, record.Record.ExtendedAttributes)
}

func TestNewScanXattr(t *testing.T) {
	pathname := "/path/to/file"
	xattrname := "foo/bar"

	record := NewScanXattr(pathname, xattrname, objects.AttributeExtended)

	require.Equal(t, pathname, record.Record.Pathname)
	require.Equal(t, xattrname, record.Record.XattrName)
	require.True(t, record.Record.IsXattr)
}

func TestNewScanError(t *testing.T) {
	pathname := "/path/to/file"
	err := fmt.Errorf("some error")

	record := NewScanError(pathname, err)

	require.Equal(t, pathname, record.Error.Pathname)
	require.Equal(t, err, record.Error.Err)
}
