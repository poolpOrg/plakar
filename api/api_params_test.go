package api

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"reflect"
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

func TestPathParamToID(t *testing.T) {
	req, err := http.NewRequest("GET", "/path/{id}", nil)
	req.Pattern = "/path/{id}"

	if err != nil {
		t.Fatal(err)
	}

	req.SetPathValue("id", "7e0e6e24a6e29faf11d022dca77826fe8b8a000aff5ea27e16650d03acefc93c")

	id, err := PathParamToID(req, "id")
	if err != nil {
		t.Errorf("PathParamToID returned error: %v", err)
	}

	expectedID := [32]uint8{
		0x7e, 0xe, 0x6e, 0x24, 0xa6, 0xe2, 0x9f, 0xaf,
		0x11, 0xd0, 0x22, 0xdc, 0xa7, 0x78, 0x26, 0xfe,
		0x8b, 0x8a, 0x0, 0xa, 0xff, 0x5e, 0xa2, 0x7e,
		0x16, 0x65, 0xd, 0x3, 0xac, 0xef, 0xc9, 0x3c,
	}

	if id != expectedID {
		t.Errorf("PathParamToID returned unexpected ID: %v", id)
	}
}

func TestPathParamToID_Invalid(t *testing.T) {
	tests := []struct {
		name string
		id   string
		err  string
	}{
		{
			name: "empty id",
			id:   "",
			err:  "invalid_params: Invalid parameter",
		},
		{
			name: "wrong format",
			id:   "0",
			err:  "invalid_params: Invalid parameter",
		},
		{
			name: "wrong length",
			id:   "67",
			err:  "invalid_params: Invalid parameter",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/path/{id}", nil)
			if err != nil {
				t.Fatal(err)
			}

			req.SetPathValue("id", test.id)

			_, err = PathParamToID(req, "id")
			if err.Error() != test.err {
				t.Errorf("wrong error, expected: %v, got: %v", err, test.err)
			}
		})
	}
}

func TestQueryParamToUint32(t *testing.T) {
	tests := []struct {
		name       string
		param      string
		want       uint32
		wantErr    bool
		wantExists bool
	}{
		{
			name:       "empty param",
			param:      "",
			want:       0,
			wantErr:    false,
			wantExists: false,
		},
		{
			name:       "valid param",
			param:      "123",
			want:       123,
			wantErr:    false,
			wantExists: true,
		},
		{
			name:       "invalid param",
			param:      "abc",
			want:       0,
			wantErr:    true,
			wantExists: true,
		},
		{
			name:       "negative param",
			param:      "-1",
			want:       0,
			wantErr:    true,
			wantExists: true,
		},
		{
			name:       "out of range param",
			param:      "4294967296",
			want:       0,
			wantErr:    true,
			wantExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/?param="+tt.param, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, gotExists, err := QueryParamToUint32(req, "param")
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryParamToUint32() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("QueryParamToUint32() got = %v, want %v", got, tt.want)
			}
			if gotExists != tt.wantExists {
				t.Errorf("QueryParamToUint32() gotExists = %v, want %v", gotExists, tt.wantExists)
			}
		})
	}
}

func TestQueryParamToInt64(t *testing.T) {
	tests := []struct {
		name       string
		param      string
		want       int64
		wantErr    bool
		wantExists bool
	}{
		{
			name:       "empty param",
			param:      "",
			want:       0,
			wantErr:    false,
			wantExists: false,
		},
		{
			name:       "valid param",
			param:      "123",
			want:       123,
			wantErr:    false,
			wantExists: true,
		},
		{
			name:       "valid param max 64 bit",
			param:      "9223372036854775807",
			want:       9223372036854775807,
			wantErr:    false,
			wantExists: true,
		},
		{
			name:       "invalid param",
			param:      "abc",
			want:       0,
			wantErr:    true,
			wantExists: true,
		},
		{
			name:       "out of range param",
			param:      "9223372036854775808",
			want:       0,
			wantErr:    true,
			wantExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/?param="+tt.param, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, gotExists, err := QueryParamToInt64(req, "param")
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryParamToInt64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("QueryParamToInt64() got = %v, want %v", got, tt.want)
			}
			if gotExists != tt.wantExists {
				t.Errorf("QueryParamToInt64() gotExists = %v, want %v", gotExists, tt.wantExists)
			}
		})
	}
}

func TestQueryParamToSortKeys(t *testing.T) {
	tests := []struct {
		name    string
		param   string
		def     string
		want    []string
		wantErr bool
	}{
		{
			name:    "empty param",
			param:   "",
			def:     "Timestamp",
			want:    []string{"Timestamp"},
			wantErr: false,
		},
		{
			name:    "valid param",
			param:   "-Timestamp",
			def:     "",
			want:    []string{"-Timestamp"},
			wantErr: false,
		},
		{
			name:    "multiple valid params",
			param:   "Timestamp,Identifier",
			def:     "",
			want:    []string{"Timestamp", "Identifier"},
			wantErr: false,
		},
		{
			name:    "invalid param",
			param:   "InvalidKey",
			def:     "",
			want:    []string{},
			wantErr: true,
		},
		{
			name:    "default multiple params",
			param:   "",
			def:     "-Identifier,-Timestamp",
			want:    []string{"-Identifier", "-Timestamp"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/?sort="+tt.param, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := QueryParamToSortKeys(req, "sort", tt.def)
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryParamToSortKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryParamToSortKeys() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func _TestSnapshotPathParam(t *testing.T) {
	testCases := []struct {
		name     string
		id       string
		config   *storage.Configuration
		location string
		err      string
	}{
		{
			name:     "empty id",
			id:       "",
			location: "/test/location",
			config:   ptesting.NewConfiguration(),
			err:      "invalid_params: Invalid parameter",
		},
		{
			name:     "empty id",
			id:       "12345:/dummy",
			location: "/test/location?behavior=oneState",
			config:   ptesting.NewConfiguration(),
			err:      "invalid_params: Invalid parameter",
		},
		{
			name:     "working",
			id:       "1000000000000000000000000000000000000000000000000000000000000000:/dummy",
			location: "/test/location?behavior=oneState",
			config:   ptesting.NewConfiguration(),
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {

			serializedConfig, err := c.config.ToBytes()
			require.NoError(t, err)

			hasher := hashing.GetHasher("SHA256")
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

			req, err := http.NewRequest("GET", "/path/{id}", nil)
			if err != nil {
				t.Fatal(err)
			}

			req.SetPathValue("id", c.id)

			_, _, err = SnapshotPathParam(req, repo, "id")
			if c.err != "" {
				require.Error(t, err)
			}
		})
	}
}
