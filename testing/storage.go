package testing

import (
	"time"

	"github.com/PlakarKorp/plakar/chunking"
	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/google/uuid"
)

type configurationOptions struct {
	*storage.Configuration
}

type ConfigurationOptions func(o *storage.Configuration)

func WithConfigurationCompression(compression *compression.Configuration) ConfigurationOptions {
	return func(o *storage.Configuration) {
		o.Compression = compression
	}
}

func NewConfiguration(opts ...ConfigurationOptions) *storage.Configuration {
	conf := storage.Configuration{
		Version:      versioning.FromString(storage.VERSION),
		Timestamp:    time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		RepositoryID: uuid.MustParse("00ff0000-0000-4000-a000-000000000001"),

		Packfile: *packfile.DefaultConfiguration(),
		Chunking: *chunking.DefaultConfiguration(),
		Hashing:  *hashing.DefaultConfiguration(),

		Compression: compression.DefaultConfiguration(),
		Encryption:  encryption.DefaultConfiguration(),
	}

	for _, f := range opts {
		f(&conf)
	}

	return &conf
}
