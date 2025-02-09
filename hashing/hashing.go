package hashing

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"hash"
)

type Configuration struct {
	Algorithm string // Hashing algorithm name (e.g., "SHA256", "BLAKE3")
	Bits      uint32
}

func DefaultConfiguration() *Configuration {
	configuration, _ := LookupDefaultConfiguration("SHA256")
	return configuration
}

func LookupDefaultConfiguration(algorithm string) (*Configuration, error) {
	switch algorithm {
	case "SHA256":
		return &Configuration{
			Algorithm: "SHA256",
			Bits:      256,
		}, nil
	default:
		return nil, fmt.Errorf("unknown hashing algorithm: %s", algorithm)
	}
}

func GetHasher(name string) hash.Hash {
	switch name {
	case "SHA256":
		return sha256.New()
	default:
		return nil
	}
}

func GetHasherHMAC(name string, secret []byte) hash.Hash {
	switch name {
	case "SHA256":
		return hmac.New(sha256.New, secret)
	default:
		return nil
	}
}
