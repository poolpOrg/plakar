package encryption

import (
	"crypto/rand"
	"io"
	"strings"
	"testing"

	"github.com/PlakarKorp/plakar/compression"
)

func TestDeriveKey(t *testing.T) {
	config := DefaultConfiguration()

	salt, err := Salt()
	if err != nil {
		t.Fatalf("Failed to generate random salt: %v", err)
	}
	config.KDFParams.Salt = salt

	passphrase := []byte("strong passphrase")
	key, err := DeriveKey(config.KDFParams, passphrase)
	if err != nil {
		t.Fatalf("Failed to derive key from passphrase: %v", err)
	}

	// Verify that derived key is non-nil and of expected length
	if key == nil || len(key) != 32 {
		t.Errorf("Unexpected derived key length. Got %d, want 32", len(key))
	}
}

func TestEncryptDecryptStream(t *testing.T) {
	config := DefaultConfiguration()

	salt, err := Salt()
	if err != nil {
		t.Fatalf("Failed to generate random salt: %v", err)
	}
	config.KDFParams.Salt = salt

	passphrase := []byte("strong passphrase")
	key, err := DeriveKey(config.KDFParams, passphrase)
	if err != nil {
		t.Fatalf("Failed to derive key from passphrase: %v", err)
	}

	// Original data to encrypt and decrypt
	originalData := "This is a test data string for encryption and decryption"
	r := strings.NewReader(originalData)

	// Encrypt the data
	encryptedReader, err := EncryptStream(key, r)
	if err != nil {
		t.Fatalf("Failed to encrypt data: %v", err)
	}

	// Decrypt the data
	decryptedReader, err := DecryptStream(key, encryptedReader)
	if err != nil {
		t.Fatalf("Failed to decrypt data: %v", err)
	}

	// Read the decrypted data
	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	// Verify the decrypted data matches the original data
	if string(decryptedData) != originalData {
		t.Errorf("Decrypted data does not match original. Got: %q, want: %q", string(decryptedData), originalData)
	}
}

func TestEncryptDecryptEmptyStream(t *testing.T) {

	config := DefaultConfiguration()

	salt, err := Salt()
	if err != nil {
		t.Fatalf("Failed to generate random salt: %v", err)
	}
	config.KDFParams.Salt = salt

	passphrase := []byte("strong passphrase")
	key, err := DeriveKey(config.KDFParams, passphrase)
	if err != nil {
		t.Fatalf("Failed to derive key from passphrase: %v", err)
	}

	// Original data to encrypt and decrypt
	originalData := ""
	r := strings.NewReader(originalData)

	// Encrypt the data
	encryptedReader, err := EncryptStream(key, r)
	if err != nil {
		t.Fatalf("Failed to encrypt data: %v", err)
	}

	// Decrypt the data
	decryptedReader, err := DecryptStream(key, encryptedReader)
	if err != nil {
		t.Fatalf("Failed to decrypt data: %v", err)
	}

	// Read the decrypted data
	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	// Verify the decrypted data matches the original data
	if string(decryptedData) != originalData {
		t.Errorf("Decrypted data does not match original. Got: %q, want: %q", string(decryptedData), originalData)
	}
}

func TestEncryptDecryptStreamWithIncorrectKey(t *testing.T) {
	config := DefaultConfiguration()

	salt, err := Salt()
	if err != nil {
		t.Fatalf("Failed to generate random salt: %v", err)
	}
	config.KDFParams.Salt = salt

	passphrase := []byte("strong passphrase")
	key, err := DeriveKey(config.KDFParams, passphrase)
	if err != nil {
		t.Fatalf("Failed to derive key from passphrase: %v", err)
	}

	// Original data to encrypt and decrypt
	originalData := "Sensitive information to protect"
	r := strings.NewReader(originalData)

	// Encrypt the data
	encryptedReader, err := EncryptStream(key, r)
	if err != nil {
		t.Fatalf("Failed to encrypt data: %v", err)
	}

	// Generate an incorrect key for decryption
	incorrectKey := make([]byte, len(key))
	if _, err := rand.Read(incorrectKey); err != nil {
		t.Fatalf("Failed to generate incorrect decryption key: %v", err)
	}

	// Attempt to decrypt the data with the incorrect key
	decryptedReader, err := DecryptStream(incorrectKey, encryptedReader)
	if err == nil {
		// Attempt to read the (likely) invalid decrypted data to trigger an error
		if _, readErr := io.ReadAll(decryptedReader); readErr == nil {
			t.Error("Expected error during decryption with incorrect key, but got none")
		}
	} else {
		t.Logf("Decryption failed as expected with incorrect key: %v", err)
	}
}

func TestCompressEncryptThenDecryptDecompressStream(t *testing.T) {
	config := DefaultConfiguration()

	salt, err := Salt()
	if err != nil {
		t.Fatalf("Failed to generate random salt: %v", err)
	}
	config.KDFParams.Salt = salt

	passphrase := []byte("strong passphrase")
	key, err := DeriveKey(config.KDFParams, passphrase)
	if err != nil {
		t.Fatalf("Failed to derive key from passphrase: %v", err)
	}

	// Original data to compress, encrypt, decrypt, and decompress
	originalData := "This is a test string for compression and encryption. It should work!"
	r := strings.NewReader(originalData)

	// Step 1: Compress the data
	compressedReader, err := compression.DeflateStream("GZIP", r)
	if err != nil {
		t.Fatalf("Failed to compress data: %v", err)
	}

	// Step 2: Encrypt the compressed data
	encryptedReader, err := EncryptStream(key, compressedReader)
	if err != nil {
		t.Fatalf("Failed to encrypt data: %v", err)
	}

	// Step 3: Decrypt the data
	decryptedReader, err := DecryptStream(key, encryptedReader)
	if err != nil {
		t.Fatalf("Failed to decrypt data: %v", err)
	}

	// Step 4: Decompress the decrypted data
	decompressedReader, err := compression.InflateStream("GZIP", decryptedReader)
	if err != nil {
		t.Fatalf("Failed to decompress data: %v", err)
	}

	// Read the final output
	finalData, err := io.ReadAll(decompressedReader)
	if err != nil {
		t.Fatalf("Failed to read decompressed data: %v", err)
	}

	// Verify the final data matches the original data
	if string(finalData) != originalData {
		t.Errorf("Final data does not match original. Got: %q, want: %q", string(finalData), originalData)
	}
}
