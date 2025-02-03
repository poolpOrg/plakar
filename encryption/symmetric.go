package encryption

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	"golang.org/x/crypto/scrypt"
)

const (
	saltSize  = 16
	chunkSize = 64 * 1024 // Size of each chunk for encryption/decryption
)

type Configuration struct {
	Algorithm string
	KDF       string
	KDFParams KDFParams
	Canary    []byte
}

type KDFParams struct {
	Salt   [saltSize]byte
	N      int
	R      int
	P      int
	KeyLen int
}

func DefaultConfiguration() *Configuration {
	return &Configuration{
		Algorithm: "AES256-GCM",
		KDF:       "SCRYPT",
		KDFParams: KDFParams{
			N:      1 << 15,
			R:      8,
			P:      1,
			KeyLen: 32,
		},
	}
}

func Salt() (salt [saltSize]byte, err error) {
	_, err = rand.Read(salt[:])
	return
}

// BuildSecretFromPassphrase generates a secret from a passphrase using scrypt
func DeriveKey(params KDFParams, passphrase []byte) ([]byte, error) {
	return scrypt.Key(passphrase, params.Salt[:], params.N, params.R, params.P, params.KeyLen)
}

func DeriveCanary(key []byte) ([]byte, error) {
	canary := make([]byte, 32)
	if _, err := rand.Read(canary); err != nil {
		return nil, err
	}

	rd, err := EncryptStream(key, bytes.NewReader(canary))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rd)
}

func VerifyCanary(key []byte, canary []byte) bool {
	rd, err := DecryptStream(key, bytes.NewReader(canary))
	if err != nil {
		return false
	}
	_, err = io.ReadAll(rd)
	return err == nil
}

// EncryptStream encrypts a stream using AES-GCM with a random session-specific subkey
func EncryptStream(key []byte, r io.Reader) (io.Reader, error) {
	// Generate a random subkey for data encryption
	subkey := make([]byte, 32)
	if _, err := rand.Read(subkey); err != nil {
		return nil, err
	}

	// Encrypt the subkey with the main key using AES-GCM
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Generate a nonce for subkey encryption
	subkeyNonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(subkeyNonce); err != nil {
		return nil, err
	}

	// Encrypt the subkey
	encSubkey := gcm.Seal(nil, subkeyNonce, subkey, nil)

	// Set up AES-GCM for data encryption using the subkey
	dataBlock, err := aes.NewCipher(subkey)
	if err != nil {
		return nil, err
	}
	dataGCM, err := cipher.NewGCM(dataBlock)
	if err != nil {
		return nil, err
	}

	// Set up the pipe for streaming encryption
	pr, pw := io.Pipe()

	// Start encryption in a goroutine
	go func() {
		defer pw.Close()

		// Write the encrypted subkey and both nonces to the output stream
		if _, err := pw.Write(subkeyNonce); err != nil {
			pw.CloseWithError(err)
			return
		}
		if _, err := pw.Write(encSubkey); err != nil {
			pw.CloseWithError(err)
			return
		}

		// Encrypt and write data chunks
		chunk := make([]byte, chunkSize)
		for {
			// Use ReadFull to read exactly chunkSize or less at EOF
			n, err := io.ReadFull(r, chunk)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				pw.CloseWithError(err)
				return
			}

			if n > 0 {
				// Generate nonce and encrypt the chunk
				dataNonce := make([]byte, dataGCM.NonceSize())
				if _, err := rand.Read(dataNonce); err != nil {
					pw.CloseWithError(err)
					return
				}
				if _, err := pw.Write(dataNonce); err != nil {
					pw.CloseWithError(err)
					return
				}

				encryptedChunk := dataGCM.Seal(nil, dataNonce, chunk[:n], nil)
				if _, err := pw.Write(encryptedChunk); err != nil {
					pw.CloseWithError(err)
					return
				}
			}

			// Stop when EOF is reached
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return
			}
		}
	}()

	return pr, nil
}

// DecryptStream decrypts a stream using AES-GCM with a random session-specific subkey
func DecryptStream(key []byte, r io.Reader) (io.Reader, error) {
	// Set up to decrypt the subkey from the input
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Read and decrypt the subkey
	subkeyNonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(r, subkeyNonce); err != nil {
		return nil, err
	}

	encSubkey := make([]byte, gcm.Overhead()+32) // GCM overhead for the 32-byte subkey
	if _, err := io.ReadFull(r, encSubkey); err != nil {
		return nil, err
	}

	subkey, err := gcm.Open(nil, subkeyNonce, encSubkey, nil)
	if err != nil {
		return nil, err
	}

	// Set up AES-GCM for actual data decryption using the subkey
	dataBlock, err := aes.NewCipher(subkey)
	if err != nil {
		return nil, err
	}
	dataGCM, err := cipher.NewGCM(dataBlock)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()

	// Start decryption in a goroutine
	go func() {
		defer pw.Close()

		buffer := make([]byte, chunkSize+dataGCM.Overhead())
		for {
			// Read the data nonce from the input
			dataNonce := make([]byte, dataGCM.NonceSize())
			if _, err := io.ReadFull(r, dataNonce); err != nil {
				pw.CloseWithError(err)
				return
			}

			n, err := r.Read(buffer)
			if err != nil {
				if err != io.EOF {
					pw.CloseWithError(err)
					return
				}
			}

			if n == 0 {
				return
			}

			// Decrypt each chunk and write it to the pipe
			decryptedChunk, err := dataGCM.Open(nil, dataNonce, buffer[:n], nil)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, err := pw.Write(decryptedChunk); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
	}()

	return pr, nil
}
