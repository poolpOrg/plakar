/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package utils

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	passwordvalidator "github.com/wagslane/go-password-validator"
	"golang.org/x/mod/semver"
	"golang.org/x/term"
	"golang.org/x/tools/blog/atom"
)

// ParseSnapshotID parses a string in the form snapshotID[:pattern] and
// returns the two strings.
func ParseSnapshotID(id string) (string, string) {
	tmp := strings.SplitN(id, ":", 2)
	prefix := id
	pattern := ""
	if len(tmp) == 2 {
		prefix, pattern = tmp[0], tmp[1]
		if runtime.GOOS != "windows" {
			if !strings.HasPrefix(pattern, "/") {
				pattern = "/" + pattern
			}
		}
	}
	return prefix, pattern
}

func HumanToDuration(human string) (time.Duration, error) {
	// support either one of the following:
	// - time.Duration string
	// - human readable string (e.g. 1h, 1d, 1w, 1m, 1y)
	// - human readable string with time.Duration suffix (e.g. 1h30m, 1d12h, 1w3d, 1m2w, 1y1m)

	// first we check if it's a time.Duration string
	duration, err := time.ParseDuration(human)
	if err == nil {
		return duration, nil
	}

	// TODO-handle iteratively constructed human readable strings

	return 0, fmt.Errorf("invalid duration: %s", human)
}

type ReleaseUpdateSummary struct {
	FoundCount int
	Latest     string

	SecurityFix    bool
	ReliabilityFix bool
}

func shouldCheckUpdate(cachedir string) bool {
	cookie := path.Join(cachedir, "last-update-check")
	cutoff := time.Now().Add(-24 * time.Hour)

	sb, err := os.Stat(cookie)
	if err == nil && sb.ModTime().After(cutoff) {
		return false
	}

	file, err := os.Create(cookie)
	if err != nil {
		file.Close()
	}

	return true
}

func CheckUpdate(cachedir string) (update ReleaseUpdateSummary, err error) {
	if !shouldCheckUpdate(cachedir) {
		return
	}

	req, err := http.NewRequest("GET", "https://plakar.io/api/releases.atom", nil)
	if err != nil {
		return
	}

	req.Header.Set("User-Agent", fmt.Sprintf("plakar/%s (%s/%s)", VERSION, runtime.GOOS, runtime.GOARCH))

	client := http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()

	var feed []atom.Feed
	err = xml.NewDecoder(res.Body).Decode(&feed)
	if err != nil {
		return
	}

	var latestEntry *atom.Entry
	for _, entry := range feed[0].Entry {
		if !semver.IsValid(entry.Title) {
			continue
		}
		if semver.Compare(VERSION, entry.Title) >= 0 {
			continue
		}

		update.FoundCount++

		if latestEntry == nil || semver.Compare(latestEntry.Title, entry.Title) < 0 {
			latestEntry = entry
		}

		if latestEntry.Content == nil {
			continue
		}

		body := latestEntry.Content.Body
		if strings.Contains(body, "SECURITY") {
			update.SecurityFix = true
		}
		if strings.Contains(body, "RELIABILITY") {
			update.ReliabilityFix = true
		}
	}
	return
}

func GetPassphrase(prefix string) ([]byte, error) {
	fmt.Fprintf(os.Stderr, "%s passphrase: ", prefix)
	passphrase, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintf(os.Stderr, "\n")
	if err != nil {
		return nil, err
	}
	return passphrase, nil
}

func GetPassphraseConfirm(prefix string, minEntropyBits float64) ([]byte, error) {
	fmt.Fprintf(os.Stderr, "%s passphrase: ", prefix)
	passphrase1, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintf(os.Stderr, "\n")
	if err != nil {
		return nil, err
	}

	// keepass considers < 80 bits as weak
	err = passwordvalidator.Validate(string(passphrase1), minEntropyBits)
	if err != nil {
		return nil, fmt.Errorf("passphrase is too weak: %s", err)
	}

	fmt.Fprintf(os.Stderr, "%s passphrase (confirm): ", prefix)
	passphrase2, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintf(os.Stderr, "\n")
	if err != nil {
		return nil, err
	}

	if string(passphrase1) != string(passphrase2) {
		return nil, errors.New("passphrases mismatch")
	}

	return passphrase1, nil
}

func GetCacheDir(appName string) (string, error) {
	var cacheDir string

	switch runtime.GOOS {
	case "windows":
		// Use %LocalAppData%
		cacheDir = os.Getenv("LocalAppData")
		if cacheDir == "" {
			return "", fmt.Errorf("LocalAppData environment variable not set")
		}
		cacheDir = filepath.Join(cacheDir, appName)
	default:
		// Use XDG_CACHE_HOME or default to ~/.cache
		cacheDir = os.Getenv("XDG_CACHE_HOME")
		if cacheDir == "" {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return "", err
			}
			cacheDir = filepath.Join(homeDir, ".cache", appName)
		} else {
			cacheDir = filepath.Join(cacheDir, appName)
		}
	}

	// Create the cache directory if it doesn't exist
	err := os.MkdirAll(cacheDir, 0700)
	if err != nil {
		return "", err
	}

	return cacheDir, nil
}

func GetConfigDir(appName string) (string, error) {
	var configDir string

	switch runtime.GOOS {
	case "windows":
		// Use %LocalAppData%
		configDir = os.Getenv("LocalAppData")
		if configDir == "" {
			return "", fmt.Errorf("LocalAppData environment variable not set")
		}
		configDir = filepath.Join(configDir, appName)
	default:
		// Use XDG_CACHE_HOME or default to ~/.cache
		configDir = os.Getenv("XDG_CONFIG_HOME")
		if configDir == "" {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return "", err
			}
			configDir = filepath.Join(homeDir, ".config", appName)
		} else {
			configDir = filepath.Join(configDir, appName)
		}
	}

	// Create the cache directory if it doesn't exist
	err := os.MkdirAll(configDir, 0700)
	if err != nil {
		return "", err
	}

	return configDir, nil
}

const VERSION = "v0.4.24-alpha"

func GetVersion() string {
	return VERSION
}

func NormalizePath(path string) (string, error) {
	path = filepath.Clean(path)
	parts := strings.Split(path, string(filepath.Separator))[1:]

	if len(parts) == 0 || parts[0] == "" {
		return "", fmt.Errorf("invalid path")
	}

	var normalizedPath string
	// For Windows, start with the drive letter.
	if filepath.IsAbs(path) {
		normalizedPath = string(filepath.Separator)
	}

	for _, part := range parts {
		if part == "" {
			continue
		}

		dirEntries, err := os.ReadDir(normalizedPath)
		if err != nil {
			return "", err
		}

		matched := false
		for _, entry := range dirEntries {
			if strings.EqualFold(entry.Name(), part) {
				normalizedPath = filepath.Join(normalizedPath, entry.Name())
				matched = true
				break
			}
		}

		if !matched {
			return "", fmt.Errorf("path not found: %s", path)
		}
	}

	return normalizedPath, nil
}
