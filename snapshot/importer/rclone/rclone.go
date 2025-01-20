package rclone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	stdpath "path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
)

type RcloneImporter struct {
	apiUrl string
	remote string
	base   string

	ino uint64
}

func init() {
	importer.Register("rclone", NewRcloneImporter)
}

// NewRcloneImporter creates a new RcloneImporter instance. It expects the location
// to be in the format "remote:path/to/dir". The path is optional, but the remote
// storage location is required, so the colon separator is always expected.
func NewRcloneImporter(location string) (importer.Importer, error) {
	location = strings.TrimPrefix(location, "rclone://")

	remote, base, found := strings.Cut(location, ":")

	if !found {
		return nil, fmt.Errorf("invalid location: %s. Expected format: remote:path/to/dir", location)
	}

	return &RcloneImporter{
		apiUrl: "http://127.0.0.1:5572",
		remote: remote,
		base:   base,
	}, nil
}

func (p *RcloneImporter) Scan() (<-chan importer.ScanResult, error) {
	results := make(chan importer.ScanResult)
	go func() {
		defer close(results)

		p.generateBaseDirectories(results)
		p.scanRecursive(results, "")
	}()
	return results, nil
}

// getPathInBackup returns the full normalized path of a file within the backup.
//
// The resulting path is constructed by joining the base path of the backup (p.base)
// with the provided relative path. If the base path (p.base) is not absolute,
// it is treated as relative to the root ("/").
func (p *RcloneImporter) getPathInBackup(path string) string {
	path = stdpath.Join(p.base, path)

	if !stdpath.IsAbs(p.base) {
		path = "/" + path
	}

	return stdpath.Clean(path)
}

// generateBaseDirectories sends all parent directories of the base path in
// reverse order to the provided results channel.
//
// For example, if the base is "remote:/path/to/dir", this function generates
// the directories "/path/to/dir", "/path/to", "/path", and "/".
func (p *RcloneImporter) generateBaseDirectories(results chan importer.ScanResult) {
	parts := generatePathComponents(p.getPathInBackup(""))

	for _, part := range parts {
		results <- importer.ScanRecord{
			Type:     importer.RecordTypeDirectory,
			Pathname: part,
			FileInfo: objects.NewFileInfo(
				path.Base(part),
				0,
				0700|os.ModeDir,
				time.Unix(0, 0).UTC(),
				0,
				atomic.AddUint64(&p.ino, 1),
				0,
				0,
				0,
			)}
	}
}

// generatePathComponents is a helper function that returns a slice of strings
// containing all the hierarchical components of an absolute path, starting
// from the full path down to the root.
//
// The path given as an argument must be an absolute clean path within the
// backup.
//
// Example:
//
//	Input:  "/path/to/dir"
//	Output: []string{"/path/to/dir", "/path/to", "/path", "/"}
//
//	Input:  "/relative/path"
//	Output: []string{"/relative/path", "/relative", "/"}
//
//	Input:  "/"
//	Output: []string{"/"}
func generatePathComponents(path string) []string {
	components := []string{}
	tmp := path

	for {
		components = append(components, tmp)
		parent := stdpath.Dir(tmp)
		if parent == tmp { // Reached the root
			break
		}
		tmp = parent
	}
	return components
}

func (p *RcloneImporter) scanRecursive(results chan importer.ScanResult, path string) {
	payload := map[string]interface{}{
		"fs":     fmt.Sprintf("%s:%s", p.remote, p.base),
		"remote": path,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		results <- importer.ScanError{Pathname: p.getPathInBackup(path), Err: err}
		return
	}

	req, err := http.NewRequest("POST", p.apiUrl+"/operations/list", bytes.NewBuffer(jsonPayload))
	if err != nil {
		results <- importer.ScanError{Pathname: p.getPathInBackup(path), Err: err}
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		results <- importer.ScanError{Pathname: p.getPathInBackup(path), Err: err}
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		results <- importer.ScanError{Pathname: p.getPathInBackup(path), Err: err}
		return
	}

	var response struct {
		List []struct {
			Path     string `json:"Path"`
			Name     string `json:"Name"`
			Size     int64  `json:"Size"`
			MimeType string `json:"MimeType"`
			ModTime  string `json:"ModTime"`
			IsDir    bool   `json:"isDir"`
			ID       string `json:"ID"`
		} `json:"list"`
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		results <- importer.ScanError{Pathname: p.getPathInBackup(path), Err: err}
		return
	}

	for _, file := range response.List {
		// Should never happen, but just in case let's fallback to the Unix epoch
		parsedTime, err := time.Parse(time.RFC3339, file.ModTime)
		if err != nil {
			parsedTime = time.Unix(0, 0).UTC()
		}

		if file.IsDir {
			p.scanRecursive(results, file.Path)

			results <- importer.ScanRecord{
				Type:     importer.RecordTypeDirectory,
				Pathname: p.getPathInBackup(file.Path),
				FileInfo: objects.NewFileInfo(
					stdpath.Base(file.Name),
					0,
					0700|os.ModeDir,
					parsedTime,
					0,
					atomic.AddUint64(&p.ino, 1),
					0,
					0,
					0,
				)}
		} else {
			filesize := file.Size

			// Hack: the importer is required to provide a size for the file. If
			// the size is not available when listing the directory, let's
			// attempt to download the file to retrieve it. This is a hack until
			// it becomes possible to return -1 as the size in the importer. It
			// has to be removed eventually, because it requires to download the
			// file twice when performing the backup.
			if file.Size < 0 {
				handle, err := p.NewReader(p.getPathInBackup(file.Path))
				if err != nil {
					results <- importer.ScanError{Pathname: p.getPathInBackup(path), Err: err}
					continue
				}
				name := handle.(*AutoremoveTmpFile).Name()
				size, err := os.Stat(name)
				if err != nil {
					results <- importer.ScanError{Pathname: p.getPathInBackup(path), Err: err}
					continue
				}

				handle.Close()

				filesize = size.Size()
			}

			fi := objects.NewFileInfo(
				stdpath.Base(file.Path),
				filesize,
				0600,
				parsedTime,
				1,
				atomic.AddUint64(&p.ino, 1),
				0,
				0,
				0,
			)

			results <- importer.ScanRecord{
				Type:     importer.RecordTypeFile,
				Pathname: p.getPathInBackup(file.Path),
				FileInfo: fi,
			}
		}
	}
}

// AutoremoveTmpFile is a wrapper around an os.File that removes the file when it's closed.
type AutoremoveTmpFile struct {
	*os.File
}

func (file *AutoremoveTmpFile) Close() error {
	defer os.Remove(file.Name())
	return file.File.Close()
}

func (p *RcloneImporter) NewReader(pathname string) (io.ReadCloser, error) {
	// pathname is an absolute path within the backup. Let's convert it to a
	// relative path to the base path.
	relativePath := strings.TrimPrefix(pathname, p.getPathInBackup(""))

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("plakar_temp_*%s", path.Ext(relativePath)))
	if err != nil {
		return nil, err
	}
	tmpFile.Close()

	payload := map[string]string{
		"srcFs":     fmt.Sprintf("%s:%s", p.remote, p.base),
		"srcRemote": relativePath,
		"dstFs":     "/",
		"dstRemote": tmpFile.Name(),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", p.apiUrl+"/operations/copyfile", bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, err
	}

	tmpFile, err = os.Open(tmpFile.Name())
	if err != nil {
		return nil, err
	}

	return &AutoremoveTmpFile{tmpFile}, nil
}

func (p *RcloneImporter) Close() error {
	return nil
}

func (p *RcloneImporter) Root() string {
	return p.getPathInBackup("")
}

func (p *RcloneImporter) Origin() string {
	return p.remote
}

func (p *RcloneImporter) Type() string {
	return "rclone"
}
