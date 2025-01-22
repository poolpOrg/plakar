package rclone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	stdpath "path"
	"strings"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/exporter"
)

type RcloneExporter struct {
	apiUrl string
	remote string
	base   string
}

func init() {
	exporter.Register("rclone", NewRcloneExporter)
}

func NewRcloneExporter(location string) (exporter.Exporter, error) {

	fmt.Printf("NewRcloneExporter: %v\n", location)

	location = strings.TrimPrefix(location, "rclone://")

	remote, base, found := strings.Cut(location, ":")

	if !found {
		return nil, fmt.Errorf("invalid location: %s. Expected format: remote:path/to/dir", location)
	}

	return &RcloneExporter{
		apiUrl: "http://127.0.0.1:5572",
		remote: remote,
		base:   base,
	}, nil
}

// getPathInBackup returns the full normalized path of a file within the backup.
//
// The resulting path is constructed by joining the base path of the backup (p.base)
// with the provided relative path. If the base path (p.base) is not absolute,
// it is treated as relative to the root ("/").
func (p *RcloneExporter) getPathInBackup(path string) string {
	path = stdpath.Join(p.base, path)

	if !stdpath.IsAbs(p.base) {
		path = "/" + path
	}

	return stdpath.Clean(path)
}

func (p *RcloneExporter) Root() string {
	return p.getPathInBackup("")
}

func (p *RcloneExporter) CreateDirectory(pathname string) error {
	relativePath := strings.TrimPrefix(pathname, p.getPathInBackup(""))

	payload := map[string]string{
		"fs":     fmt.Sprintf("%s:%s", p.remote, p.base),
		"remote": relativePath,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", p.apiUrl+"/operations/mkdir", bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return err
	}

	return nil
}

// XXX: it seems there is a race condition when restoring a directory: when
// exporting the first file, the root directory is created. When exporting the
// second file, it is possible that Google Drive doesn't see the root directory
// yet, and creates a new one. This results in a duplicated root directory, with
// some files in the first directory and the rest in the second.
func (p *RcloneExporter) StoreFile(pathname string, fp io.Reader) error {
	tmpFile, err := os.CreateTemp("", "tempfile-*.tmp")
	if err != nil {
		return err
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	_, err = io.Copy(tmpFile, fp)
	if err != nil {
		return err
	}

	relativePath := strings.TrimPrefix(pathname, p.getPathInBackup(""))

	payload := map[string]string{
		"srcFs":     "/",
		"srcRemote": tmpFile.Name(),
		"dstFs":     fmt.Sprintf("%s:%s", p.remote, p.base),
		"dstRemote": relativePath,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", p.apiUrl+"/operations/copyfile", bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return err
	}
	return nil
}

func (p *RcloneExporter) SetPermissions(pathname string, fileinfo *objects.FileInfo) error {
	return nil
}

func (p *RcloneExporter) Close() error {
	return nil
}
