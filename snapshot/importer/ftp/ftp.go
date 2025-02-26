/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
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

package ftp

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/secsy/goftp"
)

type FTPImporter struct {
	host    string
	rootDir string
	client  *goftp.Client
}

func init() {
	importer.Register("ftp", NewFTPImporter)
}

func connectToFTP(host, username, password string) (*goftp.Client, error) {
	config := goftp.Config{
		User:     username,
		Password: password,
		Timeout:  10 * time.Second,
	}
	return goftp.DialConfig(config, host)
}

func NewFTPImporter(config map[string]string) (importer.Importer, error) {

	location := config["location"]

	parsed, err := url.Parse(location)
	if err != nil {
		return nil, err
	}

	//if parsed.User != nil {
	//	username = parsed.User.Username()
	//	if tmppass, passexists := parsed.User.Password(); passexists {
	//		password = tmppass
	//	}
	//}

	return &FTPImporter{
		host:    parsed.Host,
		rootDir: parsed.Path,
		//		client:  client,
	}, nil
}

func (p *FTPImporter) ftpWalker_worker(jobs <-chan string, results chan<- *importer.ScanResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for path := range jobs {
		info, err := p.client.Stat(path)
		if err != nil {
			results <- importer.NewScanError(path, err)
			continue
		}

		fileinfo := objects.FileInfoFromStat(info)

		results <- importer.NewScanRecord(filepath.ToSlash(path), "", fileinfo, nil)

		// Handle symlinks separately
		if fileinfo.Mode()&os.ModeSymlink != 0 {
			originFile, err := os.Readlink(path)
			if err != nil {
				results <- importer.NewScanError(path, err)
				continue
			}
			results <- importer.NewScanRecord(filepath.ToSlash(path), originFile, fileinfo, nil)
		}
	}
}

func (p *FTPImporter) ftpWalker_addPrefixDirectories(jobs chan<- string, results chan<- *importer.ScanResult) {
	directory := filepath.Clean(p.rootDir)
	atoms := strings.Split(directory, string(os.PathSeparator))

	for i := 0; i < len(atoms); i++ {
		path := filepath.Join(atoms[0 : i+1]...)

		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}

		if _, err := p.client.Stat(path); err != nil {
			results <- importer.NewScanError(path, err)
			continue
		}

		jobs <- path
	}
}

func (p *FTPImporter) walkDir(root string, results chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()

	entries, err := p.client.ReadDir(root)
	if err != nil {
		log.Printf("Error reading directory %s: %v", root, err)
		return
	}

	for _, entry := range entries {
		entryPath := filepath.Join(root, entry.Name())

		// Send the current entry to the results channel
		results <- entryPath

		// If the entry is a directory, traverse it recursively
		if entry.IsDir() {
			wg.Add(1)
			go p.walkDir(entryPath, results, wg)
		}
	}
}

func (p *FTPImporter) Scan() (<-chan *importer.ScanResult, error) {
	client, err := connectToFTP(p.host, "", "")
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	p.client = client

	results := make(chan *importer.ScanResult, 1000) // Larger buffer for results
	jobs := make(chan string, 1000)                  // Buffered channel to feed paths to workers
	var wg sync.WaitGroup
	numWorkers := 256

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go p.ftpWalker_worker(jobs, results, &wg)
	}

	go func() {
		defer close(jobs)
		p.ftpWalker_addPrefixDirectories(jobs, results)
		wg.Add(1)
		p.walkDir(p.rootDir, jobs, &wg)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	return results, nil
}

func (p *FTPImporter) NewReader(pathname string) (io.ReadCloser, error) {
	tmpfile, err := os.CreateTemp("", "plakar-ftp-")
	if err != nil {
		return nil, err
	}

	err = p.client.Retrieve(pathname, tmpfile)
	if err != nil {
		return nil, err
	}
	tmpfile.Seek(0, 0)

	return tmpfile, nil
}

func (p *FTPImporter) NewExtendedAttributeReader(pathname string, attribute string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("extended attributes are not supported on FTP")
}

func (p *FTPImporter) GetExtendedAttributes(pathname string) ([]importer.ExtendedAttributes, error) {
	return nil, fmt.Errorf("extended attributes are not supported on FTP")
}

func (p *FTPImporter) Close() error {
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}

func (p *FTPImporter) Root() string {
	return p.rootDir
}

func (p *FTPImporter) Origin() string {
	return p.host
}

func (p *FTPImporter) Type() string {
	return "ftp"
}
