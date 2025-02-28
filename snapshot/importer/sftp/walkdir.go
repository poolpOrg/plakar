//go:build !windows
// +build !windows

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

package sftp

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/pkg/sftp"
)

type namecache struct {
	uidToName map[uint64]string
	gidToName map[uint64]string

	mu sync.RWMutex
}

// Worker pool to handle file scanning in parallel
func (p *SFTPImporter) walkDir_worker(jobs <-chan string, results chan<- *importer.ScanResult, wg *sync.WaitGroup, namecache *namecache) {
	defer wg.Done()

	for path := range jobs {
		info, err := p.client.Lstat(path)
		if err != nil {
			results <- importer.NewScanError(path, err)
			continue
		}

		fileinfo := objects.FileInfoFromStat(info)

		namecache.mu.RLock()
		if uname, ok := namecache.uidToName[fileinfo.Uid()]; !ok {
			if u, err := user.LookupId(fmt.Sprintf("%d", fileinfo.Uid())); err == nil {
				fileinfo.Lusername = u.Username

				namecache.mu.RUnlock()
				namecache.mu.Lock()
				namecache.uidToName[fileinfo.Uid()] = u.Username
				namecache.mu.Unlock()
				namecache.mu.RLock()
			}
		} else {
			fileinfo.Lusername = uname
		}

		if gname, ok := namecache.gidToName[fileinfo.Uid()]; !ok {
			if g, err := user.LookupGroupId(fmt.Sprintf("%d", fileinfo.Gid())); err == nil {
				fileinfo.Lgroupname = g.Name

				namecache.mu.RUnlock()
				namecache.mu.Lock()
				namecache.gidToName[fileinfo.Gid()] = g.Name
				namecache.mu.Unlock()
				namecache.mu.RLock()
			}
		} else {
			fileinfo.Lgroupname = gname
		}
		namecache.mu.RUnlock()

		var originFile string
		if fileinfo.Mode()&os.ModeSymlink != 0 {
			originFile, err = p.client.ReadLink(path)
			if err != nil {
				results <- importer.NewScanError(path, err)
				continue
			}
		}
		results <- importer.NewScanRecord(filepath.ToSlash(path), originFile, fileinfo, []string{})
	}
}

func (p *SFTPImporter) walkDir_addPrefixDirectories(jobs chan<- string, results chan<- *importer.ScanResult) {
	// Clean the directory and split the path into components
	directory := filepath.Clean(p.rootDir)
	atoms := strings.Split(directory, string(os.PathSeparator))

	for i := 0; i < len(atoms)-1; i++ {
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

func (p *SFTPImporter) walkDir_walker(numWorkers int) (<-chan *importer.ScanResult, error) {
	results := make(chan *importer.ScanResult, 1000) // Larger buffer for results
	jobs := make(chan string, 1000)                  // Buffered channel to feed paths to workers
	namecache := &namecache{
		uidToName: make(map[uint64]string),
		gidToName: make(map[uint64]string),
	}

	var wg sync.WaitGroup

	// Launch worker pool
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go p.walkDir_worker(jobs, results, &wg, namecache)
	}

	// Start walking the directory and sending file paths to workers
	go func() {
		defer close(jobs)

		info, err := p.client.Lstat(p.rootDir)
		if err != nil {
			results <- importer.NewScanError(p.rootDir, err)
			return
		}
		if info.Mode()&os.ModeSymlink != 0 {
			originFile, err := p.client.ReadLink(p.rootDir)
			if err != nil {
				results <- importer.NewScanError(p.rootDir, err)
				return
			}

			if !filepath.IsAbs(originFile) {
				originFile = filepath.Join(filepath.Dir(p.rootDir), originFile)
			}
			jobs <- p.rootDir
			p.rootDir = originFile
		}

		// Add prefix directories first
		p.walkDir_addPrefixDirectories(jobs, results)

		err = SFTPWalk(p.client, p.rootDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				results <- importer.NewScanError(path, err)
				return nil
			}
			jobs <- path
			return nil
		})
		if err != nil {
			results <- importer.NewScanError(p.rootDir, err)
		}
	}()

	// Close the results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	return results, nil
}

func SFTPWalk(client *sftp.Client, remotePath string, walkFn func(path string, info os.FileInfo, err error) error) error {
	info, err := client.Stat(remotePath)
	if err != nil {
		// If we can't stat the file, call walkFn with the error.
		return walkFn(remotePath, nil, err)
	}
	// Call the walk function for the current file/directory.
	if err := walkFn(remotePath, info, nil); err != nil {
		return err
	}
	// If it's not a directory, nothing more to do.
	if !info.IsDir() {
		return nil
	}
	// List the directory contents.
	entries, err := client.ReadDir(remotePath)
	if err != nil {
		return walkFn(remotePath, info, err)
	}
	// Recursively walk each entry.
	for _, entry := range entries {
		newPath := path.Join(remotePath, entry.Name()) // Use "path" since remote paths are POSIX style.
		if err := SFTPWalk(client, newPath, walkFn); err != nil {
			return err
		}
	}
	return nil
}
