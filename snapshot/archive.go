package snapshot

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

type ArchiveFormat = string

const (
	ArchiveTar     ArchiveFormat = "tar"
	ArchiveTarball               = "tarball"
	ArchiveZip                   = "zip"
)

var (
	ErrInvalidArchiveFormat = errors.New("unknown archive format")
	ErrNotADirectory = errors.New("is not a directory")
)

func (snap *Snapshot) Archive(w io.Writer, format ArchiveFormat, paths []string, rebase bool) error {
	fsc, err := snap.Filesystem()
	if err != nil {
		return err
	}

	var outw io.Closer
	var archiveEntry func(string, *vfs.Entry) (io.Writer, error)
	switch format {
	case ArchiveTarball:
		// wrap the outer writer with gzip
		gzipWriter := gzip.NewWriter(w)
		defer gzipWriter.Close()
		w = gzipWriter
		fallthrough
	case ArchiveTar:
		tarWriter := tar.NewWriter(w)
		outw = tarWriter
		archiveEntry = func(path string, entry *vfs.Entry) (io.Writer, error) {
			// ignore symlinks
			if entry.FileInfo.Mode()&fs.ModeSymlink != 0 {
				return nil, nil
			}
			header, err := tar.FileInfoHeader(entry.Stat(), "")
			if err != nil {
				return nil, err
			}
			header.Name = path
			if err := tarWriter.WriteHeader(header); err != nil {
				return nil, err
			}
			if !entry.FileInfo.Lmode.IsRegular() {
				return nil, nil
			}
			return tarWriter, nil
		}

	case ArchiveZip:
		zipWriter := zip.NewWriter(w)
		outw = zipWriter
		archiveEntry = func(path string, entry *vfs.Entry) (io.Writer, error) {
			if !entry.FileInfo.Lmode.IsRegular() {
				return nil, nil
			}
			header, err := zip.FileInfoHeader(entry.Stat())
			if err != nil {
				return nil, err
			}
			header.Name = path
			header.Method = zip.Deflate
			return zipWriter.CreateHeader(header)
		}

	default:
		return ErrInvalidArchiveFormat
	}

	for _, p := range paths {
		err := fsc.WalkDir(p, func(entrypath string, e *vfs.Entry, err error) error {
			if err != nil {
				return err
			}

			outpath := entrypath
			if rebase {
				outpath = strings.TrimPrefix(outpath, p)
			}
			outpath = strings.TrimLeft(outpath, "/")
			if outpath == "" {
				if e.IsDir() {
					outpath = "."
				} else {
					outpath = path.Base(entrypath)
				}
			}

			writer, err := archiveEntry(outpath, e)
			if err != nil {
				return fmt.Errorf("Failed to archive %s: %w", entrypath, err)
			}
			if writer == nil {
				return nil
			}

			fp := e.Open(fsc, e.Path())
			_, err = io.Copy(writer, fp)
			fp.Close()
			return err
		})
		if err != nil {
			return err
		}
	}

	return outw.Close()
}
