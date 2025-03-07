package snapshot

import (
	"iter"
	"path"
	"strings"

	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

type SearchOpts struct {
	// file filters
	Recursive bool
	Prefix    string // prefix directory
	Mime      string

	// pagination
	Offset int
	Limit  int
}

func matchmime(match, target string) bool {
	if match == "" {
		return true
	}

	m := strings.SplitN(match, "/", 2)
	t := strings.SplitN(target, "/", 2)

	if m[0] != t[0] {
		return false
	}
	if len(m) == 2 && len(t) == 2 {
		return m[1] == t[1]
	}
	return true
}

func visitmimes(snap *Snapshot, opts *SearchOpts) (iter.Seq2[*vfs.Entry, error], error) {
	idx, err := snap.ContentTypeIdx()
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, nil
	}

	fsp, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}

	prefix := "/" + opts.Mime
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// XXX: to further optimize this routine we'd need a ScanGlob
	// method.  Basically what we're trying to do here is to
	// resolve globs like "application/*/prefix/*"
	it, err := idx.ScanFrom(prefix)
	if err != nil {
		return nil, err
	}

	return func(yield func(*vfs.Entry, error) bool) {
		for it.Next() {
			p, mac := it.Current()

			if !strings.HasPrefix(p, prefix) {
				break
			}

			entry, err := fsp.ResolveEntry(mac)
			if err != nil {
				yield(nil, err)
				return
			}
			path := entry.Path()
			if !strings.HasPrefix(path, opts.Prefix) {
				continue
			}
			if !yield(entry, nil) {
				return
			}
		}
		if err := it.Err(); err != nil {
			yield(nil, err)
		}
	}, nil
}

func visitfiles(snap *Snapshot, opts *SearchOpts) (iter.Seq2[*vfs.Entry, error], error) {
	if opts.Recursive && opts.Mime != "" {
		it, err := visitmimes(snap, opts)
		if it != nil || err != nil {
			return it, err
		}
		// fallback
	}

	fsc, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}

	var it iter.Seq2[*vfs.Entry, error]
	if opts.Recursive {
		it = fsc.Files(opts.Prefix)
	} else {
		it, err = fsc.Children(opts.Prefix)
		if err != nil {
			return nil, err
		}
	}

	return func(yield func(*vfs.Entry, error) bool) {
		for entry, err := range it {
			if err != nil {
				yield(nil, err)
				return
			}

			if !matchmime(opts.Mime, entry.ContentType()) {
				continue
			}

			if !yield(entry, nil) {
				return
			}
		}
	}, nil
}

func (snap *Snapshot) Search(opts *SearchOpts) (iter.Seq2[*vfs.Entry, error], error) {
	if opts.Prefix != "" {
		opts.Prefix = path.Clean(opts.Prefix)
		if !strings.HasSuffix(opts.Prefix, "/") {
			opts.Prefix += "/"
		}
	}

	it, err := visitfiles(snap, opts)
	if err != nil {
		return nil, err
	}

	var n, m int
	return func(yield func(*vfs.Entry, error) bool) {
		for entry, err := range it {
			if err != nil {
				yield(nil, err)
				return
			}

			// eventually other filters on entry, e.g. size or pattern

			if n++; opts.Offset != 0 && n <= opts.Offset {
				continue
			}
			if m++; opts.Limit != 0 && m > opts.Limit {
				return
			}
			if !yield(entry, nil) {
				return
			}
		}
	}, nil
}
