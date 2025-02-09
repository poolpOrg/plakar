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

package diff

import (
	"flag"
	"fmt"
	"io"
	"path"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/alecthomas/chroma/quick"
	"github.com/pmezard/go-difflib/difflib"
)

func init() {
	subcommands.Register("diff", parse_cmd_diff)
}

func parse_cmd_diff(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var opt_highlight bool
	flags := flag.NewFlagSet("diff", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s [OPTIONS] SNAPSHOT:PATH SNAPSHOT[:PATH]\n", flags.Name())
		fmt.Fprintf(flags.Output(), "\nOPTIONS:\n")
		flags.PrintDefaults()
	}

	flags.BoolVar(&opt_highlight, "highlight", false, "highlight output")
	flags.Parse(args)

	if flags.NArg() != 2 {
		return nil, fmt.Errorf("needs two snapshot ID and/or snapshot files to diff")
	}

	snapshotPrefix1, pathname1 := utils.ParseSnapshotID(flags.Arg(0))
	snapshotPrefix2, pathname2 := utils.ParseSnapshotID(flags.Arg(1))

	return &Diff{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
		Highlight:          opt_highlight,
		SnapshotPrefix1:    snapshotPrefix1,
		Pathname1:          pathname1,
		SnapshotPrefix2:    snapshotPrefix2,
		Pathname2:          pathname2,
	}, nil
}

type Diff struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Highlight       bool
	SnapshotPrefix1 string
	Pathname1       string
	SnapshotPrefix2 string
	Pathname2       string
}

func (cmd *Diff) Name() string {
	return "diff"
}

func (cmd *Diff) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	pathname1 := cmd.Pathname1
	pathname2 := cmd.Pathname2

	snap1, err := utils.OpenSnapshotByPrefix(repo, cmd.SnapshotPrefix1)
	if err != nil {
		return 1, fmt.Errorf("diff: could not open snapshot: %s", cmd.SnapshotPrefix1)
	}
	defer snap1.Close()

	snap2, err := utils.OpenSnapshotByPrefix(repo, cmd.SnapshotPrefix2)
	if err != nil {
		return 1, fmt.Errorf("diff: could not open snapshot: %s", cmd.SnapshotPrefix2)
	}
	defer snap2.Close()

	var diff string
	if pathname1 == "" && pathname2 == "" {
		diff, err = diff_filesystems(ctx, snap1, snap2)
		if err != nil {
			return 1, fmt.Errorf("diff: could not diff snapshots: %w", err)
		}
	} else {
		if pathname1 == "" {
			pathname1 = pathname2
		}
		if pathname2 == "" {
			pathname2 = pathname1
		}
		diff, err = diff_pathnames(ctx, snap1, pathname1, snap2, pathname2)
		if err != nil {
			return 1, fmt.Errorf("diff: could not diff pathnames: %w", err)
		}
	}

	if cmd.Highlight {
		err = quick.Highlight(ctx.Stdout, diff, "diff", "terminal", "dracula")
		if err != nil {
			return 1, fmt.Errorf("diff: could not highlight diff: %w", err)
		}
	} else {
		fmt.Fprintf(ctx.Stdout, "%s", diff)
	}
	return 0, nil
}

func diff_filesystems(ctx *appcontext.AppContext, snap1 *snapshot.Snapshot, snap2 *snapshot.Snapshot) (string, error) {
	vfs1, err := snap1.Filesystem()
	if err != nil {
		return "", err
	}

	vfs2, err := snap2.Filesystem()
	if err != nil {
		return "", err
	}

	var f1, f2 *vfs.Entry
	if f1, err = vfs1.GetEntry("/"); err != nil {
		return "", err
	}
	if f2, err = vfs2.GetEntry("/"); err != nil {
		return "", err
	}

	return diff_directories(ctx, f1, f2)
}

func diff_pathnames(ctx *appcontext.AppContext, snap1 *snapshot.Snapshot, pathname1 string, snap2 *snapshot.Snapshot, pathname2 string) (string, error) {
	vfs1, err := snap1.Filesystem()
	if err != nil {
		return "", err
	}

	vfs2, err := snap2.Filesystem()
	if err != nil {
		return "", err
	}

	var f1, f2 *vfs.Entry
	if f1, err = vfs1.GetEntry(pathname1); err != nil {
		return "", err
	}
	if f2, err = vfs2.GetEntry(pathname2); err != nil {
		return "", err
	}

	if f1.Stat().IsDir() && f2.Stat().IsDir() {
		return diff_directories(ctx, f1, f2)
	}

	if f1.Stat().IsDir() || f2.Stat().IsDir() {
		return "", fmt.Errorf("can't diff different file types")
	}

	return diff_files(ctx, snap1, f1, snap2, f2)
}

func diff_directories(ctx *appcontext.AppContext, dirEntry1 *vfs.Entry, dirEntry2 *vfs.Entry) (string, error) {
	return "", fmt.Errorf("not implemented yet")
}

func diff_files(ctx *appcontext.AppContext, snap1 *snapshot.Snapshot, fileEntry1 *vfs.Entry, snap2 *snapshot.Snapshot, fileEntry2 *vfs.Entry) (string, error) {
	if fileEntry1.Object == fileEntry2.Object {
		fmt.Fprintf(ctx.Stderr, "%s:%s and %s:%s are identical\n",
			fmt.Sprintf("%x", snap1.Header.GetIndexShortID()), path.Join(fileEntry1.ParentPath, fileEntry1.Stat().Name()),
			fmt.Sprintf("%x", snap2.Header.GetIndexShortID()), path.Join(fileEntry2.ParentPath, fileEntry2.Stat().Name()))
		return "", nil
	}

	filename1 := path.Join(fileEntry1.ParentPath, fileEntry1.Stat().Name())
	filename2 := path.Join(fileEntry2.ParentPath, fileEntry2.Stat().Name())

	buf1 := make([]byte, 0)
	rd1, err := snap1.NewReader(filename1)
	if err == nil {
		buf1, err = io.ReadAll(rd1)
		if err != nil {
			return "", err
		}
	}

	buf2 := make([]byte, 0)
	rd2, err := snap2.NewReader(filename2)
	if err == nil {
		buf2, err = io.ReadAll(rd2)
		if err != nil {
			return "", err
		}
	}

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(buf1)),
		B:        difflib.SplitLines(string(buf2)),
		FromFile: fmt.Sprintf("%x", snap1.Header.GetIndexShortID()) + ":" + filename1,
		ToFile:   fmt.Sprintf("%x", snap2.Header.GetIndexShortID()) + ":" + filename2,
		Context:  3,
	}
	text, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return "", err
	}
	return text, nil
}
