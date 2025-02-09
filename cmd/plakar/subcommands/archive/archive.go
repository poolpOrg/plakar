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

package archive

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
)

func init() {
	subcommands.Register("archive", parse_cmd_archive)
}

func parse_cmd_archive(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var opt_rebase bool
	var opt_output string
	var opt_format string

	flags := flag.NewFlagSet("archive", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s [OPTIONS] [SNAPSHOT[:PATH]]\n", flags.Name())
		fmt.Fprintf(flags.Output(), "\nOPTIONS:\n")
		flags.PrintDefaults()
	}

	flags.StringVar(&opt_output, "output", "", "archive pathname")
	flags.BoolVar(&opt_rebase, "rebase", false, "strip pathname when pulling")
	flags.StringVar(&opt_format, "format", "tarball", "archive format: tar, tarball, zip")
	flags.Parse(args)

	if flags.NArg() == 0 {
		return nil, fmt.Errorf("need at least one snapshot ID to pull")
	}

	supportedFormats := map[string]string{
		"tar":     ".tar",
		"tarball": ".tar.gz",
		"zip":     ".zip",
	}
	if _, ok := supportedFormats[opt_format]; !ok {
		return nil, fmt.Errorf("unsupported format %s", opt_format)
	}

	if opt_output == "" {
		opt_output = fmt.Sprintf("plakar-%s.%s", time.Now().UTC().Format(time.RFC3339), supportedFormats[opt_format])
	}

	return &Archive{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
		Rebase:             opt_rebase,
		Output:             opt_output,
		Format:             opt_format,
		SnapshotPrefix:     flags.Arg(0),
	}, nil
}

type Archive struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Rebase         bool
	Output         string
	Format         string
	SnapshotPrefix string
}

func (cmd *Archive) Name() string {
	return "archive"
}

func (cmd *Archive) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	snapshotPrefix, pathname := utils.ParseSnapshotID(cmd.SnapshotPrefix)
	snap, err := utils.OpenSnapshotByPrefix(repo, snapshotPrefix)
	if err != nil {
		return 1, fmt.Errorf("archive: could not open snapshot: %s", snapshotPrefix)
	}
	defer snap.Close()

	var out io.Writer
	if cmd.Output == "-" {
		out = ctx.Stdout
	} else {
		tmp, err := os.CreateTemp("", "plakar-archive-")
		if err != nil {
			return 1, fmt.Errorf("archive: %s: %w", pathname, err)
		}
		defer os.Remove(tmp.Name())
		out = tmp
	}

	if err = snap.Archive(out, cmd.Format, []string{pathname}, cmd.Rebase); err != nil {
		return 1, err
	}

	if outCloser, isCloser := out.(io.Closer); isCloser {
		if err := outCloser.Close(); err != nil {
			return 1, err
		}
	}

	if out, isFile := out.(*os.File); isFile {
		if err := os.Rename(out.Name(), cmd.Output); err != nil {
			return 1, err
		}
	}
	return 0, nil
}
