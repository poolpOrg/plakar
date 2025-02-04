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

package backup

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/dustin/go-humanize"
	"github.com/gobwas/glob"
)

func init() {
	subcommands.Register("backup", parse_cmd_backup)
}

type excludeFlags []string

func (e *excludeFlags) String() string {
	return strings.Join(*e, ",")
}

func (e *excludeFlags) Set(value string) error {
	*e = append(*e, value)
	return nil
}

func parse_cmd_backup(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var opt_tags string
	var opt_excludes string
	var opt_exclude excludeFlags
	var opt_concurrency uint64
	var opt_quiet bool
	// var opt_stdio bool

	excludes := []glob.Glob{}
	flags := flag.NewFlagSet("backup", flag.ExitOnError)
	flags.Uint64Var(&opt_concurrency, "concurrency", uint64(ctx.MaxConcurrency), "maximum number of parallel tasks")
	flags.StringVar(&opt_tags, "tag", "", "tag to assign to this snapshot")
	flags.StringVar(&opt_excludes, "excludes", "", "file containing a list of exclusions")
	flags.Var(&opt_exclude, "exclude", "file containing a list of exclusions")
	flags.BoolVar(&opt_quiet, "quiet", false, "suppress output")
	//flags.BoolVar(&opt_stdio, "stdio", false, "output one line per file to stdout instead of the default interactive output")
	flags.Parse(args)

	for _, item := range opt_exclude {
		excludes = append(excludes, glob.MustCompile(item))
	}

	if opt_excludes != "" {
		fp, err := os.Open(opt_excludes)
		if err != nil {
			ctx.GetLogger().Error("%s", err)
			return nil, err
		}
		defer fp.Close()

		scanner := bufio.NewScanner(fp)
		for scanner.Scan() {
			pattern, err := glob.Compile(scanner.Text())
			if err != nil {
				ctx.GetLogger().Error("%s", err)
				return nil, err
			}
			excludes = append(excludes, pattern)
		}
		if err := scanner.Err(); err != nil {
			ctx.GetLogger().Error("%s", err)
			return nil, err
		}
	}
	return &Backup{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
		Concurrency:        opt_concurrency,
		Tags:               opt_tags,
		Excludes:           excludes,
		Exclude:            opt_exclude,
		Quiet:              opt_quiet,
		Path:               flags.Arg(0),
	}, nil
}

type Backup struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Concurrency uint64
	Tags        string
	Excludes    []glob.Glob
	Exclude     []string
	Quiet       bool
	Path        string
}

func (cmd *Backup) Name() string {
	return "backup"
}

func (cmd *Backup) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	snap, err := snapshot.New(repo)
	if err != nil {
		ctx.GetLogger().Error("%s", err)
		return 1, err
	}
	defer snap.Close()

	var tags []string
	if cmd.Tags == "" {
		tags = []string{}
	} else {
		tags = []string{cmd.Tags}
	}

	opts := &snapshot.BackupOptions{
		MaxConcurrency: cmd.Concurrency,
		Name:           "default",
		Tags:           tags,
		Excludes:       cmd.Excludes,
	}

	scanDir := ctx.CWD
	if cmd.Path != "" {
		scanDir = cmd.Path
	}

	imp, err := importer.NewImporter(scanDir)
	if err != nil {
		if !filepath.IsAbs(scanDir) {
			scanDir = filepath.Join(ctx.CWD, scanDir)
		}
		imp, err = importer.NewImporter("fs://" + scanDir)
		if err != nil {
			log.Fatalf("failed to create an import for %s: %s", scanDir, err)
		}
	}

	ep := startEventsProcessor(ctx, imp.Root(), true, cmd.Quiet)
	if err := snap.Backup(scanDir, imp, opts); err != nil {
		ep.Close()
		return 1, fmt.Errorf("failed to create snapshot: %w", err)
	}
	ep.Close()

	ctx.GetLogger().Info("created %s snapshot %x with root %s of size %s in %s",
		"unsigned",
		snap.Header.GetIndexShortID(),
		base64.RawStdEncoding.EncodeToString(snap.Header.GetSource(0).VFS[:]),
		humanize.Bytes(snap.Header.GetSource(0).Summary.Directory.Size+snap.Header.GetSource(0).Summary.Below.Size),
		snap.Header.Duration)
	return 0, nil
}
