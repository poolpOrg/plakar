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

package restore

import (
	"flag"
	"fmt"
	"strings"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/exporter"
)

func init() {
	subcommands.Register("restore", parse_cmd_restore)
}

func parse_cmd_restore(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var pullPath string
	var pullRebase bool
	var opt_concurrency uint64
	var opt_quiet bool

	flags := flag.NewFlagSet("restore", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s [OPTIONS] [SNAPSHOT[:PATH]]...\n", flags.Name())
		fmt.Fprintf(flags.Output(), "\nOPTIONS:\n")
		flags.PrintDefaults()
	}

	flags.Uint64Var(&opt_concurrency, "concurrency", uint64(ctx.MaxConcurrency), "maximum number of parallel tasks")
	flags.StringVar(&pullPath, "to", ctx.CWD, "base directory where pull will restore")
	flags.BoolVar(&pullRebase, "rebase", false, "strip pathname when pulling")
	flags.BoolVar(&opt_quiet, "quiet", false, "do not print progress")
	flags.Parse(args)

	return &Restore{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
		Path:               pullPath,
		Rebase:             pullRebase,
		Concurrency:        opt_concurrency,
		Quiet:              opt_quiet,
		Snapshots:          flags.Args(),
	}, nil
}

type Restore struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Path        string
	Rebase      bool
	Concurrency uint64
	Quiet       bool
	Snapshots   []string
}

func (cmd *Restore) Name() string {
	return "restore"
}

func (cmd *Restore) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	go eventsProcessorStdio(ctx, cmd.Quiet)

	var exporterInstance exporter.Exporter
	var err error
	exporterInstance, err = exporter.NewExporter(cmd.Path)
	if err != nil {
		return 1, err
	}
	defer exporterInstance.Close()

	opts := &snapshot.RestoreOptions{
		MaxConcurrency: cmd.Concurrency,
		Rebase:         cmd.Rebase,
	}

	if len(cmd.Snapshots) == 0 {
		metadatas, err := utils.GetHeaders(repo, nil)
		if err != nil {
			return 1, err
		}

		for i := len(metadatas); i != 0; i-- {
			metadata := metadatas[i-1]
			if ctx.CWD == metadata.GetSource(0).Importer.Directory || strings.HasPrefix(ctx.CWD, fmt.Sprintf("%s/", metadata.GetSource(0).Importer.Directory)) {
				snap, err := snapshot.Load(repo, metadata.GetIndexID())
				if err != nil {
					return 1, err
				}
				snap.Restore(exporterInstance, ctx.CWD, ctx.CWD, opts)
				snap.Close()
				return 0, nil
			}
		}
		return 1, fmt.Errorf("could not find a snapshot to restore this path from")
	}

	snapshots, err := utils.GetSnapshots(repo, cmd.Snapshots)
	if err != nil {
		return 1, err
	}

	for offset, snap := range snapshots {
		_, pattern := utils.ParseSnapshotID(cmd.Snapshots[offset])
		snap.Restore(exporterInstance, exporterInstance.Root(), pattern, opts)
		snap.Close()
	}
	return 0, nil
}
