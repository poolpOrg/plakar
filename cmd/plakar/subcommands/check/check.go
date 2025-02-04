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

package check

import (
	"flag"
	"fmt"
	"log"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/google/uuid"
)

func init() {
	subcommands.Register("check", parse_cmd_check)
}

func parse_cmd_check(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var opt_concurrency uint64
	var opt_fastCheck bool
	var opt_noVerify bool
	var opt_quiet bool

	flags := flag.NewFlagSet("check", flag.ExitOnError)
	flags.Uint64Var(&opt_concurrency, "concurrency", uint64(ctx.MaxConcurrency), "maximum number of parallel tasks")
	flags.BoolVar(&opt_noVerify, "no-verify", false, "disable signature verification")
	flags.BoolVar(&opt_fastCheck, "fast", false, "enable fast checking (no checksum verification)")
	flags.BoolVar(&opt_quiet, "quiet", false, "suppress output")
	flags.Parse(args)

	return &Check{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
		Concurrency:        opt_concurrency,
		FastCheck:          opt_fastCheck,
		NoVerify:           opt_noVerify,
		Quiet:              opt_quiet,
		Snapshots:          flags.Args(),
	}, nil
}

type Check struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Concurrency uint64
	FastCheck   bool
	NoVerify    bool
	Quiet       bool
	Snapshots   []string
}

func (cmd *Check) Name() string {
	return "check"
}

func (cmd *Check) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	go eventsProcessorStdio(ctx, cmd.Quiet)

	var snapshots []string
	if len(cmd.Snapshots) == 0 {
		for snapshotID := range repo.ListSnapshots() {
			snapshots = append(snapshots, fmt.Sprintf("%x", snapshotID))
		}
	} else {
		snapshots = cmd.Snapshots
	}

	opts := &snapshot.CheckOptions{
		MaxConcurrency: cmd.Concurrency,
		FastCheck:      cmd.FastCheck,
	}

	failures := false
	for _, arg := range snapshots {
		snapshotPrefix, pathname := utils.ParseSnapshotID(arg)
		snap, err := utils.OpenSnapshotByPrefix(repo, snapshotPrefix)
		if err != nil {
			log.Fatal(err)
		}

		if !cmd.NoVerify && snap.Header.Identity.Identifier != uuid.Nil {
			if ok, err := snap.Verify(); err != nil {
				ctx.GetLogger().Warn("%s", err)
			} else if !ok {
				ctx.GetLogger().Info("snapshot %x signature verification failed", snap.Header.Identifier)
				failures = true
			} else {
				ctx.GetLogger().Info("snapshot %x signature verification succeeded", snap.Header.Identifier)
			}
		}

		if ok, err := snap.Check(pathname, opts); err != nil {
			ctx.GetLogger().Warn("%s", err)
		} else if !ok {
			failures = true
		}

		snap.Close()
	}

	if failures {
		return 1, fmt.Errorf("check failed")
	}
	return 0, nil
}
