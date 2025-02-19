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

package rm

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
)

func init() {
	subcommands.Register("rm", parse_cmd_rm)
}

func parse_cmd_rm(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var opt_name string
	var opt_category string
	var opt_environment string
	var opt_perimeter string
	var opt_job string
	var opt_tag string
	var opt_before string
	var opt_since string
	var opt_latest bool

	flags := flag.NewFlagSet("rm", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s [OPTIONS] SNAPSHOT...\n", flags.Name())
		fmt.Fprintf(flags.Output(), "\nOPTIONS:\n")
		flags.PrintDefaults()
	}

	flags.StringVar(&opt_name, "name", "", "filter by name")
	flags.StringVar(&opt_category, "category", "", "filter by category")
	flags.StringVar(&opt_environment, "environment", "", "filter by environment")
	flags.StringVar(&opt_perimeter, "perimeter", "", "filter by perimeter")
	flags.StringVar(&opt_job, "job", "", "filter by job")
	flags.StringVar(&opt_tag, "tag", "", "filter by tag")
	flags.StringVar(&opt_before, "before", "", "filter by date")
	flags.StringVar(&opt_since, "since", "", "filter by date")
	flags.BoolVar(&opt_latest, "latest", false, "use latest snapshot")
	flags.Parse(args)

	var err error

	var beforeDate time.Time
	if opt_before != "" {
		beforeDate, err = utils.ParseTimeFlag(opt_before)
		if err != nil {
			return nil, fmt.Errorf("invalid date format: %s", opt_before)
		}
	}

	var sinceDate time.Time
	if opt_since != "" {
		sinceDate, err = utils.ParseTimeFlag(opt_since)
		if err != nil {
			return nil, fmt.Errorf("invalid date format: %s", opt_since)
		}
	}

	if flags.NArg() != 0 {
		if opt_name != "" || opt_category != "" || opt_environment != "" || opt_perimeter != "" || opt_job != "" || opt_tag != "" || !beforeDate.IsZero() || !sinceDate.IsZero() || opt_latest {
			ctx.GetLogger().Warn("snapshot specified, filters will be ignored")
		}
	} else {
		if opt_name == "" && opt_category == "" && opt_environment == "" && opt_perimeter == "" && opt_job == "" && opt_tag == "" && beforeDate.IsZero() && sinceDate.IsZero() && !opt_latest {
			return nil, fmt.Errorf("no filter specified, not going to remove everything")
		}
	}

	return &Rm{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),

		OptBefore: beforeDate,
		OptSince:  sinceDate,
		OptLatest: opt_latest,

		OptName:        opt_name,
		OptCategory:    opt_category,
		OptEnvironment: opt_environment,
		OptPerimeter:   opt_perimeter,
		OptJob:         opt_job,
		OptTag:         opt_tag,

		Snapshots: flags.Args(),
	}, nil
}

type Rm struct {
	RepositoryLocation string
	RepositorySecret   []byte

	OptBefore time.Time
	OptSince  time.Time
	OptLatest bool

	OptName        string
	OptCategory    string
	OptEnvironment string
	OptPerimeter   string
	OptJob         string
	OptTag         string

	Snapshots []string
}

func (cmd *Rm) Name() string {
	return "rm"
}

func (cmd *Rm) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	var snapshots []objects.MAC
	if len(cmd.Snapshots) == 0 {
		locateOptions := utils.NewDefaultLocateOptions()
		locateOptions.MaxConcurrency = ctx.MaxConcurrency
		locateOptions.SortOrder = utils.LocateSortOrderAscending

		locateOptions.Before = cmd.OptBefore
		locateOptions.Since = cmd.OptSince
		locateOptions.Latest = cmd.OptLatest

		locateOptions.Name = cmd.OptName
		locateOptions.Category = cmd.OptCategory
		locateOptions.Environment = cmd.OptEnvironment
		locateOptions.Perimeter = cmd.OptPerimeter
		locateOptions.Job = cmd.OptJob
		locateOptions.Tag = cmd.OptTag

		snapshotIDs, err := utils.LocateSnapshotIDs(repo, locateOptions)
		if err != nil {
			return 1, err
		}
		snapshots = append(snapshots, snapshotIDs...)
	} else {
		for _, prefix := range cmd.Snapshots {
			snapshotID, err := utils.LocateSnapshotByPrefix(repo, prefix)
			if err != nil {
				continue
			}
			snapshots = append(snapshots, snapshotID)
		}
	}

	errors := 0
	wg := sync.WaitGroup{}
	for _, snap := range snapshots {
		wg.Add(1)
		go func(snapshotID objects.MAC) {
			err := repo.DeleteSnapshot(snapshotID)
			if err != nil {
				ctx.GetLogger().Error("%s", err)
				errors++
			}
			ctx.GetLogger().Info("%s: removal of %x completed successfully",
				cmd.Name(),
				snapshotID[:4])
			wg.Done()
		}(snap)
	}
	wg.Wait()

	if errors != 0 {
		return 1, fmt.Errorf("failed to remove %d snapshots", errors)
	}

	return 0, nil
}
