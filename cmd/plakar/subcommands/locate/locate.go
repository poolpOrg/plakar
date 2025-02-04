/*
 * Copyright (c) 2025 Gilles Chehade <gilles@poolp.org>
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

package locate

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
)

func init() {
	subcommands.Register("locate", parse_cmd_locate)
}

func parse_cmd_locate(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var opt_snapshot string

	flags := flag.NewFlagSet("locate", flag.ExitOnError)
	flags.StringVar(&opt_snapshot, "snapshot", "", "snapshot to locate in")
	flags.Parse(args)

	return &Locate{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
		Snapshot:           opt_snapshot,
		Patterns:           flags.Args(),
	}, nil
}

type Locate struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Snapshot string
	Patterns []string
}

func (cmd *Locate) Name() string {
	return "locate"
}

func (cmd *Locate) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	var snapshotIDs []objects.Checksum
	if cmd.Snapshot != "" {
		snapshotIDs = utils.LookupSnapshotByPrefix(repo, cmd.Snapshot)
	} else {
		var err error
		snapshotIDs, err = repo.GetSnapshots()
		if err != nil {
			ctx.GetLogger().Error("...")
			return 1, err
		}
	}

	for _, snapshotID := range snapshotIDs {
		snap, err := snapshot.Load(repo, snapshotID)
		if err != nil {
			ctx.GetLogger().Error("locate: could not get snapshot: %s", err)
			return 1, err
		}

		fs, err := snap.Filesystem()
		if err != nil {
			ctx.GetLogger().Error("locate: could not get filesystem: %s", err)
			snap.Close()
			return 1, err
		}
		for pathname, err := range fs.Pathnames() {
			if err != nil {
				ctx.GetLogger().Error("locate: could not get pathname: %s", err)
				snap.Close()
				return 1, err
			}

			for _, pattern := range cmd.Patterns {
				matched := false
				if path.Base(pathname) == pattern {
					matched = true
				}
				if !matched {
					matched, err := path.Match(pattern, path.Base(pathname))
					if err != nil {
						ctx.GetLogger().Error("locate: could not match pattern: %s", err)
						snap.Close()
						return 1, err
					}
					if !matched {
						continue
					}
				}
				fmt.Fprintf(os.Stdout, "%x:%s\n", snap.Header.Identifier[0:4], pathname)
			}
		}
		snap.Close()
	}
	return 0, nil
}
