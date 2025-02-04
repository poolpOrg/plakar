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

package info

import (
	"flag"
	"fmt"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/repository"
)

func init() {
	subcommands.Register("info", parse_cmd_info)
}

func parse_cmd_info(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	if len(args) == 0 {
		return &InfoRepository{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
		}, nil
	}

	flags := flag.NewFlagSet("info", flag.ExitOnError)
	_ = flags.Parse(args)

	// Determine which concept to show information for based on flags.Args()[0]
	switch flags.Arg(0) {
	case "snapshot":
		if len(flags.Args()) < 2 {
			return nil, fmt.Errorf("usage: %s snapshot snapshotID", flags.Name())
		}
		return &InfoSnapshot{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			SnapshotID:         flags.Args()[1],
		}, nil
	case "errors":
		if len(flags.Args()) < 2 {
			return nil, fmt.Errorf("usage: %s errors snapshotID", flags.Name())
		}
		return &InfoErrors{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			SnapshotID:         flags.Args()[1],
		}, nil
	case "state":
		return &InfoState{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			Args:               flags.Args()[1:],
		}, nil
	case "packfile":
		return &InfoPackfile{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			Args:               flags.Args()[1:],
		}, nil
	case "object":
		if len(flags.Args()) < 2 {
			return nil, fmt.Errorf("usage: %s object objectID", flags.Name())
		}
		return &InfoObject{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			ObjectID:           flags.Args()[1],
		}, nil
	case "vfs":
		if len(flags.Args()) < 2 {
			return nil, fmt.Errorf("usage: %s vfs snapshotPathname", flags.Name())
		}
		return &InfoVFS{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			SnapshotPath:       flags.Args()[1],
		}, nil
	}
	return nil, fmt.Errorf("Invalid parameter. usage: info [snapshot|object|state|packfile|vfs|errors]")
}
