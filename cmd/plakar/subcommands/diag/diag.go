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

package diag

import (
	"flag"
	"fmt"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/repository"
)

func init() {
	subcommands.Register("diag", parse_cmd_diag)
}

func parse_cmd_diag(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	if len(args) == 0 {
		return &DiagRepository{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
		}, nil
	}

	flags := flag.NewFlagSet("diag", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s\n", flags.Name())
		fmt.Fprintf(flags.Output(), "       %s snapshot SNAPSHOT\n", flags.Name())
		fmt.Fprintf(flags.Output(), "       %s errors SNAPSHOT\n", flags.Name())
		fmt.Fprintf(flags.Output(), "       %s state [STATE]...\n", flags.Name())
		fmt.Fprintf(flags.Output(), "       %s packfile [PACKFILE]...\n", flags.Name())
		fmt.Fprintf(flags.Output(), "       %s object [OBJECT]\n", flags.Name())
		fmt.Fprintf(flags.Output(), "       %s vfs SNAPSHOT[:PATH]\n", flags.Name())
	}
	flags.Parse(args)

	// Determine which concept to show information for based on flags.Args()[0]
	switch flags.Arg(0) {
	case "snapshot":
		if len(flags.Args()) < 2 {
			return nil, fmt.Errorf("usage: %s snapshot SNAPSHOT", flags.Name())
		}
		return &DiagSnapshot{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			SnapshotID:         flags.Args()[1],
		}, nil
	case "errors":
		if len(flags.Args()) < 2 {
			return nil, fmt.Errorf("usage: %s errors SNAPSHOT", flags.Name())
		}
		return &DiagErrors{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			SnapshotID:         flags.Args()[1],
		}, nil
	case "state":
		return &DiagState{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			Args:               flags.Args()[1:],
		}, nil
	case "packfile":
		return &DiagPackfile{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			Args:               flags.Args()[1:],
		}, nil
	case "object":
		if len(flags.Args()) < 2 {
			return nil, fmt.Errorf("usage: %s object OBJECT", flags.Name())
		}
		return &DiagObject{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			ObjectID:           flags.Args()[1],
		}, nil
	case "vfs":
		if len(flags.Args()) < 2 {
			return nil, fmt.Errorf("usage: %s vfs SNAPSHOT[:PATH]", flags.Name())
		}
		return &DiagVFS{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			SnapshotPath:       flags.Args()[1],
		}, nil
	case "xattr":
		if len(flags.Args()) < 2 {
			return nil, fmt.Errorf("usage: %s xattr SNAPSHOT[:PATH]", flags.Name())
		}
		return &DiagXattr{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			SnapshotPath:       flags.Args()[1],
		}, nil
	case "contenttype":
		if len(flags.Args()) < 2 {
			return nil, fmt.Errorf("usage: %s contettype SNAPSHOT[:PATH]", flags.Name())
		}
		return &DiagContentType{
			RepositoryLocation: repo.Location(),
			RepositorySecret:   ctx.GetSecret(),
			SnapshotPath:       flags.Args()[1],
		}, nil
	}
	return nil, fmt.Errorf("Invalid parameter. usage: diag [contenttype|snapshot|object|state|packfile|vfs|xattr|errors]")
}
