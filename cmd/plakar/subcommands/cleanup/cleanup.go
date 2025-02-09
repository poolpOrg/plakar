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

package cleanup

import (
	"flag"
	"fmt"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/repository"
)

func init() {
	subcommands.Register("cleanup", parse_cmd_cleanup)
}

func parse_cmd_cleanup(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	flags := flag.NewFlagSet("cleanup", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s\n", flags.Name())
	}
	flags.Parse(args)

	return &Cleanup{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
	}, nil
}

type Cleanup struct {
	RepositoryLocation string
	RepositorySecret   []byte
}

func (cmd *Cleanup) Name() string {
	return "cleanup"
}

func (cmd *Cleanup) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	// the cleanup algorithm is a bit tricky and needs to be done in the correct sequence,
	// here's what it has to do:
	//
	// 1. fetch all snapshot indexes to figure out which blobs, objects and chunks are used
	// 2. blobs that are no longer in use can be be removed
	// 3. for each object and chunk, track which packfiles contain them
	// 4. if objects or chunks are present in more than one packfile...
	// 5. decide which one keeps it and a new packfile has to be generated for the other that contains everything BUT the object/chunk
	// 6. update indexes to reflect the new packfile
	// 7. save the new index
	return 0, nil
}
