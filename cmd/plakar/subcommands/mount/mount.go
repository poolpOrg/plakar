//go:build linux || darwin
// +build linux darwin

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

package mount

import (
	"flag"
	"fmt"
	"log"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/plakarfs"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"
)

func init() {
	subcommands.Register("mount", parse_cmd_mount)
}

func parse_cmd_mount(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	flags := flag.NewFlagSet("mount", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() != 1 {
		ctx.GetLogger().Error("need mountpoint")
		return nil, fmt.Errorf("need mountpoint")
	}
	return &Mount{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
		Mountpoint:         flags.Arg(0),
	}, nil
}

type Mount struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Mountpoint string
}

func (cmd *Mount) Name() string {
	return "mount"
}

func (cmd *Mount) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	c, err := fuse.Mount(
		cmd.Mountpoint,
		fuse.FSName("plakar"),
		fuse.Subtype("plakarfs"),
		fuse.LocalVolume(),
	)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}
	defer c.Close()
	ctx.GetLogger().Info("mounted repository %s at %s", repo.Location(), cmd.Mountpoint)

	err = fs.Serve(c, plakarfs.NewFS(repo, cmd.Mountpoint))
	if err != nil {
		return 1, err
	}
	<-c.Ready
	if err := c.MountError; err != nil {
		return 1, err
	}
	return 0, nil

}
