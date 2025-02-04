//go:build go1.16
// +build go1.16

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

package ui

import (
	"flag"
	"fmt"
	"os"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/repository"
	v2 "github.com/PlakarKorp/plakar/ui/v2"
	"github.com/google/uuid"
)

func init() {
	subcommands.Register("ui", parse_cmd_ui)
}

func parse_cmd_ui(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var opt_addr string
	var opt_cors bool
	var opt_noauth bool
	var opt_nospawn bool

	flags := flag.NewFlagSet("ui", flag.ExitOnError)
	flags.StringVar(&opt_addr, "addr", "", "address to listen on")
	flags.BoolVar(&opt_cors, "cors", false, "enable CORS")
	flags.BoolVar(&opt_noauth, "no-auth", false, "don't use authentication")
	flags.BoolVar(&opt_nospawn, "no-spawn", false, "don't spawn browser")
	flags.Parse(args)

	return &Ui{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
		Addr:               opt_addr,
		Cors:               opt_cors,
		NoAuth:             opt_noauth,
		NoSpawn:            opt_nospawn,
	}, nil
}

type Ui struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Addr    string
	Cors    bool
	NoAuth  bool
	NoSpawn bool
}

func (cmd *Ui) Name() string {
	return "ui"
}

func (cmd *Ui) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	ui_opts := v2.UiOptions{
		NoSpawn: cmd.NoSpawn,
		Cors:    cmd.Cors,
		Token:   "",
	}

	if !cmd.NoAuth {
		ui_opts.Token = uuid.NewString()
	}

	err := v2.Ui(repo, cmd.Addr, &ui_opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ui: %s\n", err)
		return 1, err
	}
	return 0, err
}
