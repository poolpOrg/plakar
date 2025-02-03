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

package server

import (
	"flag"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/server/httpd"
)

func init() {
	subcommands.Register("server", cmd_server)
}

func cmd_server(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (int, error) {
	var opt_listen string
	var opt_allowdelete bool

	flags := flag.NewFlagSet("server", flag.ExitOnError)
	flags.StringVar(&opt_listen, "listen", "127.0.0.1:9876", "address to listen on")
	flags.BoolVar(&opt_allowdelete, "allow-delete", false, "disable delete operations")
	flags.Parse(args)

	noDelete := true
	if opt_allowdelete {
		noDelete = false
	}

	httpd.Server(repo, opt_listen, noDelete)
	return 0, nil
}
