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

package help

import (
	"embed"
	"flag"
	"fmt"
	"os"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/charmbracelet/glamour"
	"github.com/muesli/termenv"
)

//go:embed docs/*
var docs embed.FS

func init() {
	subcommands.Register("help", cmd_help)
}

func cmd_help(ctx *appcontext.AppContext, _ *repository.Repository, args []string) (int, error) {
	var opt_style string
	flags := flag.NewFlagSet("help", flag.ExitOnError)
	flags.StringVar(&opt_style, "style", "dracula", "style to use")
	flags.Parse(args)

	if len(flags.Args()) == 0 {
		fmt.Fprintf(os.Stderr, "available commands:\n")
		for _, command := range subcommands.List() {
			fmt.Fprintf(os.Stderr, "  %s\n", command)
		}
		return 0, nil
	}

	content, err := docs.ReadFile(fmt.Sprintf("docs/plakar-%s.md", flags.Args()[0]))
	if err != nil {
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", flags.Args()[0])
		return 1, err
	}

	r, err := glamour.NewTermRenderer(
		glamour.WithStandardStyle(opt_style),
		glamour.WithColorProfile(termenv.TrueColor),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create renderer: %s\n", err)
		return 1, err
	}

	out, err := r.RenderBytes(content)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to render: %s\n", err)
		return 1, err
	}
	fmt.Print(string(out))

	return 1, err
}
