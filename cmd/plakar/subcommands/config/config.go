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

package version

import (
	"flag"
	"fmt"
	"os"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/repository"
)

func init() {
	subcommands.Register("config", parse_cmd_config)
}

func parse_cmd_config(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	flags := flag.NewFlagSet("config", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s\n", flags.Name())
		flags.PrintDefaults()
	}

	flags.Parse(args)
	return &Config{
		args: flags.Args(),
	}, nil
}

type Config struct {
	args []string
}

func (cmd *Config) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	if len(cmd.args) == 0 {
		ctx.Config.Render(os.Stdout)
		return 0, nil
	}

	var err error
	switch cmd.args[0] {
	case "remote":
		err = cmd_remote(ctx, repo, cmd.args[1:])
	case "repository", "repo":
		err = cmd_repository(ctx, repo, cmd.args[1:])
	default:
		err = fmt.Errorf("unknown subcommand %s", cmd.args[0])
	}

	if err != nil {
		return 1, err
	}
	return 0, nil
}

func cmd_remote(ctx *appcontext.AppContext, repo *repository.Repository, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: plakar config remote [create | default | set | validate]")
	}

	switch args[0] {
	case "create":
		if len(args) != 2 {
			return fmt.Errorf("usage: plakar config remote create name")
		}
		name := args[1]
		if ctx.Config.HasRemote(name) {
			return fmt.Errorf("remote %q already exists", name)
		}
		ctx.Config.Remotes[name] = make(map[string]string)
		return ctx.Config.Save()

	case "set":
		if len(args) != 4 {
			return fmt.Errorf("usage: plakar config remote set name key value")
		}
		name, key, value := args[1], args[2], args[3]
		if !ctx.Config.HasRemote(name) {
			return fmt.Errorf("remote %q does not exists", name)
		}
		ctx.Config.Remotes[name][key] = value
		return ctx.Config.Save()

	case "validate":
		if len(args) != 2 {
			return fmt.Errorf("usage: plakar config remote validate name")
		}
		return fmt.Errorf("validtion not implemented")

	default:
		return fmt.Errorf("usage: plakar config remote [create | default | set | validate]")
	}
}

func cmd_repository(ctx *appcontext.AppContext, repo *repository.Repository, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: plakar config repository [create | default | set | validate]")
	}

	switch args[0] {
	case "create":
		if len(args) != 2 {
			return fmt.Errorf("usage: plakar config repository create name")
		}
		name := args[1]
		if ctx.Config.HasRepository(name) {
			return fmt.Errorf("repository %q already exists", name)
		}
		ctx.Config.Repositories[name] = make(map[string]string)
		return ctx.Config.Save()

	case "default":
		if len(args) != 2 {
			return fmt.Errorf("usage: plakar config repository default name")
		}
		name := args[1]
		if !ctx.Config.HasRepository(name) {
			return fmt.Errorf("repository %q doesn't exist", name)
		}
		ctx.Config.DefaultRepository = name
		return ctx.Config.Save()

	case "set":
		if len(args) != 4 {
			return fmt.Errorf("usage: plakar config repository set name key value")
		}
		name, key, value := args[1], args[2], args[3]
		if !ctx.Config.HasRepository(name) {
			return fmt.Errorf("repository %q does not exists", name)
		}
		ctx.Config.Repositories[name][key] = value
		return ctx.Config.Save()

	case "validate":
		if len(args) != 2 {
			return fmt.Errorf("usage: plakar config repository validate name")
		}
		return fmt.Errorf("validtion not implemented")

	default:
		return fmt.Errorf("usage: plakar config repository [create | default | set | validate]")
	}
}
