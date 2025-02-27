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

package clone

import (
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/google/uuid"
)

func init() {
	subcommands.Register("clone", parse_cmd_clone)
}

func parse_cmd_clone(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	flags := flag.NewFlagSet("clone", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s to /path/to/repository\n", flags.Name())
		fmt.Fprintf(flags.Output(), "       %s to s3://bucket/path\n", flags.Name())
		flags.PrintDefaults()
	}

	flags.Parse(args)

	if flags.NArg() != 2 || flags.Arg(0) != "to" {
		return nil, fmt.Errorf("usage: %s to <repository>. See '%s -h' or 'help %s'", flags.Name(), flags.Name(), flags.Name())
	}

	return &Clone{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
		Dest:               flags.Arg(1),
	}, nil
}

type Clone struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Dest string
}

func (cmd *Clone) Name() string {
	return "clone"
}

func (cmd *Clone) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	sourceStore := repo.Store()

	configuration := repo.Configuration()
	configuration.RepositoryID = uuid.Must(uuid.NewRandom())

	serializedConfig, err := configuration.ToBytes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to decode storage configuration: %s\n", err)
		return 1, err
	}

	cloneStore, err := storage.Create(map[string]string{"location": cmd.Dest}, serializedConfig)
	if err != nil {
		return 1, fmt.Errorf("could not create repository: %w", err)
	}

	packfileMACs, err := sourceStore.GetPackfiles()
	if err != nil {
		return 1, fmt.Errorf("could not get packfiles list from repository: %w", err)
	}

	wg := sync.WaitGroup{}
	for _, _packfileMAC := range packfileMACs {
		wg.Add(1)
		go func(packfileMAC objects.MAC) {
			defer wg.Done()

			rd, err := sourceStore.GetPackfile(packfileMAC)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not get packfile from repository: %s\n", err)
				return
			}

			err = cloneStore.PutPackfile(packfileMAC, rd)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not put packfile to repository: %s\n", err)
				return
			}
		}(_packfileMAC)
	}
	wg.Wait()

	indexesMACs, err := sourceStore.GetStates()
	if err != nil {
		return 1, fmt.Errorf("could not get packfiles list from repository: %w", err)
	}

	wg = sync.WaitGroup{}
	for _, _indexMAC := range indexesMACs {
		wg.Add(1)
		go func(indexMAC objects.MAC) {
			defer wg.Done()

			data, err := sourceStore.GetState(indexMAC)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not get index from repository: %s\n", err)
				return
			}

			err = cloneStore.PutState(indexMAC, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not put packfile to repository: %s\n", err)
				return
			}
		}(_indexMAC)
	}
	wg.Wait()

	return 0, nil
}
