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
	"bytes"
	"crypto/rand"
	"flag"
	"fmt"
	"io"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot"
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
	// 1. fetch all packfiles in the repository
	// 2. iterate over snapshots
	// 3. for each snapshot, resolve the packfiles it references
	// 4. remove the packfiles from the list of packfiles
	// 5. remaining packfiles should be marked as deleted in the state
	// 6. remove the packfile in repository once it's flagged as deleted AND all snapshots have been `snapshot.Check`-ed
	// 7. rebuild a new aggregate state with a new serial without the deleted packfiles

	var packfiles map[objects.Checksum]struct{} = make(map[objects.Checksum]struct{})
	for checksum := range repo.ListPackfiles() {
		packfiles[checksum] = struct{}{}
	}

	for snapshotID := range repo.ListSnapshots() {
		fmt.Fprintf(ctx.Stdout, "snapshot: %x\n", snapshotID)

		snapshot, err := snapshot.Load(repo, snapshotID)
		if err != nil {
			return 1, err
		}

		iter, err := snapshot.ListPackfiles()
		if err != nil {
			return 1, err
		}

		for packfile, err := range iter {
			if err != nil {
				return 1, err
			}

			delete(packfiles, packfile)
		}

		snapshot.Close()
	}

	// This random id generation for non snapshot state should probably be encapsulated somewhere.
	var identifier objects.Checksum
	n, err := rand.Read(identifier[:])
	if err != nil {
		return 1, err
	}
	if n != len(identifier) {
		return 1, io.ErrShortWrite
	}

	sc, err := repo.AppContext().GetCache().Scan(identifier)
	if err != nil {
		return 1, err
	}

	// First pass, coloring, we just flag those packfiles as being selected for deletion.
	// For now we keep the same serial so that those delete gets merged in.
	// Once we do the real deletion we will rebuild the aggregated view
	// excluding those ressources alltogether.
	deltaState := repo.NewStateDelta(sc)

	fmt.Fprintf(ctx.Stdout, "cleanup: packfiles to remove: %d\n", len(packfiles))
	for packfile := range packfiles {
		has, err := repo.HasDeletedPackfile(packfile)
		if err != nil {
			return 1, err
		}

		if !has {
			fmt.Fprintf(ctx.Stdout, "cleanup: packfile: %x\n", packfile)
			if err := deltaState.DeleteResource(resources.RT_PACKFILE, packfile); err != nil {
				return 1, err
			}
		}
	}

	buf := &bytes.Buffer{}
	if err := deltaState.SerializeToStream(buf); err != nil {
		return 1, err
	}

	if err := repo.PutState(identifier, buf); err != nil {
		return 1, err
	}

	return 0, nil
}
