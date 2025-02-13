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
	"time"

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

func colourPass(ctx *appcontext.AppContext, repo *repository.Repository) error {
	var packfiles map[objects.MAC]struct{} = make(map[objects.MAC]struct{})
	for checksum := range repo.ListPackfiles() {
		packfiles[checksum] = struct{}{}
	}

	for snapshotID := range repo.ListSnapshots() {
		fmt.Fprintf(ctx.Stdout, "snapshot: %x\n", snapshotID)

		snapshot, err := snapshot.Load(repo, snapshotID)
		if err != nil {
			return err
		}

		iter, err := snapshot.ListPackfiles()
		if err != nil {
			return err
		}

		for packfile, err := range iter {
			if err != nil {
				return err
			}

			delete(packfiles, packfile)
		}

		snapshot.Close()
	}

	// This random id generation for non snapshot state should probably be encapsulated somewhere.
	var identifier objects.MAC
	n, err := rand.Read(identifier[:])
	if err != nil {
		return err
	}
	if n != len(identifier) {
		return io.ErrShortWrite
	}

	sc, err := repo.AppContext().GetCache().Scan(identifier)
	if err != nil {
		return err
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
			return err
		}

		if !has {
			fmt.Fprintf(ctx.Stdout, "cleanup: packfile: %x\n", packfile)
			if err := deltaState.DeleteResource(resources.RT_PACKFILE, packfile); err != nil {
				return err
			}
		}
	}

	buf := &bytes.Buffer{}
	if err := deltaState.SerializeToStream(buf); err != nil {
		return err
	}

	if err := repo.PutState(identifier, buf); err != nil {
		return err
	}

	return nil
}

func sweepPass(ctx *appcontext.AppContext, repo *repository.Repository) error {
	// These need to be configurable per repo, but we don't have a mechanism yet (comes in a PR soon!)
	cutoff := time.Now().AddDate(0, 0, -3)
	doDeletion := false

	// 1. For each packfile, iterate over it and remove the entries in our aggregated state.
	// 2. Actually remove the packfile (if option is enabled).
	// 3. Remove deleted packfiles entries so that we don't reprocess them next run.
	// 4. Serialize our aggregated state with a new Serial number and push it to repo.
	toDelete := make([]objects.MAC, 0)
	for packfileMAC, deletionTime := range repo.ListDeletedPackfiles() {
		if deletionTime.After(cutoff) {
			continue
		}

		packfile, err := repo.GetPackfile(packfileMAC)
		if err != nil {
			fmt.Fprintf(ctx.Stderr, "cleanup: Sweep pass failed to fetch packfile %x, skipping it\n", packfileMAC)
			continue
		}

		for _, blob := range packfile.Index {
			//XXX: Unless we have transactions this is going to be hard to handle errors.
			if err := repo.RemoveBlob(blob.Type, blob.MAC); err != nil {
				fmt.Fprintf(ctx.Stderr, "cleanup: Sweep pass failed to remove blob %x, type %s\n", blob.MAC, blob.Type)
			}
		}

		if doDeletion {
			toDelete = append(toDelete, packfileMAC)
		}
	}

	//XXX: Same here.

	//XXX: The solution might be to clone the full local leveldb state and switch to it once we know everything went well.
	if err := repo.PutCurrentState(); err != nil {
		return err
	}

	//XXX: We do this as the very last step since this is destructive.
	// The only problem is that we have to have the list in memory but if you
	// have enough packfiles to delete for this to be a concern we might have
	// other issues anyway.
	for _, packfileMAC := range toDelete {
		if err := repo.DeletePackfile(packfileMAC); err != nil {
			fmt.Fprintf(ctx.Stderr, "cleanup: Sweep pass failed to delete packfile %x, skipping it\n", packfileMAC)
		}
	}

	return nil
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

	if err := colourPass(ctx, repo); err != nil {
		fmt.Fprintf(ctx.Stderr, "cleanup: Colouring pass failed %s\n", err)
		return 1, err
	}

	if err := sweepPass(ctx, repo); err != nil {
		fmt.Fprintf(ctx.Stderr, "cleanup: Sweep pass failed %s\n", err)
		return 1, err
	}

	return 0, nil
}
