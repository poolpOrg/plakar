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
	"github.com/PlakarKorp/plakar/caching"
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

// Builds the local cache of snapshot -> packfiles
func updateCache(ctx *appcontext.AppContext, repo *repository.Repository, cache *caching.MaintainanceCache) error {
	for snapshotID := range repo.ListSnapshots() {
		snapshot, err := snapshot.Load(repo, snapshotID)
		if err != nil {
			return err
		}

		ok, err := cache.HasSnapshot(snapshotID)
		if err != nil {
			return err
		}

		if ok {
			continue
		}

		fmt.Fprintf(ctx.Stdout, "updating cache with snapshot: %x\n", snapshotID)
		iter, err := snapshot.ListPackfiles()
		if err != nil {
			return err
		}

		for packfile, err := range iter {
			if err != nil {
				return err
			}

			if err := cache.PutPackfile(snapshotID, packfile); err != nil {
				return err
			}
		}

		cache.PutSnapshot(snapshotID, nil)
		snapshot.Close()
	}

	// While ListSnapshots doesn't return deleted snapshots, we still need to
	// go over them to remove previously added one to our local cache.
	for snapshotID := range repo.ListDeletedSnapShots() {
		ok, err := cache.HasSnapshot(snapshotID)
		if err != nil {
			return err
		}

		if !ok {
			continue
		}

		fmt.Fprintf(ctx.Stdout, "deleting %x from local cache\n", snapshotID)
		cache.DeleletePackfiles(snapshotID)
		cache.DeleteSnapshot(snapshotID)
	}

	return nil
}

func colourPass(ctx *appcontext.AppContext, repo *repository.Repository, cache *caching.MaintainanceCache) error {
	var packfiles map[objects.MAC]struct{} = make(map[objects.MAC]struct{})

	for packfileMAC := range repo.ListPackfiles() {
		if !cache.HasPackfile(packfileMAC) {
			packfiles[packfileMAC] = struct{}{}
		}
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

	coloredPackfiles := 0
	for packfile := range packfiles {
		has, err := repo.HasDeletedPackfile(packfile)
		if err != nil {
			return err
		}

		if !has {
			coloredPackfiles++
			if err := deltaState.DeleteResource(resources.RT_PACKFILE, packfile); err != nil {
				return err
			}
		}
	}

	fmt.Fprintf(ctx.Stdout, "colour: Coloured %d packfiles for deletion\n", coloredPackfiles)

	buf := &bytes.Buffer{}
	if err := deltaState.SerializeToStream(buf); err != nil {
		return err
	}

	if err := repo.PutState(identifier, buf); err != nil {
		return err
	}

	return nil
}

func sweepPass(ctx *appcontext.AppContext, repo *repository.Repository, cache *caching.MaintainanceCache) error {
	// These need to be configurable per repo, but we don't have a mechanism yet (comes in a PR soon!)
	cutoff := time.Now().AddDate(0, 0, -30)
	doDeletion := false

	// First go over all the packfiles coloured by first pass.
	packfileremoved := 0
	blobRemoved := 0
	for packfileMAC, deletionTime := range repo.ListDeletedPackfiles() {
		if deletionTime.After(cutoff) {
			continue
		}

		// At this point we have to re-check if our packfile is really unused,
		// because we could have had a concurrent backup with the coloring
		// phase.
		if cache.HasPackfile(packfileMAC) {
			fmt.Fprintf(ctx.Stderr, "cleanup: Concurrent backup used %x, uncolouring the packfile.\n", packfileMAC)
			repo.RemoveDeletedPackfile(packfileMAC)
			continue
		}

		// First thing we remove the packfile entry from our state, this means
		// that now effectively all of its blob are unreachable
		if err := repo.RemovePackfile(packfileMAC); err != nil {
			fmt.Fprintf(ctx.Stderr, "cleanup: Failed to remove packfile %s from state\n", packfileMAC)
		} else {
			repo.RemoveDeletedPackfile(packfileMAC)
		}

		packfileremoved++
	}

	// Second garbage collect dangling blobs in our state. This is the blobs we
	// just orphaned plus potential orphan blobs from aborted backups etc.
	toDelete := map[objects.MAC]struct{}{}
	for blob, err := range repo.ListOrphanBlobs() {
		if err != nil {
			fmt.Fprintf(ctx.Stderr, "cleanup: Failed to fetch orphaned blob\n")
			continue
		}

		blobRemoved++
		toDelete[blob.Location.Packfile] = struct{}{}
		if err := repo.RemoveBlob(blob.Type, blob.Blob, blob.Location.Packfile); err != nil {
			// No hurt in this failing, we just have cruft left around, but they are unreachable anyway.
			fmt.Fprintf(ctx.Stderr, "cleanup: garbage orphaned blobs pass failed to remove blob %x, type %s\n", blob.Blob, blob.Type)
		}
	}

	fmt.Fprintf(ctx.Stdout, "cleanup: %d blobs and %d packfiles were removed\n", blobRemoved, packfileremoved)
	if err := repo.PutCurrentState(); err != nil {
		return err
	}

	if doDeletion {
		for packfileMAC := range toDelete {
			if err := repo.DeletePackfile(packfileMAC); err != nil {
				fmt.Fprintf(ctx.Stderr, "cleanup: Sweep pass failed to delete packfile %x, skipping it\n", packfileMAC)
			}
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
	cache, err := repo.AppContext().GetCache().Maintainance(repo.Configuration().RepositoryID)
	if err != nil {
		fmt.Fprintf(ctx.Stderr, "cleanup: Failed to open local cache %s\n", err)
		return 1, err
	}

	if err := updateCache(ctx, repo, cache); err != nil {
		fmt.Fprintf(ctx.Stderr, "cleanup: Failed to update local cache %s\n", err)
		return 1, err
	}

	if err := colourPass(ctx, repo, cache); err != nil {
		fmt.Fprintf(ctx.Stderr, "cleanup: Colouring pass failed %s\n", err)
		return 1, err
	}

	if err := sweepPass(ctx, repo, cache); err != nil {
		fmt.Fprintf(ctx.Stderr, "cleanup: Sweep pass failed %s\n", err)
		return 1, err
	}

	return 0, nil
}
