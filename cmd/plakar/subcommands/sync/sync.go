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

package sync

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/storage"
)

func init() {
	subcommands.Register("sync", parse_cmd_sync)
}

func parse_cmd_sync(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	flags := flag.NewFlagSet("sync", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s [SNAPSHOT] to REPOSITORY\n", flags.Name())
		fmt.Fprintf(flags.Output(), "       %s [SNAPSHOT] from REPOSITORY\n", flags.Name())
		flags.PrintDefaults()
	}
	flags.Parse(args)

	syncSnapshotID := ""
	direction := ""
	peerRepositoryPath := ""

	args = flags.Args()
	switch len(args) {
	case 2:
		direction = args[0]
		peerRepositoryPath = args[1]
	case 3:
		syncSnapshotID = args[0]
		direction = args[1]
		peerRepositoryPath = args[2]

	default:
		return nil, fmt.Errorf("usage: sync [SNAPSHOT] to|from REPOSITORY")
	}

	if direction != "to" && direction != "from" && direction != "with" {
		return nil, fmt.Errorf("invalid direction, must be to, from or with")
	}

	peerStore, peerStoreSerializedConfig, err := storage.Open(map[string]string{"location": peerRepositoryPath})
	if err != nil {
		return nil, err
	}

	peerStoreConfig, err := storage.NewConfigurationFromWrappedBytes(peerStoreSerializedConfig)
	if err != nil {
		return nil, err
	}

	var peerSecret []byte
	if peerStoreConfig.Encryption != nil {
		for {
			passphrase, err := utils.GetPassphrase("destination repository")
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}

			key, err := encryption.DeriveKey(peerStoreConfig.Encryption.KDFParams, passphrase)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			if !encryption.VerifyCanary(peerStoreConfig.Encryption, key) {
				fmt.Fprintf(os.Stderr, "invalid passphrase\n")
				continue
			}
			peerSecret = key
			break
		}
	}

	peerCtx := appcontext.NewAppContextFrom(ctx)
	peerCtx.SetSecret(peerSecret)
	_, err = repository.New(peerCtx, peerStore, peerStoreSerializedConfig)
	if err != nil {
		return nil, err
	}

	return &Sync{
		SourceRepositoryLocation: repo.Location(),
		SourceRepositorySecret:   ctx.GetSecret(),
		PeerRepositoryLocation:   peerRepositoryPath,
		PeerRepositorySecret:     peerSecret,
		Direction:                direction,
		SnapshotPrefix:           syncSnapshotID,
	}, nil
}

type Sync struct {
	SourceRepositoryLocation string
	SourceRepositorySecret   []byte

	PeerRepositoryLocation string
	PeerRepositorySecret   []byte

	Direction string

	SnapshotPrefix string
}

func (cmd *Sync) Name() string {
	return "sync"
}

func (cmd *Sync) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {

	storeConfig := map[string]string{"location": cmd.PeerRepositoryLocation}
	if strings.HasPrefix(cmd.PeerRepositoryLocation, "@") {
		remote, ok := ctx.Config.GetRepository(cmd.PeerRepositoryLocation[1:])
		if !ok {
			return 1, fmt.Errorf("could not resolve repository: %s", cmd.PeerRepositoryLocation)
		}
		if _, ok := remote["location"]; !ok {
			return 1, fmt.Errorf("could not resolve repositor location: %s", cmd.PeerRepositoryLocation)
		} else {
			storeConfig = remote
		}
	}

	peerStore, peerStoreSerializedConfig, err := storage.Open(storeConfig)
	if err != nil {
		return 1, fmt.Errorf("could not open peer store %s: %s", cmd.PeerRepositoryLocation, err)
	}

	peerCtx := appcontext.NewAppContextFrom(ctx)
	peerCtx.SetSecret(cmd.PeerRepositorySecret)
	peerRepository, err := repository.New(peerCtx, peerStore, peerStoreSerializedConfig)
	if err != nil {
		return 1, fmt.Errorf("could not open peer repository %s: %s", cmd.PeerRepositoryLocation, err)
	}

	var srcRepository *repository.Repository
	var dstRepository *repository.Repository

	if cmd.Direction == "to" {
		srcRepository = repo
		dstRepository = peerRepository
	} else if cmd.Direction == "from" {
		srcRepository = peerRepository
		dstRepository = repo
	} else if cmd.Direction == "with" {
		srcRepository = repo
		dstRepository = peerRepository
	} else {
		return 1, fmt.Errorf("could not synchronize %s: invalid direction, must be to, from or with", peerStore.Location())
	}

	srcSnapshots, err := srcRepository.GetSnapshots()
	if err != nil {
		return 1, fmt.Errorf("could not get list of snapshots from source repository %s: %s", srcRepository.Location(), err)
	}

	dstSnapshots, err := dstRepository.GetSnapshots()
	if err != nil {
		return 1, fmt.Errorf("could not get list of snapshots from peer repository %s: %s", dstRepository.Location(), err)
	}

	srcSnapshotsMap := make(map[objects.MAC]struct{})
	dstSnapshotsMap := make(map[objects.MAC]struct{})

	for _, snapshotID := range srcSnapshots {
		srcSnapshotsMap[snapshotID] = struct{}{}
	}

	for _, snapshotID := range dstSnapshots {
		dstSnapshotsMap[snapshotID] = struct{}{}
	}

	srcSyncList := make([]objects.MAC, 0)

	srcLocateOptions := utils.NewDefaultLocateOptions()
	srcLocateOptions.Prefix = cmd.SnapshotPrefix
	srcSnapshotIDs, err := utils.LocateSnapshotIDs(srcRepository, srcLocateOptions)
	if err != nil {
		return 1, fmt.Errorf("could not locate snapshots in source repository %s: %s", dstRepository.Location(), err)
	}

	for _, snapshotID := range srcSnapshotIDs {
		if _, exists := dstSnapshotsMap[snapshotID]; !exists {
			srcSyncList = append(srcSyncList, snapshotID)
		}
	}

	for _, snapshotID := range srcSyncList {
		err := synchronize(srcRepository, dstRepository, snapshotID)
		if err != nil {
			ctx.GetLogger().Error("failed to synchronize snapshot %x from source repository %s: %s",
				snapshotID[:4], srcRepository.Location(), err)
		}
	}

	if cmd.Direction == "with" {
		dstSnapshotIDs, err := utils.LocateSnapshotIDs(dstRepository, srcLocateOptions)
		if err != nil {
			return 1, fmt.Errorf("could not locate snapshots in peer repository %s: %s", dstRepository.Location(), err)
		}

		dstSyncList := make([]objects.MAC, 0)
		for _, snapshotID := range dstSnapshotIDs {
			if _, exists := srcSnapshotsMap[snapshotID]; !exists {
				dstSyncList = append(dstSyncList, snapshotID)
			}
		}

		for _, snapshotID := range dstSyncList {
			err := synchronize(dstRepository, srcRepository, snapshotID)
			if err != nil {
				ctx.GetLogger().Error("failed to synchronize snapshot %x from peer repository %s: %s",
					snapshotID[:4], dstRepository.Location(), err)
			}
		}
		ctx.GetLogger().Info("%s: synchronization between %s and %s completed: %d snapshots synchronized",
			cmd.Name(),
			srcRepository.Location(),
			dstRepository.Location(),
			len(srcSyncList)+len(dstSyncList))
	} else if cmd.Direction == "to" {
		ctx.GetLogger().Info("%s: synchronization from %s to %s completed: %d snapshots synchronized",
			cmd.Name(),
			srcRepository.Location(),
			dstRepository.Location(),
			len(srcSyncList))
	} else {
		ctx.GetLogger().Info("%s: synchronization from %s to %s completed: %d snapshots synchronized",
			cmd.Name(),
			dstRepository.Location(),
			srcRepository.Location(),
			len(srcSyncList))
	}

	return 0, nil
}

func synchronize(srcRepository, dstRepository *repository.Repository, snapshotID objects.MAC) error {
	srcSnapshot, err := snapshot.Load(srcRepository, snapshotID)
	if err != nil {
		return err
	}
	defer srcSnapshot.Close()

	dstSnapshot, err := snapshot.New(dstRepository)
	if err != nil {
		return err
	}
	defer dstSnapshot.Close()

	// overwrite the header, we want to keep the original snapshot info
	dstSnapshot.Header = srcSnapshot.Header

	if err := srcSnapshot.Synchronize(dstSnapshot); err != nil {
		return err
	}

	return dstSnapshot.Commit()
}
