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

package checksum

import (
	"flag"
	"fmt"
	"io"
	"path"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

func init() {
	subcommands.Register("checksum", parse_cmd_checksum)
}

func parse_cmd_checksum(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var enableFastChecksum bool

	flags := flag.NewFlagSet("checksum", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s [OPTIONS] [SNAPSHOT[:PATH]]...\n", flags.Name())
		fmt.Fprintf(flags.Output(), "\nOPTIONS:\n")
		flags.PrintDefaults()
	}

	flags.BoolVar(&enableFastChecksum, "fast", false, "enable fast checksum (return recorded checksum)")

	flags.Parse(args)

	if flags.NArg() == 0 {
		ctx.GetLogger().Error("%s: at least one parameter is required", flags.Name())
		return nil, fmt.Errorf("at least one parameter is required")
	}

	return &Checksum{
		RepositoryLocation: repo.Location(),
		RepositorySecret:   ctx.GetSecret(),
		Fast:               enableFastChecksum,
		Targets:            flags.Args(),
	}, nil
}

type Checksum struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Fast    bool
	Targets []string
}

func (cmd *Checksum) Name() string {
	return "checksum"
}

func (cmd *Checksum) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	snapshots, err := utils.GetSnapshots(repo, cmd.Targets)
	if err != nil {
		ctx.GetLogger().Error("checksum: could not obtain snapshots list: %s", err)
		return 1, err
	}

	errors := 0
	for offset, snap := range snapshots {
		defer snap.Close()

		fs, err := snap.Filesystem()
		if err != nil {
			continue
		}

		_, pathname := utils.ParseSnapshotID(cmd.Targets[offset])
		if pathname == "" {
			ctx.GetLogger().Error("checksum: missing filename for snapshot %x", snap.Header.GetIndexShortID())
			errors++
			continue
		}

		displayChecksums(ctx, fs, repo, snap, pathname, cmd.Fast)
	}

	return 0, nil
}

func displayChecksums(ctx *appcontext.AppContext, fs *vfs.Filesystem, repo *repository.Repository, snap *snapshot.Snapshot, pathname string, fastcheck bool) error {
	fsinfo, err := fs.GetEntry(pathname)
	if err != nil {
		return err
	}

	if fsinfo.Stat().Mode().IsDir() {
		iter, err := fsinfo.Getdents(fs)
		if err != nil {
			return err
		}
		for child := range iter {
			if err := displayChecksums(ctx, fs, repo, snap, path.Join(pathname, child.Stat().Name()), fastcheck); err != nil {
				return err
			}
		}
		return nil
	}
	if !fsinfo.Stat().Mode().IsRegular() {
		return nil
	}

	object, err := snap.LookupObject(fsinfo.Object)
	if err != nil {
		return err
	}

	checksum := object.Checksum
	if !fastcheck {
		rd, err := snap.NewReader(pathname)
		if err != nil {
			return err
		}
		defer rd.Close()

		hasher := repo.Hasher()
		if _, err := io.Copy(hasher, rd); err != nil {
			return err
		}
	}
	fmt.Fprintf(ctx.Stdout, "SHA256 (%s) = %x\n", pathname, checksum)
	return nil
}
