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

package create

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
)

func init() {
	subcommands.Register("create", cmd_create)
}

func cmd_create(ctx *appcontext.AppContext, _ *repository.Repository, args []string) (int, error) {
	var opt_noencryption bool
	var opt_nocompression bool

	flags := flag.NewFlagSet("create", flag.ExitOnError)
	flags.BoolVar(&opt_noencryption, "no-encryption", false, "disable transparent encryption")
	flags.BoolVar(&opt_nocompression, "no-compression", false, "disable transparent compression")
	flags.Parse(args)

	storageConfiguration := storage.NewConfiguration()
	if opt_nocompression {
		storageConfiguration.Compression = nil
	} else {
		compressionConfiguration := compression.DefaultConfiguration()
		storageConfiguration.Compression = compressionConfiguration
	}

	hashingConfiguration := hashing.DefaultConfiguration()
	storageConfiguration.Hashing = *hashingConfiguration

	if !opt_noencryption {
		storageConfiguration.Encryption.Algorithm = encryption.DefaultConfiguration().Algorithm

		var passphrase []byte

		envPassphrase := os.Getenv("PLAKAR_PASSPHRASE")
		if ctx.KeyFromFile == "" {
			if envPassphrase != "" {
				passphrase = []byte(envPassphrase)
			} else {
				for {
					tmp, err := utils.GetPassphraseConfirm("repository")
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s\n", err)
						continue
					}
					passphrase = tmp
					break
				}
			}
		} else {
			passphrase = []byte(ctx.KeyFromFile)
		}

		if len(passphrase) == 0 {
			return 1, fmt.Errorf("can't encrypt the repository with an empty passphrase")
		}

		salt, err := encryption.Salt()
		if err != nil {
			return 1, err
		}
		storageConfiguration.Encryption.KDFParams.Salt = salt

		key, err := encryption.DeriveKey(storageConfiguration.Encryption.KDFParams, passphrase)
		if err != nil {
			return 1, err
		}

		canary, err := encryption.DeriveCanary(key)
		if err != nil {
			return 1, err
		}
		storageConfiguration.Encryption.Canary = canary
	} else {
		storageConfiguration.Encryption = nil
	}

	switch flags.NArg() {
	case 0:
		repo, err := storage.Create(filepath.Join(ctx.HomeDir, ".plakar"), *storageConfiguration)
		if err != nil {
			return 1, err
		}
		repo.Close()
	case 1:
		repo, err := storage.Create(flags.Arg(0), *storageConfiguration)
		if err != nil {
			return 1, err
		}
		repo.Close()
	default:
		return 1, fmt.Errorf("%s: too many parameters", flag.CommandLine.Name())
	}

	return 0, nil
}
