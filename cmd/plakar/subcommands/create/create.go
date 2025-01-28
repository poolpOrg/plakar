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
	"strings"

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
	var opt_hashing string
	var opt_compression string

	flags := flag.NewFlagSet("create", flag.ExitOnError)
	flags.BoolVar(&opt_noencryption, "no-encryption", false, "disable transparent encryption")
	flags.BoolVar(&opt_nocompression, "no-compression", false, "disable transparent compression")
	flags.StringVar(&opt_hashing, "hashing", "SHA256", "swap the hashing function")
	flags.StringVar(&opt_compression, "compression", "LZ4", "swap the compression function")
	flags.Parse(args)

	storageConfiguration := storage.NewConfiguration()
	if opt_nocompression {
		storageConfiguration.Compression = nil
	} else {
		compressionConfiguration, err := compression.LookupDefaultConfiguration(strings.ToUpper(opt_compression))
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1, err
		}
		storageConfiguration.Compression = compressionConfiguration
	}

	hashingConfiguration, err := hashing.LookupDefaultConfiguration(strings.ToUpper(opt_hashing))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
		return 1, err
	}
	storageConfiguration.Hashing = *hashingConfiguration

	if !opt_noencryption {
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

		encryptionKey, err := encryption.BuildSecretFromPassphrase(passphrase)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1, err
		}

		storageConfiguration.Encryption.Algorithm = encryption.DefaultConfiguration().Algorithm
		storageConfiguration.Encryption.Key = encryptionKey
	} else {
		storageConfiguration.Encryption = nil
	}

	switch flags.NArg() {
	case 0:
		repo, err := storage.Create(filepath.Join(ctx.HomeDir, ".plakar"), *storageConfiguration)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1, err
		}
		repo.Close()
	case 1:
		repo, err := storage.Create(flags.Arg(0), *storageConfiguration)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1, err
		}
		repo.Close()
	default:
		fmt.Fprintf(os.Stderr, "%s: too many parameters\n", flag.CommandLine.Name())
		return 1, fmt.Errorf("%s: too many parameters", flag.CommandLine.Name())
	}

	return 0, nil
}
