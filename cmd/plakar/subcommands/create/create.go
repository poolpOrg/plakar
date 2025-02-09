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
	passwordvalidator "github.com/wagslane/go-password-validator"
)

func init() {
	subcommands.Register("create", parse_cmd_create)
}

func parse_cmd_create(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var opt_noencryption bool
	var opt_nocompression bool
	var opt_allowweak bool

	flags := flag.NewFlagSet("create", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s [OPTIONS] /path/to/repository\n", flags.Name())
		fmt.Fprintf(flags.Output(), "       %s [OPTIONS] s3://bucket/path\n", flags.Name())
		fmt.Fprintf(flags.Output(), "\nOPTIONS:\n")
		flags.PrintDefaults()
	}

	flags.BoolVar(&opt_allowweak, "weak-passphrase", false, "allow weak passphrase to protect the repository")
	flags.BoolVar(&opt_noencryption, "no-encryption", false, "disable transparent encryption")
	flags.BoolVar(&opt_nocompression, "no-compression", false, "disable transparent compression")
	flags.Parse(args)

	if flags.NArg() > 1 {
		return nil, fmt.Errorf("%s: too many parameters", flag.CommandLine.Name())
	}

	return &Create{
		AllowWeak:     opt_allowweak,
		NoEncryption:  opt_noencryption,
		NoCompression: opt_nocompression,
		Location:      flags.Arg(0),
	}, nil
}

type Create struct {
	AllowWeak     bool
	NoEncryption  bool
	NoCompression bool
	Location      string
}

func (cmd *Create) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	storageConfiguration := storage.NewConfiguration()
	if cmd.NoCompression {
		storageConfiguration.Compression = nil
	} else {
		compressionConfiguration := compression.DefaultConfiguration()
		storageConfiguration.Compression = compressionConfiguration
	}

	hashingConfiguration := hashing.DefaultConfiguration()
	storageConfiguration.Hashing = *hashingConfiguration

	if !cmd.NoEncryption {
		storageConfiguration.Encryption.Algorithm = encryption.DefaultConfiguration().Algorithm

		var passphrase []byte

		envPassphrase := os.Getenv("PLAKAR_PASSPHRASE")
		if ctx.KeyFromFile == "" {
			if envPassphrase != "" {
				passphrase = []byte(envPassphrase)
			} else {
				for attempt := 0; attempt < 3; attempt++ {
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

		if !cmd.AllowWeak {
			// keepass considers < 80 bits as weak
			minEntropBits := 80.
			err := passwordvalidator.Validate(string(passphrase), minEntropBits)
			if err != nil {
				return 1, fmt.Errorf("passphrase is too weak: %s", err)
			}
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

	if cmd.Location == "" {
		repo, err := storage.Create(filepath.Join(ctx.HomeDir, ".plakar"), *storageConfiguration)
		if err != nil {
			return 1, err
		}
		repo.Close()
	} else {
		repo, err := storage.Create(cmd.Location, *storageConfiguration)
		if err != nil {
			return 1, err
		}
		repo.Close()
	}
	return 0, nil
}
