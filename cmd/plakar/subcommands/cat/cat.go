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

package cat

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/alecthomas/chroma/formatters"
	"github.com/alecthomas/chroma/lexers"
	"github.com/alecthomas/chroma/styles"
)

func init() {
	subcommands.Register(&Cat{}, "cat")
}

type Cat struct {
	RepositoryLocation string
	RepositorySecret   []byte

	NoDecompress bool
	Highlight    bool
	Paths        []string
}

func (cmd *Cat) Parse(ctx *appcontext.AppContext, repo *repository.Repository, args []string) error {
	flags := flag.NewFlagSet("cat", flag.ExitOnError)
	flags.BoolVar(&cmd.NoDecompress, "no-decompress", false, "do not try to decompress output")
	flags.BoolVar(&cmd.Highlight, "highlight", false, "highlight output")
	flags.Parse(args)

	if flags.NArg() == 0 {
		return fmt.Errorf("at least one parameter is required")
	}

	cmd.RepositoryLocation = repo.Location()
	cmd.RepositorySecret = ctx.GetSecret()
	cmd.Paths = flags.Args()
	return nil
}

func (cmd *Cat) Name() string {
	return "cat"
}

func (cmd *Cat) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	snapshots, err := utils.GetSnapshots(repo, cmd.Paths)
	if err != nil {
		ctx.GetLogger().Error("cat: could not obtain snapshots list: %s", err)
		return 1, err
	}

	errors := 0
	for offset, snap := range snapshots {
		defer snap.Close()

		_, pathname := utils.ParseSnapshotID(cmd.Paths[offset])

		if pathname == "" {
			ctx.GetLogger().Error("cat: missing filename for snapshot")
			errors++
			continue
		}

		fs, err := snap.Filesystem()
		if err != nil {
			ctx.GetLogger().Error("cat: %s: %s", pathname, err)
			errors++
			continue
		}

		entry, err := fs.GetEntry(pathname)
		if err != nil {
			ctx.GetLogger().Error("cat: %s: no such file", pathname)
			errors++
			continue
		}

		if !entry.Stat().Mode().IsRegular() {
			ctx.GetLogger().Error("cat: %s: not a regular file", pathname)
			errors++
			continue
		}

		file := entry.Open(fs, pathname)
		var rd io.ReadCloser = file

		if !cmd.NoDecompress {
			if entry.Object.ContentType == "application/gzip" && !cmd.NoDecompress {
				gzRd, err := gzip.NewReader(rd)
				if err != nil {
					ctx.GetLogger().Error("cat: %s: %s", pathname, err)
					errors++
					file.Close()
					continue
				}
				rd = gzRd
			}
		}

		if cmd.Highlight {
			lexer := lexers.Match(pathname)
			if lexer == nil {
				lexer = lexers.Get(entry.Object.ContentType)
			}
			if lexer == nil {
				lexer = lexers.Fallback // Fallback if no lexer is found
			}
			formatter := formatters.Get("terminal")
			style := styles.Get("dracula")

			reader := bufio.NewReader(rd)
			buffer := make([]byte, 4096) // Fixed-size buffer for chunked reading
			for {
				n, err := reader.Read(buffer) // Read up to the size of the buffer
				if n > 0 {
					chunk := string(buffer[:n])

					// Tokenize the chunk and apply syntax highlighting
					iterator, errTokenize := lexer.Tokenise(nil, chunk)
					if errTokenize != nil {
						ctx.GetLogger().Error("cat: %s: %s", pathname, errTokenize)
						errors++
						break
					}

					errFormat := formatter.Format(ctx.Stdout, style, iterator)
					if errFormat != nil {
						ctx.GetLogger().Error("cat: %s: %s", pathname, errFormat)
						errors++
						break
					}
				}

				// Check for end of file (EOF)
				if err == io.EOF {
					break
				} else if err != nil {
					ctx.GetLogger().Error("cat: %s: %s", pathname, err)
					errors++
					break
				}
			}
		} else {
			_, err = io.Copy(ctx.Stdout, rd)
		}
		file.Close()
		if err != nil {
			ctx.GetLogger().Error("cat: %s: %s", pathname, err)
			errors++
			continue
		}
	}

	if errors != 0 {
		return 1, fmt.Errorf("errors occurred")
	}
	return 0, nil
}
