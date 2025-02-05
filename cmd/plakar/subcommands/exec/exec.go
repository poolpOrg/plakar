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

package exec

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
)

func init() {
	subcommands.Register(&Exec{}, "exec")
}

type Exec struct {
	RepositoryLocation string
	RepositorySecret   []byte

	SnapshotPrefix string
	Args           []string
}

func (cmd *Exec) Parse(ctx *appcontext.AppContext, repo *repository.Repository, args []string) error {
	flags := flag.NewFlagSet("exec", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		return fmt.Errorf("at least one parameters is required")
	}

	cmd.RepositoryLocation = repo.Location()
	cmd.RepositorySecret = ctx.GetSecret()
	cmd.SnapshotPrefix = flags.Arg(0)
	cmd.Args = flags.Args()[1:]

	return nil
}

func (cmd *Exec) Name() string {
	return "exec"
}

func (cmd *Exec) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	snapshots, err := utils.GetSnapshots(repo, []string{cmd.SnapshotPrefix})
	if err != nil {
		log.Fatal(err)
	}
	if len(snapshots) != 1 {
		return 0, nil
	}
	snap := snapshots[0]
	defer snap.Close()

	_, pathname := utils.ParseSnapshotID(cmd.SnapshotPrefix)

	rd, err := snap.NewReader(pathname)
	if err != nil {
		ctx.GetLogger().Error("exec: %s: failed to open: %s", pathname, err)
		return 1, err
	}
	defer rd.Close()

	file, err := os.CreateTemp(os.TempDir(), "plakar")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())
	file.Chmod(0500)

	_, err = io.Copy(file, rd)
	if err != nil {
		log.Fatal(err)
	}
	file.Close()

	p := exec.Command(file.Name(), cmd.Args...)
	stdin, err := p.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		defer stdin.Close()
		io.Copy(stdin, os.Stdin)
	}()

	stdout, err := p.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		defer stdout.Close()
		io.Copy(os.Stdout, stdout)
	}()

	stderr, err := p.StderrPipe()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		defer stdout.Close()
		io.Copy(os.Stderr, stderr)
	}()
	if p.Start() == nil {
		p.Wait()
		return p.ProcessState.ExitCode(), nil
	}
	return 1, err
}
