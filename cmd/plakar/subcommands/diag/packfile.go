package diag

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/repository"
)

type DiagPackfile struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Args []string
}

func (cmd *DiagPackfile) Name() string {
	return "diag_packfile"
}

func (cmd *DiagPackfile) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	if len(cmd.Args) == 0 {
		packfiles, err := repo.GetPackfiles()
		if err != nil {
			return 1, err
		}

		for _, packfile := range packfiles {
			fmt.Fprintf(ctx.Stdout, "%x\n", packfile)
		}
	} else {
		for _, arg := range cmd.Args {
			// convert arg to [32]byte
			if len(arg) != 64 {
				return 1, fmt.Errorf("invalid packfile hash: %s", arg)
			}

			b, err := hex.DecodeString(arg)
			if err != nil {
				return 1, fmt.Errorf("invalid packfile hash: %s", arg)
			}

			// Convert the byte slice to a [32]byte
			var byteArray [32]byte
			copy(byteArray[:], b)

			p, err := repo.GetPackfile(byteArray)
			if err != nil {
				return 1, err
			}

			fmt.Fprintf(ctx.Stdout, "Version: %s\n", p.Footer.Version)
			fmt.Fprintf(ctx.Stdout, "Timestamp: %s\n", time.Unix(0, p.Footer.Timestamp))
			fmt.Fprintf(ctx.Stdout, "Index MAC: %x\n", p.Footer.IndexMAC)
			fmt.Fprintln(ctx.Stdout)

			for i, entry := range p.Index {
				fmt.Fprintf(ctx.Stdout, "blob[%d]: %x %d %d %x %s\n", i, entry.MAC, entry.Offset, entry.Length, entry.Flags, entry.Type)
			}
		}
	}
	return 0, nil
}
