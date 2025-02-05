package info

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/repository"
)

type InfoPackfile struct {
	RepositoryLocation string
	RepositorySecret   []byte

	Args []string
}

func (cmd *InfoPackfile) Name() string {
	return "info_packfile"
}

func (cmd *InfoPackfile) Parse(ctx *appcontext.AppContext, repo *repository.Repository, args []string) error {
	flags := flag.NewFlagSet("info packfile", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		return fmt.Errorf("usage: %s packfile...", flags.Name())
	}

	cmd.RepositoryLocation = repo.Location()
	cmd.RepositorySecret = ctx.GetSecret()
	cmd.Args = flags.Args()
	return nil
}

func (cmd *InfoPackfile) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
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

			rd, err := repo.GetPackfile(byteArray)
			if err != nil {
				return 1, err
			}

			rawPackfile, err := io.ReadAll(rd)
			if err != nil {
				return 1, err
			}

			versionBytes := rawPackfile[len(rawPackfile)-5 : len(rawPackfile)-5+4]
			version := binary.LittleEndian.Uint32(versionBytes)

			//			version := rawPackfile[len(rawPackfile)-2]
			footerOffset := rawPackfile[len(rawPackfile)-1]
			rawPackfile = rawPackfile[:len(rawPackfile)-5]

			_ = version

			footerbuf := rawPackfile[len(rawPackfile)-int(footerOffset):]
			rawPackfile = rawPackfile[:len(rawPackfile)-int(footerOffset)]

			footerbuf, err = repo.DecodeBuffer(footerbuf)
			if err != nil {
				return 1, err
			}
			footer, err := packfile.NewFooterFromBytes(footerbuf)
			if err != nil {
				return 1, err
			}

			indexbuf := rawPackfile[int(footer.IndexOffset):]
			rawPackfile = rawPackfile[:int(footer.IndexOffset)]

			indexbuf, err = repo.DecodeBuffer(indexbuf)
			if err != nil {
				return 1, err
			}

			hasher := sha256.New()
			hasher.Write(indexbuf)

			if !bytes.Equal(hasher.Sum(nil), footer.IndexChecksum[:]) {
				return 1, fmt.Errorf("index checksum mismatch")
			}

			rawPackfile = append(rawPackfile, indexbuf...)
			rawPackfile = append(rawPackfile, footerbuf...)

			p, err := packfile.NewFromBytes(rawPackfile)
			if err != nil {
				return 1, err
			}

			fmt.Fprintf(ctx.Stdout, "Version: %d.%d.%d\n", p.Footer.Version/100, p.Footer.Version%100/10, p.Footer.Version%10)
			fmt.Fprintf(ctx.Stdout, "Timestamp: %s\n", time.Unix(0, p.Footer.Timestamp))
			fmt.Fprintf(ctx.Stdout, "Index checksum: %x\n", p.Footer.IndexChecksum)
			fmt.Fprintln(ctx.Stdout)

			for i, entry := range p.Index {
				fmt.Fprintf(ctx.Stdout, "blob[%d]: %x %d %d %s\n", i, entry.Checksum, entry.Offset, entry.Length, entry.Type)
			}
		}
	}
	return 0, nil
}
