package info

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
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

			hasher := repo.HasherHMAC()
			hasher.Write(indexbuf)

			if !bytes.Equal(hasher.Sum(nil), footer.IndexHMAC[:]) {
				return 1, fmt.Errorf("index HMAC mismatch")
			}

			rawPackfile = append(rawPackfile, indexbuf...)
			rawPackfile = append(rawPackfile, footerbuf...)

			p, err := packfile.NewFromBytes(hasher, rawPackfile)
			if err != nil {
				return 1, err
			}

			fmt.Fprintf(ctx.Stdout, "Version: %d.%d.%d\n", p.Footer.Version/100, p.Footer.Version%100/10, p.Footer.Version%10)
			fmt.Fprintf(ctx.Stdout, "Timestamp: %s\n", time.Unix(0, p.Footer.Timestamp))
			fmt.Fprintf(ctx.Stdout, "Index HMAC: %x\n", p.Footer.IndexHMAC)
			fmt.Fprintln(ctx.Stdout)

			for i, entry := range p.Index {
				fmt.Fprintf(ctx.Stdout, "blob[%d]: %x %d %d %s\n", i, entry.HMAC, entry.Offset, entry.Length, entry.Type)
			}
		}
	}
	return 0, nil
}
