package info

import (
	"fmt"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/dustin/go-humanize"
)

type InfoRepository struct {
	RepositoryLocation string
	RepositorySecret   []byte
}

func (cmd *InfoRepository) Name() string {
	return "info_repository"
}

func (cmd *InfoRepository) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	metadatas, err := utils.GetHeaders(repo, nil)
	if err != nil {
		return 1, err
	}

	fmt.Fprintln(ctx.Stdout, "Version:", repo.Configuration().Version)
	fmt.Fprintln(ctx.Stdout, "Timestamp:", repo.Configuration().Timestamp)
	fmt.Fprintln(ctx.Stdout, "RepositoryID:", repo.Configuration().RepositoryID)

	fmt.Fprintln(ctx.Stdout, "Packfile:")
	fmt.Fprintf(ctx.Stdout, " - MaxSize: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().Packfile.MaxSize)),
		repo.Configuration().Packfile.MaxSize)

	fmt.Fprintln(ctx.Stdout, "Chunking:")
	fmt.Fprintln(ctx.Stdout, " - Algorithm:", repo.Configuration().Chunking.Algorithm)
	fmt.Fprintf(ctx.Stdout, " - MinSize: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().Chunking.MinSize)), repo.Configuration().Chunking.MinSize)
	fmt.Fprintf(ctx.Stdout, " - NormalSize: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().Chunking.NormalSize)), repo.Configuration().Chunking.NormalSize)
	fmt.Fprintf(ctx.Stdout, " - MaxSize: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().Chunking.MaxSize)), repo.Configuration().Chunking.MaxSize)

	fmt.Fprintln(ctx.Stdout, "Hashing:")
	fmt.Fprintln(ctx.Stdout, " - Algorithm:", repo.Configuration().Hashing.Algorithm)
	fmt.Fprintln(ctx.Stdout, " - Bits:", repo.Configuration().Hashing.Bits)

	if repo.Configuration().Compression != nil {
		fmt.Fprintln(ctx.Stdout, "Compression:")
		fmt.Fprintln(ctx.Stdout, " - Algorithm:", repo.Configuration().Compression.Algorithm)
		fmt.Fprintln(ctx.Stdout, " - Level:", repo.Configuration().Compression.Level)
	}

	if repo.Configuration().Encryption != nil {
		fmt.Fprintln(ctx.Stdout, "Encryption:")
		fmt.Fprintln(ctx.Stdout, " - Algorithm:", repo.Configuration().Encryption.Algorithm)
		fmt.Fprintf(ctx.Stdout, " - Canary: %x\n", repo.Configuration().Encryption.Canary)
		fmt.Fprintln(ctx.Stdout, " - KDF:", repo.Configuration().Encryption.KDF)
		fmt.Fprintln(ctx.Stdout, " - KDFParams:")
		fmt.Fprintf(ctx.Stdout, "   - N: %d\n", repo.Configuration().Encryption.KDFParams.N)
		fmt.Fprintf(ctx.Stdout, "   - R: %d\n", repo.Configuration().Encryption.KDFParams.R)
		fmt.Fprintf(ctx.Stdout, "   - P: %d\n", repo.Configuration().Encryption.KDFParams.P)
		fmt.Fprintf(ctx.Stdout, "   - Salt: %x\n", repo.Configuration().Encryption.KDFParams.Salt)
		fmt.Fprintf(ctx.Stdout, "   - KeyLen: %d\n", repo.Configuration().Encryption.KDFParams.KeyLen)
	}

	fmt.Fprintln(ctx.Stdout, "Snapshots:", len(metadatas))
	totalSize := uint64(0)
	for _, metadata := range metadatas {
		totalSize += metadata.GetSource(0).Summary.Directory.Size + metadata.GetSource(0).Summary.Below.Size
	}
	fmt.Fprintf(ctx.Stdout, "Size: %s (%d bytes)\n", humanize.Bytes(totalSize), totalSize)

	return 0, nil
}
