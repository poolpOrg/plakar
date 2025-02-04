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
		repo.Logger().Warn("%s", err)
		return 1, err
	}

	fmt.Println("Version:", repo.Configuration().Version)
	fmt.Println("Timestamp:", repo.Configuration().Timestamp)
	fmt.Println("RepositoryID:", repo.Configuration().RepositoryID)

	fmt.Println("Packfile:")
	fmt.Printf(" - MaxSize: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().Packfile.MaxSize)),
		repo.Configuration().Packfile.MaxSize)

	fmt.Println("Chunking:")
	fmt.Println(" - Algorithm:", repo.Configuration().Chunking.Algorithm)
	fmt.Printf(" - MinSize: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().Chunking.MinSize)), repo.Configuration().Chunking.MinSize)
	fmt.Printf(" - NormalSize: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().Chunking.NormalSize)), repo.Configuration().Chunking.NormalSize)
	fmt.Printf(" - MaxSize: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().Chunking.MaxSize)), repo.Configuration().Chunking.MaxSize)

	fmt.Println("Hashing:")
	fmt.Println(" - Algorithm:", repo.Configuration().Hashing.Algorithm)
	fmt.Println(" - Bits:", repo.Configuration().Hashing.Bits)

	if repo.Configuration().Compression != nil {
		fmt.Println("Compression:")
		fmt.Println(" - Algorithm:", repo.Configuration().Compression.Algorithm)
		fmt.Println(" - Level:", repo.Configuration().Compression.Level)
	}

	if repo.Configuration().Encryption != nil {
		fmt.Println("Encryption:")
		fmt.Println(" - Algorithm:", repo.Configuration().Encryption.Algorithm)
		fmt.Printf(" - Canary: %x\n", repo.Configuration().Encryption.Canary)
		fmt.Println(" - KDF:", repo.Configuration().Encryption.KDF)
		fmt.Println(" - KDFParams:")
		fmt.Printf("   - N: %d\n", repo.Configuration().Encryption.KDFParams.N)
		fmt.Printf("   - R: %d\n", repo.Configuration().Encryption.KDFParams.R)
		fmt.Printf("   - P: %d\n", repo.Configuration().Encryption.KDFParams.P)
		fmt.Printf("   - Salt: %x\n", repo.Configuration().Encryption.KDFParams.Salt)
		fmt.Printf("   - KeyLen: %d\n", repo.Configuration().Encryption.KDFParams.KeyLen)
	}

	fmt.Println("Snapshots:", len(metadatas))
	totalSize := uint64(0)
	for _, metadata := range metadatas {
		totalSize += metadata.GetSource(0).Summary.Directory.Size + metadata.GetSource(0).Summary.Below.Size
	}
	fmt.Printf("Size: %s (%d bytes)\n", humanize.Bytes(totalSize), totalSize)

	return 0, nil
}
