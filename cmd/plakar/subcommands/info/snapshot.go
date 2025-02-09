package info

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
)

type InfoSnapshot struct {
	RepositoryLocation string
	RepositorySecret   []byte

	SnapshotID string
}

func (cmd *InfoSnapshot) Name() string {
	return "info_snapshot"
}

func (cmd *InfoSnapshot) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	snap, err := utils.OpenSnapshotByPrefix(repo, cmd.SnapshotID)
	if err != nil {
		return 1, err
	}
	defer snap.Close()

	header := snap.Header

	indexID := header.GetIndexID()
	fmt.Fprintf(ctx.Stdout, "Version: %s\n", repo.Configuration().Version)
	fmt.Fprintf(ctx.Stdout, "SnapshotID: %s\n", hex.EncodeToString(indexID[:]))
	fmt.Fprintf(ctx.Stdout, "Timestamp: %s\n", header.Timestamp)
	fmt.Fprintf(ctx.Stdout, "Duration: %s\n", header.Duration)

	fmt.Fprintf(ctx.Stdout, "Name: %s\n", header.Name)
	fmt.Fprintf(ctx.Stdout, "Environment: %s\n", header.Environment)
	fmt.Fprintf(ctx.Stdout, "Perimeter: %s\n", header.Perimeter)
	fmt.Fprintf(ctx.Stdout, "Category: %s\n", header.Category)
	if len(header.Tags) > 0 {
		fmt.Fprintf(ctx.Stdout, "Tags: %s\n", strings.Join(header.Tags, ", "))
	}

	if header.Identity.Identifier != uuid.Nil {
		fmt.Fprintln(ctx.Stdout, "Identity:")
		fmt.Fprintf(ctx.Stdout, " - Identifier: %s\n", header.Identity.Identifier)
		fmt.Fprintf(ctx.Stdout, " - PublicKey: %s\n", base64.RawStdEncoding.EncodeToString(header.Identity.PublicKey))
	}

	fmt.Fprintf(ctx.Stdout, "VFS: %x\n", header.GetSource(0).VFS)

	fmt.Fprintln(ctx.Stdout, "Importer:")
	fmt.Fprintf(ctx.Stdout, " - Type: %s\n", header.GetSource(0).Importer.Type)
	fmt.Fprintf(ctx.Stdout, " - Origin: %s\n", header.GetSource(0).Importer.Origin)
	fmt.Fprintf(ctx.Stdout, " - Directory: %s\n", header.GetSource(0).Importer.Directory)

	fmt.Fprintln(ctx.Stdout, "Context:")
	fmt.Fprintf(ctx.Stdout, " - MachineID: %s\n", header.GetContext("MachineID"))
	fmt.Fprintf(ctx.Stdout, " - Hostname: %s\n", header.GetContext("Hostname"))
	fmt.Fprintf(ctx.Stdout, " - Username: %s\n", header.GetContext("Username"))
	fmt.Fprintf(ctx.Stdout, " - OperatingSystem: %s\n", header.GetContext("OperatingSystem"))
	fmt.Fprintf(ctx.Stdout, " - Architecture: %s\n", header.GetContext("Architecture"))
	fmt.Fprintf(ctx.Stdout, " - NumCPU: %s\n", header.GetContext("NumCPU"))
	fmt.Fprintf(ctx.Stdout, " - GOMAXPROCS: %s\n", header.GetContext("GOMAXPROCS"))
	fmt.Fprintf(ctx.Stdout, " - ProcessID: %s\n", header.GetContext("ProcessID"))
	fmt.Fprintf(ctx.Stdout, " - Client: %s\n", header.GetContext("Client"))
	fmt.Fprintf(ctx.Stdout, " - CommandLine: %s\n", header.GetContext("CommandLine"))

	fmt.Fprintln(ctx.Stdout, "Summary:")
	fmt.Fprintf(ctx.Stdout, " - Directories: %d\n", header.GetSource(0).Summary.Directory.Directories+header.GetSource(0).Summary.Below.Directories)
	fmt.Fprintf(ctx.Stdout, " - Files: %d\n", header.GetSource(0).Summary.Directory.Files+header.GetSource(0).Summary.Below.Files)
	fmt.Fprintf(ctx.Stdout, " - Symlinks: %d\n", header.GetSource(0).Summary.Directory.Symlinks+header.GetSource(0).Summary.Below.Symlinks)
	fmt.Fprintf(ctx.Stdout, " - Devices: %d\n", header.GetSource(0).Summary.Directory.Devices+header.GetSource(0).Summary.Below.Devices)
	fmt.Fprintf(ctx.Stdout, " - Pipes: %d\n", header.GetSource(0).Summary.Directory.Pipes+header.GetSource(0).Summary.Below.Pipes)
	fmt.Fprintf(ctx.Stdout, " - Sockets: %d\n", header.GetSource(0).Summary.Directory.Sockets+header.GetSource(0).Summary.Below.Sockets)
	fmt.Fprintf(ctx.Stdout, " - Setuid: %d\n", header.GetSource(0).Summary.Directory.Setuid+header.GetSource(0).Summary.Below.Setuid)
	fmt.Fprintf(ctx.Stdout, " - Setgid: %d\n", header.GetSource(0).Summary.Directory.Setgid+header.GetSource(0).Summary.Below.Setgid)
	fmt.Fprintf(ctx.Stdout, " - Sticky: %d\n", header.GetSource(0).Summary.Directory.Sticky+header.GetSource(0).Summary.Below.Sticky)

	fmt.Fprintf(ctx.Stdout, " - Objects: %d\n", header.GetSource(0).Summary.Directory.Objects+header.GetSource(0).Summary.Below.Objects)
	fmt.Fprintf(ctx.Stdout, " - Chunks: %d\n", header.GetSource(0).Summary.Directory.Chunks+header.GetSource(0).Summary.Below.Chunks)
	fmt.Fprintf(ctx.Stdout, " - MinSize: %s (%d bytes)\n", humanize.Bytes(min(header.GetSource(0).Summary.Directory.MinSize, header.GetSource(0).Summary.Below.MinSize)), min(header.GetSource(0).Summary.Directory.MinSize, header.GetSource(0).Summary.Below.MinSize))
	fmt.Fprintf(ctx.Stdout, " - MaxSize: %s (%d bytes)\n", humanize.Bytes(max(header.GetSource(0).Summary.Directory.MaxSize, header.GetSource(0).Summary.Below.MaxSize)), max(header.GetSource(0).Summary.Directory.MaxSize, header.GetSource(0).Summary.Below.MaxSize))
	fmt.Fprintf(ctx.Stdout, " - Size: %s (%d bytes)\n", humanize.Bytes(header.GetSource(0).Summary.Directory.Size+header.GetSource(0).Summary.Below.Size), header.GetSource(0).Summary.Directory.Size+header.GetSource(0).Summary.Below.Size)
	fmt.Fprintf(ctx.Stdout, " - MinModTime: %s\n", time.Unix(min(header.GetSource(0).Summary.Directory.MinModTime, header.GetSource(0).Summary.Below.MinModTime), 0))
	fmt.Fprintf(ctx.Stdout, " - MaxModTime: %s\n", time.Unix(max(header.GetSource(0).Summary.Directory.MaxModTime, header.GetSource(0).Summary.Below.MaxModTime), 0))
	fmt.Fprintf(ctx.Stdout, " - MinEntropy: %f\n", min(header.GetSource(0).Summary.Directory.MinEntropy, header.GetSource(0).Summary.Below.MinEntropy))
	fmt.Fprintf(ctx.Stdout, " - MaxEntropy: %f\n", max(header.GetSource(0).Summary.Directory.MaxEntropy, header.GetSource(0).Summary.Below.MaxEntropy))
	fmt.Fprintf(ctx.Stdout, " - HiEntropy: %d\n", header.GetSource(0).Summary.Directory.HiEntropy+header.GetSource(0).Summary.Below.HiEntropy)
	fmt.Fprintf(ctx.Stdout, " - LoEntropy: %d\n", header.GetSource(0).Summary.Directory.LoEntropy+header.GetSource(0).Summary.Below.LoEntropy)
	fmt.Fprintf(ctx.Stdout, " - MIMEAudio: %d\n", header.GetSource(0).Summary.Directory.MIMEAudio+header.GetSource(0).Summary.Below.MIMEAudio)
	fmt.Fprintf(ctx.Stdout, " - MIMEVideo: %d\n", header.GetSource(0).Summary.Directory.MIMEVideo+header.GetSource(0).Summary.Below.MIMEVideo)
	fmt.Fprintf(ctx.Stdout, " - MIMEImage: %d\n", header.GetSource(0).Summary.Directory.MIMEImage+header.GetSource(0).Summary.Below.MIMEImage)
	fmt.Fprintf(ctx.Stdout, " - MIMEText: %d\n", header.GetSource(0).Summary.Directory.MIMEText+header.GetSource(0).Summary.Below.MIMEText)
	fmt.Fprintf(ctx.Stdout, " - MIMEApplication: %d\n", header.GetSource(0).Summary.Directory.MIMEApplication+header.GetSource(0).Summary.Below.MIMEApplication)
	fmt.Fprintf(ctx.Stdout, " - MIMEOther: %d\n", header.GetSource(0).Summary.Directory.MIMEOther+header.GetSource(0).Summary.Below.MIMEOther)

	fmt.Fprintf(ctx.Stdout, " - Errors: %d\n", header.GetSource(0).Summary.Directory.Errors+header.GetSource(0).Summary.Below.Errors)
	return 0, nil
}
