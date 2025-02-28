package diag

import (
	"fmt"
	"path"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/dustin/go-humanize"
)

type DiagVFS struct {
	RepositoryLocation string
	RepositorySecret   []byte

	SnapshotPath string
}

func (cmd *DiagVFS) Name() string {
	return "diag_vfs"
}

func (cmd *DiagVFS) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	snap1, pathname, err := utils.OpenSnapshotByPath(repo, cmd.SnapshotPath)
	if err != nil {
		return 1, err
	}
	defer snap1.Close()

	fs, err := snap1.Filesystem()
	if err != nil {
		return 1, err
	}

	pathname = path.Clean(pathname)
	entry, err := fs.GetEntry(pathname)
	if err != nil {
		return 1, err
	}

	if entry.Stat().Mode().IsDir() {
		fmt.Fprintf(ctx.Stdout, "[DirEntry]\n")
	} else {
		fmt.Fprintf(ctx.Stdout, "[FileEntry]\n")
	}

	fmt.Fprintf(ctx.Stdout, "Version: %d\n", entry.Version)
	fmt.Fprintf(ctx.Stdout, "ParentPath: %s\n", entry.ParentPath)
	fmt.Fprintf(ctx.Stdout, "Name: %s\n", entry.Stat().Name())
	fmt.Fprintf(ctx.Stdout, "Size: %s (%d bytes)\n", humanize.Bytes(uint64(entry.Stat().Size())), entry.Stat().Size())
	fmt.Fprintf(ctx.Stdout, "Permissions: %s\n", entry.Stat().Mode())
	fmt.Fprintf(ctx.Stdout, "ModTime: %s\n", entry.Stat().ModTime())
	fmt.Fprintf(ctx.Stdout, "DeviceID: %d\n", entry.Stat().Dev())
	fmt.Fprintf(ctx.Stdout, "InodeID: %d\n", entry.Stat().Ino())
	fmt.Fprintf(ctx.Stdout, "UserID: %d\n", entry.Stat().Uid())
	fmt.Fprintf(ctx.Stdout, "GroupID: %d\n", entry.Stat().Gid())
	fmt.Fprintf(ctx.Stdout, "Username: %s\n", entry.Stat().Username())
	fmt.Fprintf(ctx.Stdout, "Groupname: %s\n", entry.Stat().Groupname())
	fmt.Fprintf(ctx.Stdout, "NumLinks: %d\n", entry.Stat().Nlink())
	fmt.Fprintf(ctx.Stdout, "ExtendedAttributes: %s\n", entry.ExtendedAttributes)
	fmt.Fprintf(ctx.Stdout, "FileAttributes: %v\n", entry.FileAttributes)
	if entry.SymlinkTarget != "" {
		fmt.Fprintf(ctx.Stdout, "SymlinkTarget: %s\n", entry.SymlinkTarget)
	}
	fmt.Fprintf(ctx.Stdout, "Classification:\n")
	for _, classification := range entry.Classifications {
		fmt.Fprintf(ctx.Stdout, " - %s:\n", classification.Analyzer)
		for _, class := range classification.Classes {
			fmt.Fprintf(ctx.Stdout, "   - %s\n", class)
		}
	}
	fmt.Fprintf(ctx.Stdout, "CustomMetadata: %s\n", entry.CustomMetadata)
	fmt.Fprintf(ctx.Stdout, "Tags: %s\n", entry.Tags)
	fmt.Fprintf(ctx.Stdout, "ExtendedAttributes: %v\n", entry.ExtendedAttributes)

	if entry.Summary != nil {
		fmt.Fprintf(ctx.Stdout, "Below.Directories: %d\n", entry.Summary.Below.Directories)
		fmt.Fprintf(ctx.Stdout, "Below.Files: %d\n", entry.Summary.Below.Files)
		fmt.Fprintf(ctx.Stdout, "Below.Symlinks: %d\n", entry.Summary.Below.Symlinks)
		fmt.Fprintf(ctx.Stdout, "Below.Devices: %d\n", entry.Summary.Below.Devices)
		fmt.Fprintf(ctx.Stdout, "Below.Pipes: %d\n", entry.Summary.Below.Pipes)
		fmt.Fprintf(ctx.Stdout, "Below.Sockets: %d\n", entry.Summary.Below.Sockets)
		fmt.Fprintf(ctx.Stdout, "Below.Setuid: %d\n", entry.Summary.Below.Setuid)
		fmt.Fprintf(ctx.Stdout, "Below.Setgid: %d\n", entry.Summary.Below.Setgid)
		fmt.Fprintf(ctx.Stdout, "Below.Sticky: %d\n", entry.Summary.Below.Sticky)
		fmt.Fprintf(ctx.Stdout, "Below.Objects: %d\n", entry.Summary.Below.Objects)
		fmt.Fprintf(ctx.Stdout, "Below.Chunks: %d\n", entry.Summary.Below.Chunks)
		fmt.Fprintf(ctx.Stdout, "Below.MinSize: %s (%d bytes)\n", humanize.Bytes(uint64(entry.Summary.Below.MinSize)), entry.Summary.Below.MinSize)
		fmt.Fprintf(ctx.Stdout, "Below.MaxSize: %s (%d bytes)\n", humanize.Bytes(uint64(entry.Summary.Below.MaxSize)), entry.Summary.Below.MaxSize)
		fmt.Fprintf(ctx.Stdout, "Below.Size: %s (%d bytes)\n", humanize.Bytes(uint64(entry.Summary.Below.Size)), entry.Summary.Below.Size)
		fmt.Fprintf(ctx.Stdout, "Below.MinModTime: %s\n", time.Unix(entry.Summary.Below.MinModTime, 0))
		fmt.Fprintf(ctx.Stdout, "Below.MaxModTime: %s\n", time.Unix(entry.Summary.Below.MaxModTime, 0))
		fmt.Fprintf(ctx.Stdout, "Below.MinEntropy: %f\n", entry.Summary.Below.MinEntropy)
		fmt.Fprintf(ctx.Stdout, "Below.MaxEntropy: %f\n", entry.Summary.Below.MaxEntropy)
		fmt.Fprintf(ctx.Stdout, "Below.HiEntropy: %d\n", entry.Summary.Below.HiEntropy)
		fmt.Fprintf(ctx.Stdout, "Below.LoEntropy: %d\n", entry.Summary.Below.LoEntropy)
		fmt.Fprintf(ctx.Stdout, "Below.MIMEAudio: %d\n", entry.Summary.Below.MIMEAudio)
		fmt.Fprintf(ctx.Stdout, "Below.MIMEVideo: %d\n", entry.Summary.Below.MIMEVideo)
		fmt.Fprintf(ctx.Stdout, "Below.MIMEImage: %d\n", entry.Summary.Below.MIMEImage)
		fmt.Fprintf(ctx.Stdout, "Below.MIMEText: %d\n", entry.Summary.Below.MIMEText)
		fmt.Fprintf(ctx.Stdout, "Below.MIMEApplication: %d\n", entry.Summary.Below.MIMEApplication)
		fmt.Fprintf(ctx.Stdout, "Below.MIMEOther: %d\n", entry.Summary.Below.MIMEOther)
		fmt.Fprintf(ctx.Stdout, "Below.Errors: %d\n", entry.Summary.Below.Errors)
		fmt.Fprintf(ctx.Stdout, "Directory.Directories: %d\n", entry.Summary.Directory.Directories)
		fmt.Fprintf(ctx.Stdout, "Directory.Files: %d\n", entry.Summary.Directory.Files)
		fmt.Fprintf(ctx.Stdout, "Directory.Symlinks: %d\n", entry.Summary.Directory.Symlinks)
		fmt.Fprintf(ctx.Stdout, "Directory.Devices: %d\n", entry.Summary.Directory.Devices)
		fmt.Fprintf(ctx.Stdout, "Directory.Pipes: %d\n", entry.Summary.Directory.Pipes)
		fmt.Fprintf(ctx.Stdout, "Directory.Sockets: %d\n", entry.Summary.Directory.Sockets)
		fmt.Fprintf(ctx.Stdout, "Directory.Setuid: %d\n", entry.Summary.Directory.Setuid)
		fmt.Fprintf(ctx.Stdout, "Directory.Setgid: %d\n", entry.Summary.Directory.Setgid)
		fmt.Fprintf(ctx.Stdout, "Directory.Sticky: %d\n", entry.Summary.Directory.Sticky)
		fmt.Fprintf(ctx.Stdout, "Directory.Objects: %d\n", entry.Summary.Directory.Objects)
		fmt.Fprintf(ctx.Stdout, "Directory.Chunks: %d\n", entry.Summary.Directory.Chunks)
		fmt.Fprintf(ctx.Stdout, "Directory.MinSize: %s (%d bytes)\n", humanize.Bytes(uint64(entry.Summary.Directory.MinSize)), entry.Summary.Directory.MinSize)
		fmt.Fprintf(ctx.Stdout, "Directory.MaxSize: %s (%d bytes)\n", humanize.Bytes(uint64(entry.Summary.Directory.MaxSize)), entry.Summary.Directory.MaxSize)
		fmt.Fprintf(ctx.Stdout, "Directory.Size: %s (%d bytes)\n", humanize.Bytes(uint64(entry.Summary.Directory.Size)), entry.Summary.Directory.Size)
		fmt.Fprintf(ctx.Stdout, "Directory.MinModTime: %s\n", time.Unix(entry.Summary.Directory.MinModTime, 0))
		fmt.Fprintf(ctx.Stdout, "Directory.MaxModTime: %s\n", time.Unix(entry.Summary.Directory.MaxModTime, 0))
		fmt.Fprintf(ctx.Stdout, "Directory.MinEntropy: %f\n", entry.Summary.Directory.MinEntropy)
		fmt.Fprintf(ctx.Stdout, "Directory.MaxEntropy: %f\n", entry.Summary.Directory.MaxEntropy)
		fmt.Fprintf(ctx.Stdout, "Directory.AvgEntropy: %f\n", entry.Summary.Directory.AvgEntropy)
		fmt.Fprintf(ctx.Stdout, "Directory.HiEntropy: %d\n", entry.Summary.Directory.HiEntropy)
		fmt.Fprintf(ctx.Stdout, "Directory.LoEntropy: %d\n", entry.Summary.Directory.LoEntropy)
		fmt.Fprintf(ctx.Stdout, "Directory.MIMEAudio: %d\n", entry.Summary.Directory.MIMEAudio)
		fmt.Fprintf(ctx.Stdout, "Directory.MIMEVideo: %d\n", entry.Summary.Directory.MIMEVideo)
		fmt.Fprintf(ctx.Stdout, "Directory.MIMEImage: %d\n", entry.Summary.Directory.MIMEImage)
		fmt.Fprintf(ctx.Stdout, "Directory.MIMEText: %d\n", entry.Summary.Directory.MIMEText)
		fmt.Fprintf(ctx.Stdout, "Directory.MIMEApplication: %d\n", entry.Summary.Directory.MIMEApplication)
		fmt.Fprintf(ctx.Stdout, "Directory.MIMEOther: %d\n", entry.Summary.Directory.MIMEOther)
		fmt.Fprintf(ctx.Stdout, "Directory.Errors: %d\n", entry.Summary.Directory.Errors)
		fmt.Fprintf(ctx.Stdout, "Directory.Children: %d\n", entry.Summary.Directory.Children)
	}

	iter, err := entry.Getdents(fs)
	if err != nil {
		return 1, err
	}
	offset := 0
	for child := range iter {
		fmt.Fprintf(ctx.Stdout, "Child[%d].FileInfo.Name(): %s\n", offset, child.Stat().Name())
		fmt.Fprintf(ctx.Stdout, "Child[%d].FileInfo.Size(): %d\n", offset, child.Stat().Size())
		fmt.Fprintf(ctx.Stdout, "Child[%d].FileInfo.Mode(): %s\n", offset, child.Stat().Mode())
		fmt.Fprintf(ctx.Stdout, "Child[%d].FileInfo.Dev(): %d\n", offset, child.Stat().Dev())
		fmt.Fprintf(ctx.Stdout, "Child[%d].FileInfo.Ino(): %d\n", offset, child.Stat().Ino())
		fmt.Fprintf(ctx.Stdout, "Child[%d].FileInfo.Uid(): %d\n", offset, child.Stat().Uid())
		fmt.Fprintf(ctx.Stdout, "Child[%d].FileInfo.Gid(): %d\n", offset, child.Stat().Gid())
		fmt.Fprintf(ctx.Stdout, "Child[%d].FileInfo.Username(): %s\n", offset, child.Stat().Username())
		fmt.Fprintf(ctx.Stdout, "Child[%d].FileInfo.Groupname(): %s\n", offset, child.Stat().Groupname())
		fmt.Fprintf(ctx.Stdout, "Child[%d].FileInfo.Nlink(): %d\n", offset, child.Stat().Nlink())
		fmt.Fprintf(ctx.Stdout, "Child[%d].ExtendedAttributes(): %v\n", offset, child.ExtendedAttributes)
		offset++
	}

	errors, err := fs.Errors(pathname)
	if err != nil {
		return 1, err
	}
	offset = 0
	for err := range errors {
		fmt.Fprintf(ctx.Stdout, "Error[%d]: %s: %s\n", offset, err.Name, err.Error)
		offset++
	}
	return 0, nil
}
