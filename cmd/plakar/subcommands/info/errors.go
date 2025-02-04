package info

import (
	"fmt"
	"strings"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/repository"
)

type InfoErrors struct {
	RepositoryLocation string
	RepositorySecret   []byte

	SnapshotID string
}

func (cmd *InfoErrors) Name() string {
	return "info_errors"
}

func (cmd *InfoErrors) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	prefix, pathname := utils.ParseSnapshotID(cmd.SnapshotID)
	if !strings.HasSuffix(pathname, "/") {
		pathname = pathname + "/"
	}

	snap, err := utils.OpenSnapshotByPrefix(repo, prefix)
	if err != nil {
		return 1, err
	}
	defer snap.Close()

	errstream, err := snap.Errors(pathname)
	if err != nil {
		return 1, err
	}

	for item := range errstream {
		fmt.Fprintf(ctx.Stdout, "%s: %s\n", item.Name, item.Error)
	}
	return 0, nil
}
