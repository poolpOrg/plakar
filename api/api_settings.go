package api

import (
	"encoding/json"
	"net/http"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/config"
)

func settingsView(ctx *appcontext.AppContext) func(w http.ResponseWriter, r *http.Request) error {
	return func(w http.ResponseWriter, r *http.Request) error {
		return json.NewEncoder(w).Encode(Item[config.Config]{
			Item: *ctx.Config,
		})
	}
}
