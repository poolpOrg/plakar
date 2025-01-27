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

package agent

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/PlakarKorp/plakar/agent"
	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	subcommands.Register("agent", cmd_agent)
}

func cmd_agent(ctx *appcontext.AppContext, _ *repository.Repository, args []string) int {
	var opt_socketPath string

	flags := flag.NewFlagSet("agent", flag.ExitOnError)
	flags.StringVar(&opt_socketPath, "socket", filepath.Join(ctx.CacheDir, "agent.sock"), "path to socket file")
	flags.Parse(args)

	daemon, err := agent.NewAgent(ctx, "unix", opt_socketPath)
	if err != nil {
		ctx.GetLogger().Error("failed to create agent daemon: %s", err)
		return 1
	}
	defer daemon.Close()

	go func() {
		if err := daemon.ListenAndServe(handleClientRequest); err != nil {
			ctx.GetLogger().Error("%s", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	<-quit
	fmt.Println("Shutting down server...")

	sigctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shutdown the server gracefully
	if err := daemon.Shutdown(sigctx); err != nil {
		log.Fatalf("Server shutdown failed: %s", err)
	}

	log.Println("Server gracefully stopped")

	return 0
}

func handleClientRequest(serverContext *appcontext.AppContext, conn net.Conn) (int, error) {
	decoder := msgpack.NewDecoder(conn)

	var request agent.CommandRequest
	if err := decoder.Decode(&request); err != nil {
		fmt.Println("Failed to decode client request:", err)
		return 1, err
	}

	clientContext := appcontext.NewAppContextFrom(serverContext)

	// Create a context tied to the connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientContext.SetContext(ctx)

	store, err := storage.Open(request.Repository)
	if err != nil {
		fmt.Println("Failed to open storage:", err)
		return 1, err
	}
	defer store.Close()

	repo, err := repository.New(clientContext, store, nil)
	if err != nil {
		fmt.Println("Failed to open repository:", err)
		return 1, err
	}
	defer repo.Close()

	return subcommands.Execute(clientContext, repo, request.Cmd, request.Argv, true)
}
