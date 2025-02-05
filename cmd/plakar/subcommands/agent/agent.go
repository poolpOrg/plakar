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
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/PlakarKorp/plakar/agent"
	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/archive"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/backup"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/cat"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/check"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/checksum"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/cleanup"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/clone"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/diff"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/exec"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/info"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/locate"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/ls"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/mount"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/restore"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/rm"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/server"
	cmd_sync "github.com/PlakarKorp/plakar/cmd/plakar/subcommands/sync"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/ui"
	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	subcommands.Register(&Agent{}, "agent")
}

type Agent struct {
	prometheus string
	socketPath string

	listener net.Listener
}

func (cmd *Agent) Parse(ctx *appcontext.AppContext, repo *repository.Repository, args []string) error {
	flags := flag.NewFlagSet("agent", flag.ExitOnError)
	flags.StringVar(&cmd.prometheus, "prometheus", "", "prometheus exporter interface")
	flags.Parse(args)

	cmd.socketPath = filepath.Join(ctx.CacheDir, "agent.sock")
	return nil
}

func (cmd *Agent) checkSocket() bool {
	conn, err := net.Dial("unix", cmd.socketPath)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (cmd *Agent) Close() error {
	if cmd.listener != nil {
		cmd.listener.Close()
	}
	if err := os.Remove(cmd.socketPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func isDisconnectError(err error) bool {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (cmd *Agent) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	if err := cmd.ListenAndServe(ctx); err != nil {
		return 1, err
	}
	log.Println("Server gracefully stopped")
	return 0, nil
}

func (cmd *Agent) ListenAndServe(ctx *appcontext.AppContext) error {
	if _, err := os.Stat(cmd.socketPath); err == nil {
		if !cmd.checkSocket() {
			cmd.Close()
		} else {
			return fmt.Errorf("already running")
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	var err error

	// Bind socket
	cmd.listener, err = net.Listen("unix", cmd.socketPath)
	if err != nil {
		return fmt.Errorf("failed to bind socket: %w", err)
	}
	defer os.Remove(cmd.socketPath)

	// Set socket permissions
	if err := os.Chmod(cmd.socketPath, 0600); err != nil {
		cmd.Close()
		return fmt.Errorf("failed to set socket permissions: %w", err)
	}

	cancelCtx, _ := context.WithCancel(context.Background())

	if cmd.prometheus != "" {
		promlistener, err := net.Listen("tcp", cmd.prometheus)
		if err != nil {
			return fmt.Errorf("failed to bind prometheus listener: %w", err)
		}
		defer promlistener.Close()

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			http.Serve(promlistener, nil)
		}()
	}

	var wg sync.WaitGroup

	for {
		select {
		case <-cancelCtx.Done():
			return nil
		default:
		}

		conn, err := cmd.listener.Accept()
		if err != nil {
			select {
			case <-cancelCtx.Done():
				return nil
			default:
				if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
					return nil
				}
				return fmt.Errorf("failed to accept connection: %w", err)
			}
		}

		wg.Add(1)

		go func(_conn net.Conn) {
			defer _conn.Close()
			defer wg.Done()

			mu := sync.Mutex{}

			encoder := msgpack.NewEncoder(_conn)
			var encodingErrorOccurred bool

			// Create a context tied to the connection
			cancelCtx, cancel := context.WithCancel(context.Background())
			defer cancel()

			clientContext := appcontext.NewAppContextFrom(ctx)
			clientContext.SetContext(cancelCtx)
			defer clientContext.Close()

			processStdout := func(data string) {
				if encodingErrorOccurred {
					return
				}
				select {
				case <-clientContext.GetContext().Done():
					return
				default:
					response := agent.Packet{
						Type: "stdout",
						Data: []byte(data),
					}
					mu.Lock()
					if err := encoder.Encode(&response); err != nil {
						encodingErrorOccurred = true
					}
					mu.Unlock()
				}
			}

			processStderr := func(data string) {
				if encodingErrorOccurred {
					return
				}

				select {
				case <-clientContext.GetContext().Done():
					return
				default:
					response := agent.Packet{
						Type: "stderr",
						Data: []byte(data),
					}
					mu.Lock()
					if err := encoder.Encode(&response); err != nil {
						encodingErrorOccurred = true
					}
					mu.Unlock()

				}
			}

			clientContext.Stdout = &CustomWriter{processFunc: processStdout}
			clientContext.Stderr = &CustomWriter{processFunc: processStderr}

			logger := logging.NewLogger(clientContext.Stdout, clientContext.Stderr)
			logger.EnableInfo()
			clientContext.SetLogger(logger)

			decoder := msgpack.NewDecoder(conn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable to initialize decoder\n")
				return
			}

			name, request, err := subcommands.DecodeRPC(decoder)
			if err != nil {
				if isDisconnectError(err) {
					fmt.Fprintf(os.Stderr, "Client disconnected during initial request\n")
					cancel() // Cancel the context on disconnect
					return
				}
				fmt.Fprintf(os.Stderr, "%s\n", err)
				return
			}

			// Monitor the connection for subsequent disconnection
			go func() {
				// Attempt another decode to detect client disconnection during processing
				var tmp interface{}
				if err := decoder.Decode(&tmp); err != nil {
					if isDisconnectError(err) {
						handleDisconnect()
						cancel()
					}
				}
			}()

			var subcommand subcommands.RPC
			var repositoryLocation string
			var repositorySecret []byte

			switch name {
			case (&cat.Cat{}).Name():
				var cmd struct {
					Name       string
					Subcommand cat.Cat
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&ls.Ls{}).Name():
				var cmd struct {
					Name       string
					Subcommand ls.Ls
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&backup.Backup{}).Name():
				var cmd struct {
					Name       string
					Subcommand backup.Backup
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&info.InfoRepository{}).Name():
				var cmd struct {
					Name       string
					Subcommand info.InfoRepository
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&info.InfoSnapshot{}).Name():
				var cmd struct {
					Name       string
					Subcommand info.InfoSnapshot
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&info.InfoErrors{}).Name():
				var cmd struct {
					Name       string
					Subcommand info.InfoErrors
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&info.InfoState{}).Name():
				var cmd struct {
					Name       string
					Subcommand info.InfoState
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&info.InfoPackfile{}).Name():
				var cmd struct {
					Name       string
					Subcommand info.InfoPackfile
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&info.InfoObject{}).Name():
				var cmd struct {
					Name       string
					Subcommand info.InfoObject
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&info.InfoVFS{}).Name():
				var cmd struct {
					Name       string
					Subcommand info.InfoVFS
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&rm.Rm{}).Name():
				var cmd struct {
					Name       string
					Subcommand rm.Rm
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&checksum.Checksum{}).Name():
				var cmd struct {
					Name       string
					Subcommand checksum.Checksum
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&locate.Locate{}).Name():
				var cmd struct {
					Name       string
					Subcommand locate.Locate
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&check.Check{}).Name():
				var cmd struct {
					Name       string
					Subcommand check.Check
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&cleanup.Cleanup{}).Name():
				var cmd struct {
					Name       string
					Subcommand cleanup.Cleanup
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&clone.Clone{}).Name():
				var cmd struct {
					Name       string
					Subcommand clone.Clone
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&archive.Archive{}).Name():
				var cmd struct {
					Name       string
					Subcommand archive.Archive
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&diff.Diff{}).Name():
				var cmd struct {
					Name       string
					Subcommand diff.Diff
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&exec.Exec{}).Name():
				var cmd struct {
					Name       string
					Subcommand exec.Exec
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&mount.Mount{}).Name():
				var cmd struct {
					Name       string
					Subcommand mount.Mount
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&restore.Restore{}).Name():
				var cmd struct {
					Name       string
					Subcommand restore.Restore
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&server.Server{}).Name():
				var cmd struct {
					Name       string
					Subcommand server.Server
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&cmd_sync.Sync{}).Name():
				var cmd struct {
					Name       string
					Subcommand cmd_sync.Sync
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&ui.Ui{}).Name():
				var cmd struct {
					Name       string
					Subcommand ui.Ui
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			}

			var repo *repository.Repository

			if repositoryLocation != "" {
				store, err := storage.Open(repositoryLocation)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to open storage: %s\n", err)
					return
				}
				defer store.Close()

				repo, err = repository.New(clientContext, store, clientContext.GetSecret())
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to open repository: %s\n", err)
					return
				}
				defer repo.Close()
			}

			if repositorySecret != nil {
				clientContext.SetSecret(repositorySecret)
			}

			eventsDone := make(chan struct{})
			eventsChan := clientContext.Events().Listen()
			go func() {
				for evt := range eventsChan {
					serialized, err := events.Serialize(evt)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to serialize event: %s\n", err)
						return
					}
					// Send the event to the client
					response := agent.Packet{
						Type: "event",
						Data: serialized,
					}
					select {
					case <-clientContext.GetContext().Done():
						return
					default:
						mu.Lock()
						err = encoder.Encode(&response)
						mu.Unlock()
						if err != nil {
							fmt.Fprintf(os.Stderr, "Failed to encode event: %s\n", err)
							return
						}
					}
				}
				eventsDone <- struct{}{}
			}()

			status, err := subcommand.Execute(clientContext, repo)

			clientContext.Close()
			<-eventsDone

			select {
			case <-clientContext.GetContext().Done():
				return
			default:
				errStr := ""
				if err != nil {
					errStr = err.Error()
				}
				response := agent.Packet{
					Type:     "exit",
					ExitCode: status,
					Err:      errStr,
				}
				mu.Lock()
				if err := encoder.Encode(&response); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to encode response: %s\n", err)
				}
				mu.Unlock()
			}

		}(conn)
	}
}

type CustomWriter struct {
	processFunc func(string) // Function to handle the log lines
}

// Write implements the `io.Writer` interface.
func (cw *CustomWriter) Write(p []byte) (n int, err error) {
	cw.processFunc(string(p))
	return len(p), nil
}
