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
	"syscall"

	"github.com/PlakarKorp/plakar/agent"
	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/archive"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/backup"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/cat"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/check"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/cleanup"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/clone"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/diag"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/diff"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/digest"
	cmd_exec "github.com/PlakarKorp/plakar/cmd/plakar/subcommands/exec"
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
	"github.com/PlakarKorp/plakar/scheduler"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	subcommands.Register("agent", parse_cmd_agent)
}

func daemonize(argv []string) error {
	binary, err := os.Executable()
	if err != nil {
		return err
	}

	procAttr := syscall.ProcAttr{}
	procAttr.Files = []uintptr{
		uintptr(syscall.Stdin),
		uintptr(syscall.Stdout),
		uintptr(syscall.Stderr),
	}
	procAttr.Env = append(os.Environ(),
		"REEXEC=1",
	)

	pid, err := syscall.ForkExec(binary, argv, &procAttr)
	if err != nil {
		return err
	}
	fmt.Printf("agent started with pid=%d\n", pid)
	os.Exit(0)
	return nil
}

func parse_cmd_agent(ctx *appcontext.AppContext, repo *repository.Repository, args []string) (subcommands.Subcommand, error) {
	var opt_foreground bool
	var opt_stop bool
	//var opt_prometheus string
	var opt_tasks string
	var opt_logfile string

	flags := flag.NewFlagSet("agent", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s [OPTIONS]\n", flags.Name())
		fmt.Fprintf(flags.Output(), "\nOPTIONS:\n")
		flags.PrintDefaults()
	}

	flags.StringVar(&opt_tasks, "tasks", "", "tasks configuration file")
	//flags.StringVar(&opt_prometheus, "prometheus", "", "prometheus exporter interface, e.g. 127.0.0.1:9090")
	flags.BoolVar(&opt_foreground, "foreground", false, "run in foreground")
	flags.StringVar(&opt_logfile, "log", "", "log file")
	flags.BoolVar(&opt_stop, "stop", false, "stop the agent")
	flags.Parse(args)

	if opt_stop {
		client, err := agent.NewClient(filepath.Join(ctx.CacheDir, "agent.sock"))
		if err != nil {
			return nil, err
		}
		defer client.Close()

		retval, err := client.SendCommand(ctx, &AgentStop{}, nil)
		if err != nil {
			return nil, err
		}
		os.Exit(retval)
	}

	var schedConfig *scheduler.Configuration
	if opt_tasks != "" {
		tmp, err := scheduler.ParseConfigFile(opt_tasks)
		if err != nil {
			return nil, err
		}
		schedConfig = tmp
	}

	if !opt_foreground && os.Getenv("REEXEC") == "" {
		err := daemonize(os.Args)
		return nil, err
	}

	if opt_logfile != "" {
		f, err := os.OpenFile(opt_logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		ctx.GetLogger().SetOutput(f)
	}

	return &Agent{
		//prometheus:  opt_prometheus,
		socketPath:  filepath.Join(ctx.CacheDir, "agent.sock"),
		schedConfig: schedConfig,
	}, nil
}

type AgentStop struct{}

func (cmd *AgentStop) Name() string {
	return "agent-stop"
}
func (cmd *AgentStop) Execute(ctx *appcontext.AppContext, repo *repository.Repository) (int, error) {
	return 1, nil
}

type Agent struct {
	prometheus string
	socketPath string

	listener net.Listener

	schedConfig *scheduler.Configuration
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
	if cmd.schedConfig != nil {
		go func() {
			scheduler.NewScheduler(ctx, cmd.schedConfig).Run()
		}()
	}

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
		conn, err := cmd.listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
				return nil
			}
			return fmt.Errorf("failed to accept connection: %w", err)
		}

		wg.Add(1)

		go func(_conn net.Conn) {
			defer _conn.Close()
			defer wg.Done()

			mu := sync.Mutex{}

			var encodingErrorOccurred bool
			encoder := msgpack.NewEncoder(_conn)
			decoder := msgpack.NewDecoder(_conn)

			// Create a context tied to the connection
			cancelCtx, cancel := context.WithCancel(context.Background())
			defer cancel()

			clientContext := appcontext.NewAppContextFrom(ctx)
			clientContext.SetContext(cancelCtx)
			defer clientContext.Close()

			write := func(packet agent.Packet) {
				if encodingErrorOccurred {
					return
				}
				select {
				case <-clientContext.GetContext().Done():
					return
				default:
					mu.Lock()
					if err := encoder.Encode(&packet); err != nil {
						encodingErrorOccurred = true
					}
					mu.Unlock()
				}
			}
			read := func(v interface{}) (interface{}, error) {
				if err := decoder.Decode(v); err != nil {
					if isDisconnectError(err) {
						handleDisconnect()
						cancel()
					}
					return nil, err
				}
				return v, nil
			}

			processStdout := func(data string) {
				write(agent.Packet{
					Type: "stdout",
					Data: []byte(data),
				})
			}

			processStderr := func(data string) {
				write(agent.Packet{
					Type: "stderr",
					Data: []byte(data),
				})
			}

			clientContext.Stdout = &CustomWriter{processFunc: processStdout}
			clientContext.Stderr = &CustomWriter{processFunc: processStderr}

			logger := logging.NewLogger(clientContext.Stdout, clientContext.Stderr)
			logger.EnableInfo()
			clientContext.SetLogger(logger)

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

			// Attempt another decode to detect client disconnection during processing
			go func() {
				var tmp interface{}
				read(&tmp)
			}()

			var subcommand subcommands.RPC
			var repositoryLocation string
			var repositorySecret []byte

			switch name {
			case (&AgentStop{}).Name():
				var cmd struct {
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &AgentStop{}
				os.Exit(0)
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
			case (&diag.DiagContentType{}).Name():
				var cmd struct {
					Name       string
					Subcommand diag.DiagContentType
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&diag.DiagErrors{}).Name():
				var cmd struct {
					Name       string
					Subcommand diag.DiagErrors
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&diag.DiagObject{}).Name():
				var cmd struct {
					Name       string
					Subcommand diag.DiagObject
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&diag.DiagPackfile{}).Name():
				var cmd struct {
					Name       string
					Subcommand diag.DiagPackfile
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&diag.DiagRepository{}).Name():
				var cmd struct {
					Name       string
					Subcommand diag.DiagRepository
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&diag.DiagSnapshot{}).Name():
				var cmd struct {
					Name       string
					Subcommand diag.DiagSnapshot
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&diag.DiagState{}).Name():
				var cmd struct {
					Name       string
					Subcommand diag.DiagState
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&diag.DiagVFS{}).Name():
				var cmd struct {
					Name       string
					Subcommand diag.DiagVFS
				}
				if err := msgpack.Unmarshal(request, &cmd); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode client request: %s\n", err)
					return
				}
				subcommand = &cmd.Subcommand
				repositoryLocation = cmd.Subcommand.RepositoryLocation
				repositorySecret = cmd.Subcommand.RepositorySecret
			case (&diag.DiagXattr{}).Name():
				var cmd struct {
					Name       string
					Subcommand diag.DiagXattr
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
			case (&digest.Digest{}).Name():
				var cmd struct {
					Name       string
					Subcommand digest.Digest
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
			case (&cmd_exec.Exec{}).Name():
				var cmd struct {
					Name       string
					Subcommand cmd_exec.Exec
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
				repositoryLocation = cmd.Subcommand.SourceRepositoryLocation
				repositorySecret = cmd.Subcommand.SourceRepositorySecret
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
				if repositorySecret != nil {
					clientContext.SetSecret(repositorySecret)
				}

				store, serializedConfig, err := storage.Open(map[string]string{"location": repositoryLocation})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to open storage: %s\n", err)
					return
				}
				defer store.Close()

				repo, err = repository.New(clientContext, store, serializedConfig)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to open repository: %s\n", err)
					return
				}
				defer repo.Close()
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
					write(agent.Packet{
						Type: "event",
						Data: serialized,
					})
				}
				eventsDone <- struct{}{}
			}()

			status, err := subcommand.Execute(clientContext, repo)

			clientContext.Close()
			<-eventsDone

			errStr := ""
			if err != nil {
				errStr = err.Error()
			}
			write(agent.Packet{
				Type:     "exit",
				ExitCode: status,
				Err:      errStr,
			})

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
