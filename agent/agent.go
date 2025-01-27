package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

type Agent struct {
	socketPath string
	listener   net.Listener
	ctx        *appcontext.AppContext
	cancelCtx  context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.Mutex
}

func NewAgent(ctx *appcontext.AppContext, network string, address string) (*Agent, error) {
	if network != "unix" {
		return nil, fmt.Errorf("unsupported network: %s", network)
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	d := &Agent{
		socketPath: address,
		ctx:        ctx,
		cancelCtx:  cancelCtx,
		cancelFunc: cancelFunc,
	}

	if _, err := os.Stat(d.socketPath); err == nil {
		if !d.checkSocket() {
			d.Close()
		} else {
			return nil, fmt.Errorf("already running")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	listener, err := net.Listen("unix", d.socketPath)
	if err != nil {
		return nil, err
	}
	d.listener = listener

	if err := os.Chmod(d.socketPath, 0600); err != nil {
		d.Close()
		return nil, err
	}

	return d, nil
}

func (d *Agent) checkSocket() bool {
	conn, err := net.Dial("unix", d.socketPath)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (d *Agent) Close() error {
	if d.listener != nil {
		d.listener.Close()
	}
	if err := os.Remove(d.socketPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (d *Agent) Shutdown(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.listener != nil {
		if err := d.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
		d.listener = nil
	}

	// Wait for all active connections or until the context is done
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All connections gracefully closed
	case <-ctx.Done():
		// Context canceled or timed out
		return ctx.Err()
	}

	return d.Close()
}

type CustomWriter struct {
	processFunc func(string) // Function to handle the log lines
}

// Write implements the `io.Writer` interface.
func (cw *CustomWriter) Write(p []byte) (n int, err error) {
	cw.processFunc(string(p))
	return len(p), nil
}

func isDisconnectError(err error) bool {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && !netErr.Temporary()
}

func (d *Agent) ListenAndServe(handler func(*appcontext.AppContext, *repository.Repository, string, []string) (int, error)) error {
	for {
		select {
		case <-d.cancelCtx.Done():
			return nil
		default:
		}

		conn, err := d.listener.Accept()
		if err != nil {
			select {
			case <-d.cancelCtx.Done():
				return nil
			default:
				if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
					return nil
				}
				return fmt.Errorf("failed to accept connection: %w", err)
			}
		}

		d.wg.Add(1)
		go func(_conn net.Conn) {
			defer _conn.Close()
			defer d.wg.Done()

			encoder := msgpack.NewEncoder(_conn)
			var encodingErrorOccurred bool

			// Create a context tied to the connection
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			clientContext := appcontext.NewAppContextFrom(d.ctx)
			clientContext.SetContext(ctx)

			processStdout := func(data string) {
				if encodingErrorOccurred {
					return
				}
				select {
				case <-clientContext.GetContext().Done():
					return
				default:
					response := Packet{
						Type:   "stdout",
						Output: data,
					}
					if err := encoder.Encode(&response); err != nil {
						encodingErrorOccurred = true
					}
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
					response := Packet{
						Type:   "stderr",
						Output: data,
					}
					if err := encoder.Encode(&response); err != nil {
						encodingErrorOccurred = true
					}
				}
			}

			clientContext.SetStdout(&CustomWriter{processFunc: processStdout})
			clientContext.SetStderr(&CustomWriter{processFunc: processStderr})

			logger := logging.NewLogger(clientContext.Stdout(), clientContext.Stderr())
			logger.EnableInfo()
			clientContext.SetLogger(logger)

			decoder := msgpack.NewDecoder(conn)

			// Decode the client request
			var request CommandRequest
			if err := decoder.Decode(&request); err != nil {
				if isDisconnectError(err) {
					fmt.Println("Client disconnected during initial request")
					cancel() // Cancel the context on disconnect
					return
				}
				fmt.Println("Failed to decode client request:", err)
				return
			}
			clientContext.SetSecret(request.RepositorySecret)

			// Monitor the connection for subsequent disconnection
			go func() {
				// Attempt another decode to detect client disconnection during processing
				var tmp interface{}
				if err := decoder.Decode(&tmp); err != nil {
					if isDisconnectError(err) {
						cancel()
					}
				}
			}()

			store, err := storage.Open(request.Repository)
			if err != nil {
				fmt.Println("Failed to open storage:", err)
				return
			}
			defer store.Close()

			repo, err := repository.New(clientContext, store, clientContext.GetSecret())
			if err != nil {
				fmt.Println("Failed to open repository:", err)
				return
			}
			defer repo.Close()

			status, err := handler(clientContext, repo, request.Cmd, request.Argv)
			fmt.Println("status:", status, "err:", err)
			select {
			case <-clientContext.GetContext().Done():
				return
			default:
				response := Packet{
					Type:     "exit",
					ExitCode: status,
					Err:      fmt.Sprintf("%v", err),
				}
				if err := encoder.Encode(&response); err != nil {
					fmt.Println("failed to encode response:", err)
				}
			}
		}(conn)
	}
}

// Client structure and other code remain unchanged

type CommandRequest struct {
	AppContext       *appcontext.AppContext
	Repository       string
	RepositorySecret []byte
	Cmd              string
	Argv             []string
}

type Packet struct {
	Type     string
	Output   string
	ExitCode int
	Err      string
}

type Client struct {
	conn net.Conn
}

func NewClient(socketPath string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to daemon: %w", err)
	}

	return &Client{conn: conn}, nil
}

func (c *Client) SendCommand(ctx *appcontext.AppContext, repo string, cmd string, argv []string) (int, error) {
	encoder := msgpack.NewEncoder(c.conn)
	decoder := msgpack.NewDecoder(c.conn)

	request := CommandRequest{
		AppContext: ctx,
		Repository: repo,
		Cmd:        cmd,
		Argv:       argv,
	}
	if ctx.GetSecret() != nil {
		request.RepositorySecret = ctx.GetSecret()
	}

	if err := encoder.Encode(&request); err != nil {
		return 1, fmt.Errorf("failed to encode command: %w", err)
	}

	var response Packet
	for {
		if err := decoder.Decode(&response); err != nil {
			return 1, fmt.Errorf("failed to decode response: %w", err)
		}
		switch response.Type {
		case "stdout":
			fmt.Printf("%s", response.Output)
		case "stderr":
			fmt.Fprintf(os.Stderr, "%s", response.Output)
		case "exit":
			return response.ExitCode, fmt.Errorf("%s", response.Err)
		}
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}
