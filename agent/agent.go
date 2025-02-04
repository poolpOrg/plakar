package agent

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/vmihailenco/msgpack/v5"
)

type Packet struct {
	Type     string
	Data     []byte
	ExitCode int
	Err      string
}

type Client struct {
	conn net.Conn
}

func ExecuteRPC(ctx *appcontext.AppContext, repo *repository.Repository, cmd subcommands.Subcommand) (int, error) {
	rpcCmd, ok := cmd.(subcommands.RPC)
	if !ok {
		return 1, fmt.Errorf("subcommand is not an RPC")
	}

	client, err := NewClient(filepath.Join(ctx.CacheDir, "agent.sock"))
	if err != nil {
		return 1, err
	}
	defer client.Close()
	if status, err := client.SendCommand(ctx, rpcCmd, repo); err != nil {
		return status, err
	}
	return 0, nil
}

func NewClient(socketPath string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to daemon: %w", err)
	}
	return &Client{conn: conn}, nil
}

func (c *Client) SendCommand(ctx *appcontext.AppContext, cmd subcommands.RPC, repo *repository.Repository) (int, error) {
	encoder := msgpack.NewEncoder(c.conn)
	decoder := msgpack.NewDecoder(c.conn)

	if err := subcommands.EncodeRPC(encoder, cmd); err != nil {
		return 1, err
	}

	var response Packet
	for {
		if err := decoder.Decode(&response); err != nil {
			return 1, fmt.Errorf("failed to decode response: %w", err)
		}
		switch response.Type {
		case "stdout":
			fmt.Printf("%s", string(response.Data))
		case "stderr":
			fmt.Fprintf(os.Stderr, "%s", string(response.Data))
		case "event":
			evt, err := events.Deserialize(response.Data)
			if err != nil {
				return 1, fmt.Errorf("failed to deserialize event: %w", err)
			}
			ctx.Events().Send(evt)
		case "exit":
			var err error
			if response.Err != "" {
				err = fmt.Errorf("%s", response.Err)
			}
			return response.ExitCode, err
		}
	}
}
func (c *Client) Close() error {
	return c.conn.Close()
}
