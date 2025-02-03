package plakard

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/PlakarKorp/plakar/network"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/stretchr/testify/require"
)

// MockTCPServer starts a mock TCP server on a random port and returns the address.
// It accepts a handler function and a context to control the server's lifecycle.
func MockTCPServer(t *testing.T, ctx context.Context, handler func(net.Conn, context.Context)) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0") // Listen on a random port
	if err != nil {
		t.Fatalf("Failed to start mock TCP server: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1) // Indicate that the server is ready to accept connections

	// Goroutine to handle incoming connections
	go func() {
		defer listener.Close()
		wg.Done() // Signal that the server is running

		for {
			select {
			case <-ctx.Done():
				// Context canceled, stop accepting new connections
				t.Log("Mock TCP server shutting down...")
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					t.Logf("Failed to accept connection: %v", err)
					continue
				}

				// Handle the connection in a separate goroutine
				go handler(conn, ctx)
			}
		}
	}()

	// Wait for the server to start
	wg.Wait()
	return listener.Addr().String()
}

type storedeData struct {
	checksum objects.Checksum
	data     []byte
}

type localCache struct {
	configuration storage.Configuration
	states        []storedeData
	packfiles     []storedeData
}

func _TestPlakardBackendTCP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cache := &localCache{}

	serverAddr := MockTCPServer(t, ctx, func(conn net.Conn, ctx context.Context) {
		defer conn.Close()

		decoder := gob.NewDecoder(conn)
		encoder := gob.NewEncoder(conn)

		var wg sync.WaitGroup

		for {
			select {
			case <-ctx.Done():
				t.Log("Connection handler shutting down...")
				return
			default:
				request := network.Request{}
				err := decoder.Decode(&request)
				if err != nil {
					break
				}
				switch request.Type {
				case "ReqCreate":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()
						c.configuration = request.Payload.(network.ReqCreate).Configuration
						result := network.Request{
							Uuid:    request.Uuid,
							Type:    "ResCreate",
							Payload: network.ResCreate{Err: ""},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				case "ReqOpen":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()
						var payload network.ResOpen
						config := c.configuration
						payload = network.ResOpen{Configuration: &config, Err: ""}

						result := network.Request{
							Uuid:    request.Uuid,
							Type:    "ResOpen",
							Payload: payload,
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				case "ReqClose":
					wg.Add(1)
					go func() {
						defer wg.Done()

						result := network.Request{
							Uuid: request.Uuid,
							Type: "ResClose",
							Payload: network.ResClose{
								Err: "",
							},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}()

				case "ReqPutState":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()
						c.states = append(c.states, storedeData{request.Payload.(network.ReqPutState).Checksum, request.Payload.(network.ReqPutState).Data})
						result := network.Request{
							Uuid: request.Uuid,
							Type: "ResPutState",
							Payload: network.ResPutState{
								Err: "",
							},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				case "ReqGetStates":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()
						checksums := make([]objects.Checksum, len(c.states))
						for i, state := range c.states {
							checksums[i] = state.checksum
						}

						result := network.Request{
							Uuid: request.Uuid,
							Type: "ResGetStates",
							Payload: network.ResGetStates{
								Checksums: checksums,
								Err:       "",
							},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				case "ReqGetState":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()

						var data []byte
						for _, state := range c.states {
							if state.checksum == request.Payload.(network.ReqGetState).Checksum {
								data = state.data
								break
							}
						}
						result := network.Request{
							Uuid: request.Uuid,
							Type: "ResGetState",
							Payload: network.ResGetState{
								Data: data,
								Err:  "",
							},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				case "ReqDeleteState":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()

						var idxToDelete int
						for idx, state := range c.states {
							if state.checksum == request.Payload.(network.ReqDeleteState).Checksum {
								idxToDelete = idx
								break
							}
						}
						c.states = append(c.states[:idxToDelete], c.states[idxToDelete+1:]...)

						result := network.Request{
							Uuid: request.Uuid,
							Type: "ResDeleteState",
							Payload: network.ResDeleteState{
								Err: "",
							},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				case "ReqPutPackfile":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()
						c.packfiles = append(c.packfiles, storedeData{request.Payload.(network.ReqPutPackfile).Checksum, request.Payload.(network.ReqPutPackfile).Data})
						result := network.Request{
							Uuid: request.Uuid,
							Type: "ResPutPackfile",
							Payload: network.ResPutPackfile{
								Err: "",
							},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				case "ReqGetPackfiles":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()
						checksums := make([]objects.Checksum, len(c.packfiles))
						for i, packfile := range c.packfiles {
							checksums[i] = packfile.checksum
						}

						result := network.Request{
							Uuid: request.Uuid,
							Type: "ResGetPackfiles",
							Payload: network.ResGetPackfiles{
								Checksums: checksums,
								Err:       "",
							},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				case "ReqGetPackfileBlob":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()

						var data []byte
						for _, packfile := range c.packfiles {
							if packfile.checksum == request.Payload.(network.ReqGetPackfileBlob).Checksum {
								data = packfile.data[request.Payload.(network.ReqGetPackfileBlob).Offset : request.Payload.(network.ReqGetPackfileBlob).Offset+request.Payload.(network.ReqGetPackfileBlob).Length]
								break
							}
						}

						result := network.Request{
							Uuid: request.Uuid,
							Type: "ResGetPackfileBlob",
							Payload: network.ResGetPackfileBlob{
								Data: data,
								Err:  "",
							},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				case "ReqDeletePackfile":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()

						var idxToDelete int
						for idx, packfile := range c.packfiles {
							if packfile.checksum == request.Payload.(network.ReqDeletePackfile).Checksum {
								idxToDelete = idx
								break
							}
						}
						c.packfiles = append(c.packfiles[:idxToDelete], c.packfiles[idxToDelete+1:]...)

						result := network.Request{
							Uuid: request.Uuid,
							Type: "ResDeletePackfile",
							Payload: network.ResDeletePackfile{
								Err: "",
							},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				case "ReqGetPackfile":
					wg.Add(1)
					go func(c *localCache) {
						defer wg.Done()

						var data []byte
						for _, packfile := range c.packfiles {
							if packfile.checksum == request.Payload.(network.ReqGetPackfile).Checksum {
								data = packfile.data
								break
							}
						}

						result := network.Request{
							Uuid: request.Uuid,
							Type: "ResGetPackfile",
							Payload: network.ResGetPackfile{
								Data: data,
								Err:  "",
							},
						}
						err = encoder.Encode(&result)
						if err != nil {
							t.Errorf("%s", err)
						}
					}(cache)

				default:
					fmt.Println("Unknown request type", request.Type)
				}
			}
		}
	})

	location := fmt.Sprintf("tcp://%s", serverAddr)
	repo := NewRepository(location)
	require.NotNil(t, repo)

	require.Equal(t, location, repo.Location())

	err := repo.Create(location, *storage.NewConfiguration())
	require.NoError(t, err)

	err = repo.Open(location)
	require.NoError(t, err)
	require.Equal(t, repo.Configuration().Version, storage.VERSION)

	err = repo.Close()
	require.NoError(t, err)

	// states
	checksum1 := objects.Checksum{0x10, 0x20}
	checksum2 := objects.Checksum{0x30, 0x40}
	err = repo.PutState(checksum1, bytes.NewReader([]byte("test1")))
	require.NoError(t, err)
	err = repo.PutState(checksum2, bytes.NewReader([]byte("test2")))
	require.NoError(t, err)

	states, err := repo.GetStates()
	require.NoError(t, err)
	expected := []objects.Checksum{
		{0x10, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		{0x30, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
	}
	require.Equal(t, expected, states)

	rd, err := repo.GetState(checksum2)
	require.NoError(t, err)
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, rd)
	require.NoError(t, err)
	require.Equal(t, "test2", buf.String())

	err = repo.DeleteState(checksum1)
	require.NoError(t, err)

	states, err = repo.GetStates()
	require.NoError(t, err)
	expected = []objects.Checksum{{0x30, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}}
	require.Equal(t, expected, states)

	// packfiles
	checksum3 := objects.Checksum{0x50, 0x60}
	checksum4 := objects.Checksum{0x60, 0x70}
	err = repo.PutPackfile(checksum3, bytes.NewReader([]byte("test3")))
	require.NoError(t, err)
	err = repo.PutPackfile(checksum4, bytes.NewReader([]byte("test4")))
	require.NoError(t, err)

	packfiles, err := repo.GetPackfiles()
	require.NoError(t, err)
	expected = []objects.Checksum{
		{0x50, 0x60, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		{0x60, 0x70, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
	}
	require.Equal(t, expected, packfiles)

	rd, err = repo.GetPackfileBlob(checksum4, 0, 4)
	buf = new(bytes.Buffer)
	_, err = io.Copy(buf, rd)
	require.NoError(t, err)
	require.Equal(t, "test", buf.String())

	err = repo.DeletePackfile(checksum3)
	require.NoError(t, err)

	packfiles, err = repo.GetPackfiles()
	require.NoError(t, err)
	expected = []objects.Checksum{{0x60, 0x70, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}}
	require.Equal(t, expected, packfiles)

	rd, err = repo.GetPackfile(checksum4)
	buf = new(bytes.Buffer)
	_, err = io.Copy(buf, rd)
	require.NoError(t, err)
	require.Equal(t, "test4", buf.String())

	// Cancel the context to shut down the mock server
	cancel()
}
