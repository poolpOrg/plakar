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

package plakard

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"

	"github.com/PlakarKorp/plakar/network"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/google/uuid"
	giturls "github.com/whilp/git-urls"

	"sync"
)

type Repository struct {
	config   storage.Configuration
	location string

	encoder *gob.Encoder
	decoder *gob.Decoder
	mu      sync.Mutex

	Repository string

	inflightRequests map[uuid.UUID]chan network.Request
	notifications    chan network.Request
}

func init() {
	network.ProtocolRegister()
	storage.Register("plakard", NewRepository)
}

func NewRepository(location string) storage.Store {
	return &Repository{
		location: location,
	}
}

func (repo *Repository) Location() string {
	return repo.location
}

func (repository *Repository) connect(location *url.URL) error {
	scheme := location.Scheme
	switch scheme {
	case "ssh":
		err := repository.connectSSH(location)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported protocol: %s", scheme)
	}

	return nil
}

func (repository *Repository) connectSSH(location *url.URL) error {
	connectUrl := "ssh://"
	if location.User != nil {
		connectUrl += location.User.Username() + "@"
	}
	connectUrl += location.Hostname()
	if location.Port() != "" {
		connectUrl += ":" + location.Port()
	}

	subProcess := exec.Command("ssh", connectUrl, "plakar stdio")

	stdin, err := subProcess.StdinPipe()
	if err != nil {
		return err
	}

	stdout, err := subProcess.StdoutPipe()
	if err != nil {
		return err
	}

	subProcess.Stderr = os.Stderr

	repository.encoder = gob.NewEncoder(stdin)
	repository.decoder = gob.NewDecoder(stdout)

	if err = subProcess.Start(); err != nil {
		return err
	}

	repository.inflightRequests = make(map[uuid.UUID]chan network.Request)
	repository.notifications = make(chan network.Request)

	go func() {
		for m := range repository.notifications {
			repository.mu.Lock()
			notify := repository.inflightRequests[m.Uuid]
			repository.mu.Unlock()
			notify <- m
		}
	}()

	go func() {
		for {
			result := network.Request{}
			err = repository.decoder.Decode(&result)
			if err != nil {
				stdin.Close()
				subProcess.Wait()
				return
			}
			repository.notifications <- result
		}
	}()

	return nil
}

func (repository *Repository) sendRequest(Type string, Payload interface{}) (*network.Request, error) {
	Uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	request := network.Request{
		Uuid:    Uuid,
		Type:    Type,
		Payload: Payload,
	}

	notify := make(chan network.Request)
	repository.mu.Lock()
	repository.inflightRequests[request.Uuid] = notify
	repository.mu.Unlock()

	err = repository.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := <-notify

	repository.mu.Lock()
	delete(repository.inflightRequests, request.Uuid)
	repository.mu.Unlock()
	close(notify)

	return &result, nil
}

func (repository *Repository) Create(location string, config storage.Configuration) error {
	parsed, err := giturls.Parse(location)
	if err != nil {
		return err
	}

	err = repository.connect(parsed)
	if err != nil {
		return err
	}

	result, err := repository.sendRequest("ReqCreate", network.ReqCreate{
		Repository:    parsed.Path,
		Configuration: config,
	})
	if err != nil {
		return err
	}

	if result.Payload.(network.ResCreate).Err != "" {
		return fmt.Errorf("%s", result.Payload.(network.ResCreate).Err)
	}

	repository.config = config
	return nil
}

func (repository *Repository) Open(location string) error {
	parsed, err := giturls.Parse(location)
	if err != nil {
		return err
	}

	err = repository.connect(parsed)
	if err != nil {
		return err
	}

	result, err := repository.sendRequest("ReqOpen", network.ReqOpen{
		Repository: parsed.Path,
	})
	if err != nil {
		return err
	}

	if result.Payload.(network.ResOpen).Err != "" {
		return fmt.Errorf("%s", result.Payload.(network.ResOpen).Err)
	}

	repository.config = *result.Payload.(network.ResOpen).Configuration
	return nil
}

func (repository *Repository) Close() error {
	result, err := repository.sendRequest("ReqClose", network.ReqClose{})
	if err != nil {
		return err
	}

	if result.Payload.(network.ResClose).Err != "" {
		return fmt.Errorf("%s", result.Payload.(network.ResClose).Err)
	}
	return nil
}

func (repository *Repository) Configuration() storage.Configuration {
	return repository.config
}

// states
func (repository *Repository) GetStates() ([]objects.Checksum, error) {
	result, err := repository.sendRequest("ReqGetStates", network.ReqGetStates{})
	if err != nil {
		return nil, err
	}
	if result.Payload.(network.ResGetStates).Err != "" {
		return nil, fmt.Errorf("%s", result.Payload.(network.ResGetStates).Err)
	}

	ret := make([]objects.Checksum, len(result.Payload.(network.ResGetStates).Checksums))
	copy(ret, result.Payload.(network.ResGetStates).Checksums)
	return ret, nil
}

func (repository *Repository) PutState(checksum objects.Checksum, rd io.Reader) error {
	data, err := io.ReadAll(rd)
	if err != nil {
		return err
	}
	result, err := repository.sendRequest("ReqPutState", network.ReqPutState{
		Checksum: checksum,
		Data:     data,
	})
	if err != nil {
		return err
	}

	if result.Payload.(network.ResPutState).Err != "" {
		return fmt.Errorf("%s", result.Payload.(network.ResPutState).Err)
	}
	return nil
}

func (repository *Repository) GetState(checksum objects.Checksum) (io.Reader, error) {
	result, err := repository.sendRequest("ReqGetState", network.ReqGetState{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}

	if result.Payload.(network.ResGetState).Err != "" {
		return nil, fmt.Errorf("%s", result.Payload.(network.ResGetState).Err)
	}

	return bytes.NewBuffer(result.Payload.(network.ResGetState).Data), nil
}

func (repository *Repository) DeleteState(checksum objects.Checksum) error {
	result, err := repository.sendRequest("ReqDeleteState", network.ReqDeleteState{
		Checksum: checksum,
	})
	if err != nil {
		return err
	}

	if result.Payload.(network.ResDeleteState).Err != "" {
		return fmt.Errorf("%s", result.Payload.(network.ResDeleteState).Err)
	}
	return nil
}

// packfiles
func (repository *Repository) GetPackfiles() ([]objects.Checksum, error) {
	result, err := repository.sendRequest("ReqGetPackfiles", network.ReqGetPackfiles{})
	if err != nil {
		return nil, err
	}
	if result.Payload.(network.ResGetPackfiles).Err != "" {
		return nil, fmt.Errorf("%s", result.Payload.(network.ResGetPackfiles).Err)
	}

	ret := make([]objects.Checksum, len(result.Payload.(network.ResGetPackfiles).Checksums))
	copy(ret, result.Payload.(network.ResGetPackfiles).Checksums)
	return ret, nil
}

func (repository *Repository) PutPackfile(checksum objects.Checksum, rd io.Reader) error {
	data, err := io.ReadAll(rd)
	if err != nil {
		return err
	}
	result, err := repository.sendRequest("ReqPutPackfile", network.ReqPutPackfile{
		Checksum: checksum,
		Data:     data,
	})
	if err != nil {
		return err
	}
	if result.Payload.(network.ResPutPackfile).Err != "" {
		return fmt.Errorf("%s", result.Payload.(network.ResPutPackfile).Err)
	}
	return nil
}

func (repository *Repository) GetPackfile(checksum objects.Checksum) (io.Reader, error) {
	result, err := repository.sendRequest("ReqGetPackfile", network.ReqGetPackfile{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}
	if result.Payload.(network.ResGetPackfile).Err != "" {
		return nil, fmt.Errorf("%s", result.Payload.(network.ResGetPackfile).Err)
	}

	data := result.Payload.(network.ResGetPackfile).Data
	return bytes.NewBuffer(data), nil
}

func (repository *Repository) GetPackfileBlob(checksum objects.Checksum, offset uint32, length uint32) (io.Reader, error) {
	result, err := repository.sendRequest("ReqGetPackfileBlob", network.ReqGetPackfileBlob{
		Checksum: checksum,
		Offset:   offset,
		Length:   length,
	})
	if err != nil {
		return nil, err
	}

	if result.Payload.(network.ResGetPackfileBlob).Err != "" {
		return nil, fmt.Errorf("%s", result.Payload.(network.ResGetPackfileBlob).Err)
	}

	data := result.Payload.(network.ResGetPackfileBlob).Data

	return bytes.NewBuffer(data), nil
}

func (repository *Repository) DeletePackfile(checksum objects.Checksum) error {
	result, err := repository.sendRequest("ReqDeletePackfile", network.ReqDeletePackfile{
		Checksum: checksum,
	})
	if err != nil {
		return err
	}

	if result.Payload.(network.ResDeletePackfile).Err != "" {
		return fmt.Errorf("%s", result.Payload.(network.ResDeletePackfile).Err)
	}
	return nil
}
