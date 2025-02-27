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

package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/PlakarKorp/plakar/network"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/storage"
)

type Repository struct {
	config     storage.Configuration
	Repository string
	location   string
}

func init() {
	network.ProtocolRegister()
	storage.Register("http", NewRepository)
}

func NewRepository(storeConfig map[string]string) (storage.Store, error) {
	return &Repository{
		location: storeConfig["location"],
	}, nil
}

func (repo *Repository) Location() string {
	return repo.location
}

func (repo *Repository) sendRequest(method string, url string, requestType string, payload interface{}) (*http.Response, error) {
	requestBody, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(method, url+requestType, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	return client.Do(req)
}

func (repo *Repository) Create(config []byte) error {
	return nil
}

func (repo *Repository) Open() ([]byte, error) {
	repo.Repository = repo.location
	r, err := repo.sendRequest("GET", repo.location, "/", network.ReqOpen{
		Repository: "",
	})
	if err != nil {
		return nil, err
	}

	var resOpen network.ResOpen
	if err := json.NewDecoder(r.Body).Decode(&resOpen); err != nil {
		return nil, err
	}
	if resOpen.Err != "" {
		return nil, fmt.Errorf("%s", resOpen.Err)
	}
	return resOpen.Configuration, nil
}

func (repo *Repository) Close() error {
	r, err := repo.sendRequest("POST", repo.Repository, "/", network.ReqClose{
		Uuid: repo.config.RepositoryID.String(),
	})
	if err != nil {
		return err
	}

	var resClose network.ResClose
	if err := json.NewDecoder(r.Body).Decode(&resClose); err != nil {
		return err
	}
	if resClose.Err != "" {
		return fmt.Errorf("%s", resClose.Err)
	}

	return nil
}

func (repo *Repository) Configuration() storage.Configuration {
	return repo.config
}

// states
func (repo *Repository) GetStates() ([]objects.MAC, error) {
	r, err := repo.sendRequest("GET", repo.Repository, "/states", network.ReqGetStates{})
	if err != nil {
		return nil, err
	}

	var resGetStates network.ResGetStates
	if err := json.NewDecoder(r.Body).Decode(&resGetStates); err != nil {
		return nil, err
	}
	if resGetStates.Err != "" {
		return nil, fmt.Errorf("%s", resGetStates.Err)
	}

	ret := make([]objects.MAC, len(resGetStates.MACs))
	for i, MAC := range resGetStates.MACs {
		ret[i] = MAC
	}
	return ret, nil
}

func (repo *Repository) PutState(MAC objects.MAC, rd io.Reader) error {
	data, err := io.ReadAll(rd)
	if err != nil {
		return err
	}

	r, err := repo.sendRequest("PUT", repo.Repository, "/state", network.ReqPutState{
		MAC:  MAC,
		Data: data,
	})
	if err != nil {
		return err
	}

	var resPutState network.ResPutState
	if err := json.NewDecoder(r.Body).Decode(&resPutState); err != nil {
		return err
	}
	if resPutState.Err != "" {
		return fmt.Errorf("%s", resPutState.Err)
	}
	return nil
}

func (repo *Repository) GetState(MAC objects.MAC) (io.Reader, error) {
	r, err := repo.sendRequest("GET", repo.Repository, "/state", network.ReqGetState{
		MAC: MAC,
	})
	if err != nil {
		return nil, err
	}

	var resGetState network.ResGetState
	if err := json.NewDecoder(r.Body).Decode(&resGetState); err != nil {
		return nil, err
	}
	if resGetState.Err != "" {
		return nil, fmt.Errorf("%s", resGetState.Err)
	}
	return bytes.NewBuffer(resGetState.Data), nil
}

func (repo *Repository) DeleteState(MAC objects.MAC) error {
	r, err := repo.sendRequest("DELETE", repo.Repository, "/state", network.ReqDeleteState{
		MAC: MAC,
	})
	if err != nil {
		return err
	}

	var resDeleteState network.ResDeleteState
	if err := json.NewDecoder(r.Body).Decode(&resDeleteState); err != nil {
		return err
	}
	if resDeleteState.Err != "" {
		return fmt.Errorf("%s", resDeleteState.Err)
	}
	return nil
}

// packfiles
func (repo *Repository) GetPackfiles() ([]objects.MAC, error) {
	r, err := repo.sendRequest("GET", repo.Repository, "/packfiles", network.ReqGetPackfiles{})
	if err != nil {
		return nil, err
	}

	var resGetPackfiles network.ResGetPackfiles
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfiles); err != nil {
		return nil, err
	}
	if resGetPackfiles.Err != "" {
		return nil, fmt.Errorf("%s", resGetPackfiles.Err)
	}

	ret := make([]objects.MAC, len(resGetPackfiles.MACs))
	for i, MAC := range resGetPackfiles.MACs {
		ret[i] = MAC
	}
	return ret, nil
}

func (repo *Repository) PutPackfile(MAC objects.MAC, rd io.Reader) error {
	data, err := io.ReadAll(rd)
	if err != nil {
		return err
	}
	r, err := repo.sendRequest("PUT", repo.Repository, "/packfile", network.ReqPutPackfile{
		MAC:  MAC,
		Data: data,
	})
	if err != nil {
		return err
	}

	var resPutPackfile network.ResPutPackfile
	if err := json.NewDecoder(r.Body).Decode(&resPutPackfile); err != nil {
		return err
	}
	if resPutPackfile.Err != "" {
		return fmt.Errorf("%s", resPutPackfile.Err)
	}
	return nil
}

func (repo *Repository) GetPackfile(MAC objects.MAC) (io.Reader, error) {
	r, err := repo.sendRequest("GET", repo.Repository, "/packfile", network.ReqGetPackfile{
		MAC: MAC,
	})
	if err != nil {
		return nil, err
	}

	var resGetPackfile network.ResGetPackfile
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfile); err != nil {
		return nil, err
	}
	if resGetPackfile.Err != "" {
		return nil, fmt.Errorf("%s", resGetPackfile.Err)
	}
	return bytes.NewBuffer(resGetPackfile.Data), nil
}

func (repo *Repository) GetPackfileBlob(MAC objects.MAC, offset uint64, length uint32) (io.Reader, error) {
	r, err := repo.sendRequest("GET", repo.Repository, "/packfile/blob", network.ReqGetPackfileBlob{
		MAC:    MAC,
		Offset: offset,
		Length: length,
	})
	if err != nil {
		return nil, err
	}

	var resGetPackfileBlob network.ResGetPackfileBlob
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfileBlob); err != nil {
		return nil, err
	}
	if resGetPackfileBlob.Err != "" {
		return nil, fmt.Errorf("%s", resGetPackfileBlob.Err)
	}
	return bytes.NewBuffer(resGetPackfileBlob.Data), nil
}

func (repo *Repository) DeletePackfile(MAC objects.MAC) error {
	r, err := repo.sendRequest("DELETE", repo.Repository, "/packfile", network.ReqDeletePackfile{
		MAC: MAC,
	})
	if err != nil {
		return err
	}

	var resDeletePackfile network.ResDeletePackfile
	if err := json.NewDecoder(r.Body).Decode(&resDeletePackfile); err != nil {
		return err
	}
	if resDeletePackfile.Err != "" {
		return fmt.Errorf("%s", resDeletePackfile.Err)
	}
	return nil
}
