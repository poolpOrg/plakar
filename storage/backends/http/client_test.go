package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PlakarKorp/plakar/api"
	"github.com/PlakarKorp/plakar/network"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/stretchr/testify/require"
)

type storedeData struct {
	checksum objects.Checksum
	data     []byte
}

type MyHandler struct {
	states    []storedeData
	packfiles []storedeData
}

func (h MyHandler) Configuration(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(http.StatusOK)
	configuration := storage.NewConfiguration()
	config, err := json.Marshal(configuration)
	if err != nil {
		return err
	}
	w.Write([]byte(fmt.Sprintf(`{"Configuration": %s}`, config)))
	return nil
}

func (h MyHandler) Close(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
	return nil
}

func (h *MyHandler) PutState(w http.ResponseWriter, r *http.Request) error {
	var reqPutState network.ReqPutState
	if err := json.NewDecoder(r.Body).Decode(&reqPutState); err != nil {
		return err
	}

	h.states = append(h.states, storedeData{reqPutState.Checksum, reqPutState.Data})

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
	return nil
}

func (h *MyHandler) GetStates(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(http.StatusOK)
	var resGetStates network.ResGetStates
	resGetStates.Checksums = make([]objects.Checksum, len(h.states))
	for i, state := range h.states {
		resGetStates.Checksums[i] = state.checksum
	}

	if err := json.NewEncoder(w).Encode(resGetStates); err != nil {
		return err
	}
	return nil
}

func (h *MyHandler) GetState(w http.ResponseWriter, r *http.Request) error {
	var reqGetState network.ReqGetState
	if err := json.NewDecoder(r.Body).Decode(&reqGetState); err != nil {
		return err
	}

	w.WriteHeader(http.StatusOK)
	var resGetState network.ResGetState
	for _, state := range h.states {
		if state.checksum == reqGetState.Checksum {
			resGetState.Data = state.data
			break
		}
	}

	if err := json.NewEncoder(w).Encode(resGetState); err != nil {
		return err
	}
	return nil
}

func (h *MyHandler) DeleteState(w http.ResponseWriter, r *http.Request) error {
	var reqDeleteState network.ReqDeleteState
	if err := json.NewDecoder(r.Body).Decode(&reqDeleteState); err != nil {
		return err
	}

	var idxToDelete int
	for idx, state := range h.states {
		if state.checksum == reqDeleteState.Checksum {
			idxToDelete = idx
			break
		}
	}
	h.states = append(h.states[:idxToDelete], h.states[idxToDelete+1:]...)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
	return nil
}

func (h *MyHandler) PutPackfile(w http.ResponseWriter, r *http.Request) error {
	var reqPutPackfile network.ReqPutPackfile
	if err := json.NewDecoder(r.Body).Decode(&reqPutPackfile); err != nil {
		return err
	}

	h.packfiles = append(h.packfiles, storedeData{reqPutPackfile.Checksum, reqPutPackfile.Data})

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
	return nil
}

func (h *MyHandler) GetPackfiles(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(http.StatusOK)
	var resGetPackfiles network.ResGetPackfiles
	resGetPackfiles.Checksums = make([]objects.Checksum, len(h.packfiles))
	for i, packfile := range h.packfiles {
		resGetPackfiles.Checksums[i] = packfile.checksum
	}

	if err := json.NewEncoder(w).Encode(resGetPackfiles); err != nil {
		return err
	}
	return nil
}

func (h *MyHandler) GetPackfileBlob(w http.ResponseWriter, r *http.Request) error {
	var reqGetPackfileBlob network.ReqGetPackfileBlob
	if err := json.NewDecoder(r.Body).Decode(&reqGetPackfileBlob); err != nil {
		return err
	}

	w.WriteHeader(http.StatusOK)
	var resGetState network.ResGetState
	for _, packfile := range h.packfiles {
		if packfile.checksum == reqGetPackfileBlob.Checksum {
			resGetState.Data = packfile.data[reqGetPackfileBlob.Offset : reqGetPackfileBlob.Offset+uint64(reqGetPackfileBlob.Length)]
			break
		}
	}

	if err := json.NewEncoder(w).Encode(resGetState); err != nil {
		return err
	}
	return nil
}

func (h *MyHandler) DeletePackfile(w http.ResponseWriter, r *http.Request) error {
	var reqDeletePackfile network.ReqDeletePackfile
	if err := json.NewDecoder(r.Body).Decode(&reqDeletePackfile); err != nil {
		return err
	}

	var idxToDelete int
	for idx, packfile := range h.packfiles {
		if packfile.checksum == reqDeletePackfile.Checksum {
			idxToDelete = idx
			break
		}
	}
	h.packfiles = append(h.packfiles[:idxToDelete], h.packfiles[idxToDelete+1:]...)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
	return nil
}

func (h *MyHandler) GetPackfile(w http.ResponseWriter, r *http.Request) error {
	var reqGetPackfile network.ReqGetPackfile
	if err := json.NewDecoder(r.Body).Decode(&reqGetPackfile); err != nil {
		return err
	}

	w.WriteHeader(http.StatusOK)
	var resGetState network.ResGetPackfile
	for _, packfile := range h.packfiles {
		if packfile.checksum == reqGetPackfile.Checksum {
			resGetState.Data = packfile.data
			break
		}
	}

	if err := json.NewEncoder(w).Encode(resGetState); err != nil {
		return err
	}
	return nil
}

func TestHttpBackend(t *testing.T) {
	mux := http.NewServeMux()
	handler := MyHandler{
		states:    []storedeData{},
		packfiles: []storedeData{},
	}
	mux.Handle("GET /", api.APIView(handler.Configuration))
	mux.Handle("POST /", api.APIView(handler.Close))
	mux.Handle("PUT /", api.APIView(handler.PutState))
	mux.Handle("GET /states", api.APIView(handler.GetStates))
	mux.Handle("GET /state", api.APIView(handler.GetState))
	mux.Handle("DELETE /state", api.APIView(handler.DeleteState))
	mux.Handle("PUT /packfile", api.APIView(handler.PutPackfile))
	mux.Handle("GET /packfiles", api.APIView(handler.GetPackfiles))
	mux.Handle("GET /packfile/blob", api.APIView(handler.GetPackfileBlob))
	mux.Handle("DELETE /packfile", api.APIView(handler.DeletePackfile))
	mux.Handle("GET /packfile", api.APIView(handler.GetPackfile))

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	// create a repository
	repo := NewRepository(ts.URL)
	if repo == nil {
		t.Fatal("error creating repository")
	}

	location := repo.Location()
	require.Equal(t, ts.URL, location)

	err := repo.Create(ts.URL, *storage.NewConfiguration())
	require.NoError(t, err)

	err = repo.Open(ts.URL)
	require.NoError(t, err)
	require.Equal(t, repo.Configuration().Version, versioning.FromString(storage.VERSION))

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
}
