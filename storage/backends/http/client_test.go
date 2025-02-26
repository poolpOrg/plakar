package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PlakarKorp/plakar/api"
	"github.com/PlakarKorp/plakar/network"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/stretchr/testify/require"
)

type storedeData struct {
	MAC  objects.MAC
	data []byte
}

type MyHandler struct {
	states    []storedeData
	packfiles []storedeData
}

func (h MyHandler) Configuration(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(http.StatusOK)
	configuration := storage.NewConfiguration()

	res := make(map[string][]byte)
	var err error
	res["Configuration"], err = configuration.ToBytes()
	if err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(res)
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

	h.states = append(h.states, storedeData{reqPutState.MAC, reqPutState.Data})

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
	return nil
}

func (h *MyHandler) GetStates(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(http.StatusOK)
	var resGetStates network.ResGetStates
	resGetStates.MACs = make([]objects.MAC, len(h.states))
	for i, state := range h.states {
		resGetStates.MACs[i] = state.MAC
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
		if state.MAC == reqGetState.MAC {
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
		if state.MAC == reqDeleteState.MAC {
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

	h.packfiles = append(h.packfiles, storedeData{reqPutPackfile.MAC, reqPutPackfile.Data})

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
	return nil
}

func (h *MyHandler) GetPackfiles(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(http.StatusOK)
	var resGetPackfiles network.ResGetPackfiles
	resGetPackfiles.MACs = make([]objects.MAC, len(h.packfiles))
	for i, packfile := range h.packfiles {
		resGetPackfiles.MACs[i] = packfile.MAC
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
		if packfile.MAC == reqGetPackfileBlob.MAC {
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
		if packfile.MAC == reqDeletePackfile.MAC {
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
		if packfile.MAC == reqGetPackfile.MAC {
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
	repo, err := NewRepository(map[string]string{"location": ts.URL})
	if err != nil {
		t.Fatal("error creating repository", err)
	}

	location := repo.Location()
	require.Equal(t, ts.URL, location)

	config := storage.NewConfiguration()
	serializedConfig, err := config.ToBytes()
	require.NoError(t, err)

	err = repo.Create(serializedConfig)
	require.NoError(t, err)

	_, err = repo.Open()
	require.NoError(t, err)
	//require.Equal(t, repo.Configuration().Version, versioning.FromString(storage.VERSION))

	err = repo.Close()
	require.NoError(t, err)

	// states
	MAC1 := objects.MAC{0x10, 0x20}
	MAC2 := objects.MAC{0x30, 0x40}
	err = repo.PutState(MAC1, bytes.NewReader([]byte("test1")))
	require.NoError(t, err)
	err = repo.PutState(MAC2, bytes.NewReader([]byte("test2")))
	require.NoError(t, err)

	states, err := repo.GetStates()
	require.NoError(t, err)
	expected := []objects.MAC{
		{0x10, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		{0x30, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
	}
	require.Equal(t, expected, states)

	rd, err := repo.GetState(MAC2)
	require.NoError(t, err)
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, rd)
	require.NoError(t, err)
	require.Equal(t, "test2", buf.String())

	err = repo.DeleteState(MAC1)
	require.NoError(t, err)

	states, err = repo.GetStates()
	require.NoError(t, err)
	expected = []objects.MAC{{0x30, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}}
	require.Equal(t, expected, states)

	// packfiles
	MAC3 := objects.MAC{0x50, 0x60}
	MAC4 := objects.MAC{0x60, 0x70}
	err = repo.PutPackfile(MAC3, bytes.NewReader([]byte("test3")))
	require.NoError(t, err)
	err = repo.PutPackfile(MAC4, bytes.NewReader([]byte("test4")))
	require.NoError(t, err)

	packfiles, err := repo.GetPackfiles()
	require.NoError(t, err)
	expected = []objects.MAC{
		{0x50, 0x60, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		{0x60, 0x70, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
	}
	require.Equal(t, expected, packfiles)

	rd, err = repo.GetPackfileBlob(MAC4, 0, 4)
	buf = new(bytes.Buffer)
	_, err = io.Copy(buf, rd)
	require.NoError(t, err)
	require.Equal(t, "test", buf.String())

	err = repo.DeletePackfile(MAC3)
	require.NoError(t, err)

	packfiles, err = repo.GetPackfiles()
	require.NoError(t, err)
	expected = []objects.MAC{{0x60, 0x70, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}}
	require.Equal(t, expected, packfiles)

	rd, err = repo.GetPackfile(MAC4)
	buf = new(bytes.Buffer)
	_, err = io.Copy(buf, rd)
	require.NoError(t, err)
	require.Equal(t, "test4", buf.String())
}
