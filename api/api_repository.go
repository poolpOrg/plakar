package api

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/gorilla/mux"
)

func repositoryConfiguration(w http.ResponseWriter, r *http.Request) error {
	configuration := lrepository.Configuration()
	return json.NewEncoder(w).Encode(configuration)
}

func repositorySnapshots(w http.ResponseWriter, r *http.Request) error {
	offset, _, err := QueryParamToUint32(r, "offset")
	if err != nil {
		return err
	}
	limit, _, err := QueryParamToUint32(r, "limit")
	if err != nil {
		return err
	}

	sortKeys, err := QueryParamToSortKeys(r, "sort", "Timestamp")
	if err != nil {
		return err
	}

	lrepository.RebuildState()

	snapshotIDs, err := lrepository.GetSnapshots()
	if err != nil {
		return err
	}

	headers := make([]header.Header, 0, len(snapshotIDs))
	for _, snapshotID := range snapshotIDs {
		snap, err := snapshot.Load(lrepository, snapshotID)
		if err != nil {
			return err
		}
		headers = append(headers, *snap.Header)
	}

	if limit == 0 {
		limit = uint32(len(headers))
	}

	header.SortHeaders(headers, sortKeys)
	if offset > uint32(len(headers)) {
		headers = []header.Header{}
	} else if offset+limit > uint32(len(headers)) {
		headers = headers[offset:]
	} else {
		headers = headers[offset : offset+limit]
	}

	items := Items{
		Total: len(snapshotIDs),
		Items: make([]interface{}, len(headers)),
	}
	for i, header := range headers {
		items.Items[i] = header
	}

	return json.NewEncoder(w).Encode(items)
}

func repositoryStates(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	_ = vars

	states, err := lrepository.GetStates()
	if err != nil {
		return err
	}

	items := Items{
		Total: len(states),
		Items: make([]interface{}, len(states)),
	}
	for i, state := range states {
		items.Items[i] = state
	}

	return json.NewEncoder(w).Encode(items)
}

func repositoryState(w http.ResponseWriter, r *http.Request) error {
	stateBytes32, err := PathParamToID(r, "state")
	if err != nil {
		return err
	}

	buffer, _, err := lrepository.GetState(stateBytes32)
	if err != nil {
		return err
	}

	if _, err := w.Write(buffer); err != nil {
		log.Println("write failed:", err)
	}
	return nil
}

func repositoryPackfiles(w http.ResponseWriter, r *http.Request) error {
	packfiles, err := lrepository.GetPackfiles()
	if err != nil {
		return err
	}

	items := Items{
		Total: len(packfiles),
		Items: make([]interface{}, len(packfiles)),
	}
	for i, packfile := range packfiles {
		items.Items[i] = packfile
	}

	return json.NewEncoder(w).Encode(items)
}

func repositoryPackfile(w http.ResponseWriter, r *http.Request) error {
	packfileBytes32, err := PathParamToID(r, "packfile")
	if err != nil {
		return err
	}

	offset, offsetExists, err := QueryParamToUint32(r, "offset")
	if err != nil {
		return err
	}

	length, lengthExists, err := QueryParamToUint32(r, "length")
	if err != nil {
		return err
	}

	if (offsetExists && !lengthExists) || (!offsetExists && lengthExists) {
		param := "offset"
		if !offsetExists {
			param = "length"
		}
		return parameterError(param, MissingArgument, ErrMissingField)
	}

	var rd io.Reader
	if offsetExists && lengthExists {
		rd, _, err = lrepository.GetPackfileBlob(packfileBytes32, offset, length)
		if err != nil {
			return err
		}
	} else {
		rd, _, err = lrepository.GetPackfile(packfileBytes32)
		if err != nil {
			return err
		}
	}
	if _, err := io.Copy(w, rd); err != nil {
		log.Println("copy failed:", err)
	}
	return nil
}
