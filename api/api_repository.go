package api

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/header"
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

	_ = lrepository.RebuildState()

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

	_ = header.SortHeaders(headers, sortKeys)
	if offset > uint32(len(headers)) {
		headers = []header.Header{}
	} else if offset+limit > uint32(len(headers)) {
		headers = headers[offset:]
	} else {
		headers = headers[offset : offset+limit]
	}

	items := Items[header.Header]{
		Total: len(snapshotIDs),
		Items: make([]header.Header, len(headers)),
	}
	copy(items.Items, headers)

	return json.NewEncoder(w).Encode(items)
}

func repositoryStates(w http.ResponseWriter, r *http.Request) error {
	states, err := lrepository.GetStates()
	if err != nil {
		return err
	}

	items := Items[objects.Checksum]{
		Total: len(states),
		Items: make([]objects.Checksum, len(states)),
	}
	copy(items.Items, states)
	return json.NewEncoder(w).Encode(items)
}

func repositoryState(w http.ResponseWriter, r *http.Request) error {
	stateBytes32, err := PathParamToID(r, "state")
	if err != nil {
		return err
	}

	rd, err := lrepository.GetState(stateBytes32)
	if err != nil {
		return err
	}

	if _, err := io.Copy(w, rd); err != nil {
		log.Println("write failed:", err)
	}
	return nil
}

func repositoryPackfiles(w http.ResponseWriter, r *http.Request) error {
	packfiles, err := lrepository.GetPackfiles()
	if err != nil {
		return err
	}

	items := Items[objects.Checksum]{
		Total: len(packfiles),
		Items: make([]objects.Checksum, len(packfiles)),
	}
	copy(items.Items, packfiles)
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
		rd, err = lrepository.GetPackfileBlob(packfileBytes32, offset, length)
		if err != nil {
			return err
		}
	} else {
		rd, err = lrepository.GetPackfile(packfileBytes32)
		if err != nil {
			return err
		}
	}
	if _, err := io.Copy(w, rd); err != nil {
		log.Println("copy failed:", err)
	}
	return nil
}
