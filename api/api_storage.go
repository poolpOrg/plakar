package api

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/PlakarKorp/plakar/objects"
)

func storageConfiguration(w http.ResponseWriter, r *http.Request) error {
	configuration := lstore.Configuration()
	return json.NewEncoder(w).Encode(configuration)
}

func storageStates(w http.ResponseWriter, r *http.Request) error {
	states, err := lstore.GetStates()
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

func storageState(w http.ResponseWriter, r *http.Request) error {
	stateBytes32, err := PathParamToID(r, "state")
	if err != nil {
		return err
	}

	rd, err := lstore.GetState(stateBytes32)
	if err != nil {
		return err
	}

	if _, err := io.Copy(w, rd); err != nil {
		log.Println("copy failed:", err)
	}
	return nil
}

func storagePackfiles(w http.ResponseWriter, r *http.Request) error {
	packfiles, err := lstore.GetPackfiles()
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

func storagePackfile(w http.ResponseWriter, r *http.Request) error {
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
		rd, err = lstore.GetPackfileBlob(packfileBytes32, uint32(offset), uint32(length))
		if err != nil {
			return err
		}
	} else {
		rd, err = lstore.GetPackfile(packfileBytes32)
		if err != nil {
			return err
		}
	}
	if _, err := io.Copy(w, rd); err != nil {
		log.Println("copy failed:", err)
	}
	return nil
}
