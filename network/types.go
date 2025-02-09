package network

import (
	"encoding/gob"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/google/uuid"
)

type Request struct {
	Uuid    uuid.UUID
	Type    string
	Payload interface{}
}

type ReqCreate struct {
	Repository    string
	Configuration storage.Configuration
}

type ResCreate struct {
	Err string
}

type ReqOpen struct {
	Repository string
}

type ResOpen struct {
	Configuration *storage.Configuration
	Err           string
}

type ReqClose struct {
	Uuid string
}

type ResClose struct {
	Err string
}

// states
type ReqGetStates struct {
}

type ResGetStates struct {
	Checksums []objects.Checksum
	Err       string
}

type ReqPutState struct {
	Checksum objects.Checksum
	Data     []byte
}

type ResPutState struct {
	Err string
}

type ReqGetState struct {
	Checksum objects.Checksum
}

type ResGetState struct {
	Data []byte
	Err  string
}

type ReqDeleteState struct {
	Checksum objects.Checksum
}
type ResDeleteState struct {
	Err string
}

// packfiles
type ReqGetPackfiles struct {
}

type ResGetPackfiles struct {
	Checksums []objects.Checksum
	Err       string
}

type ReqPutPackfile struct {
	Checksum objects.Checksum
	Data     []byte
}

type ResPutPackfile struct {
	Err string
}

type ReqGetPackfile struct {
	Checksum objects.Checksum
}

type ResGetPackfile struct {
	Data []byte
	Err  string
}

type ReqGetPackfileBlob struct {
	Checksum objects.Checksum
	Offset   uint64
	Length   uint32
}

type ResGetPackfileBlob struct {
	Data []byte
	Err  string
}

type ReqDeletePackfile struct {
	Checksum objects.Checksum
}
type ResDeletePackfile struct {
	Err string
}

func ProtocolRegister() {
	gob.Register(Request{})

	gob.Register(ReqCreate{})
	gob.Register(ResCreate{})

	gob.Register(ReqOpen{})
	gob.Register(ResOpen{})

	gob.Register(ReqClose{})
	gob.Register(ResClose{})

	// states
	gob.Register(ReqGetStates{})
	gob.Register(ResGetStates{})

	gob.Register(ReqPutState{})
	gob.Register(ResPutState{})

	gob.Register(ReqGetState{})
	gob.Register(ResGetState{})

	gob.Register(ReqDeleteState{})
	gob.Register(ResDeleteState{})

	// packfiles
	gob.Register(ReqGetPackfiles{})
	gob.Register(ResGetPackfiles{})

	gob.Register(ReqPutPackfile{})
	gob.Register(ResPutPackfile{})

	gob.Register(ReqGetPackfile{})
	gob.Register(ResGetPackfile{})

	gob.Register(ReqGetPackfileBlob{})
	gob.Register(ResGetPackfileBlob{})

	gob.Register(ReqDeletePackfile{})
	gob.Register(ResDeletePackfile{})
}
