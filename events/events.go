package events

import (
	"time"
)

type Event interface {
	Timestamp() time.Time
}

/**/
type Start struct {
	ts time.Time
}

func StartEvent() Start {
	return Start{ts: time.Now()}
}
func (e Start) Timestamp() time.Time {
	return e.ts
}

/**/
type Done struct {
	ts time.Time
}

func DoneEvent() Done {
	return Done{ts: time.Now()}
}
func (e Done) Timestamp() time.Time {
	return e.ts
}

/**/
type Warning struct {
	ts time.Time

	SnapshotID [32]byte
	Message    string
}

func WarningEvent(snapshotID [32]byte, message string) Warning {
	return Warning{ts: time.Now(), SnapshotID: snapshotID, Message: message}
}
func (e Warning) Timestamp() time.Time {
	return e.ts
}

/**/
type Error struct {
	ts time.Time

	SnapshotID [32]byte
	Message    string
}

func ErrorEvent(snapshotID [32]byte, message string) Error {
	return Error{ts: time.Now(), SnapshotID: snapshotID, Message: message}
}
func (e Error) Timestamp() time.Time {
	return e.ts
}

/**/
type Path struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
}

func PathEvent(snapshotID [32]byte, pathname string) Path {
	return Path{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname}
}
func (e Path) Timestamp() time.Time {
	return e.ts
}

/**/
type PathError struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
	Message    string
}

func PathErrorEvent(snapshotID [32]byte, pathname string, message string) PathError {
	return PathError{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname, Message: message}
}
func (e PathError) Timestamp() time.Time {
	return e.ts
}

/**/
type Directory struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
}

func DirectoryEvent(snapshotID [32]byte, pathname string) Directory {
	return Directory{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname}
}
func (e Directory) Timestamp() time.Time {
	return e.ts
}

/**/
type File struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
}

func FileEvent(snapshotID [32]byte, pathname string) File {
	return File{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname}
}
func (e File) Timestamp() time.Time {
	return e.ts
}

/**/
type Object struct {
	ts time.Time

	SnapshotID [32]byte
	Checksum   [32]byte
}

func ObjectEvent(snapshotID [32]byte, checksum [32]byte) Object {
	return Object{ts: time.Now(), SnapshotID: snapshotID, Checksum: checksum}
}
func (e Object) Timestamp() time.Time {
	return e.ts
}

/**/
type Chunk struct {
	ts time.Time

	SnapshotID [32]byte
	Checksum   [32]byte
}

func ChunkEvent(snapshotID [32]byte, checksum [32]byte) Chunk {
	return Chunk{ts: time.Now(), SnapshotID: snapshotID, Checksum: checksum}
}
func (e Chunk) Timestamp() time.Time {
	return e.ts
}

/**/
type DirectoryOK struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
}

func DirectoryOKEvent(snapshotID [32]byte, pathname string) DirectoryOK {
	return DirectoryOK{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname}
}
func (e DirectoryOK) Timestamp() time.Time {
	return e.ts
}

/**/
type DirectoryError struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
	Message    string
}

func DirectoryErrorEvent(snapshotID [32]byte, pathname string, message string) DirectoryError {
	return DirectoryError{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname, Message: message}
}
func (e DirectoryError) Timestamp() time.Time {
	return e.ts
}

/**/
type DirectoryMissing struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
}

func DirectoryMissingEvent(snapshotID [32]byte, pathname string) DirectoryMissing {
	return DirectoryMissing{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname}
}
func (e DirectoryMissing) Timestamp() time.Time {
	return e.ts
}

/**/
type DirectoryCorrupted struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
}

func DirectoryCorruptedEvent(snapshotID [32]byte, pathname string) DirectoryCorrupted {
	return DirectoryCorrupted{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname}
}
func (e DirectoryCorrupted) Timestamp() time.Time {
	return e.ts
}

/**/
type FileOK struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
}

func FileOKEvent(snapshotID [32]byte, pathname string) FileOK {
	return FileOK{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname}
}
func (e FileOK) Timestamp() time.Time {
	return e.ts
}

/**/
type FileError struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
	Message    string
}

func FileErrorEvent(snapshotID [32]byte, pathname string, message string) FileError {
	return FileError{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname, Message: message}
}
func (e FileError) Timestamp() time.Time {
	return e.ts
}

/**/
type FileMissing struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
}

func FileMissingdEvent(snapshotID [32]byte, pathname string) FileMissing {
	return FileMissing{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname}
}
func (e FileMissing) Timestamp() time.Time {
	return e.ts
}

/**/
type FileCorrupted struct {
	ts time.Time

	SnapshotID [32]byte
	Pathname   string
}

func FileCorruptedEvent(snapshotID [32]byte, pathname string) FileCorrupted {
	return FileCorrupted{ts: time.Now(), SnapshotID: snapshotID, Pathname: pathname}
}
func (e FileCorrupted) Timestamp() time.Time {
	return e.ts
}

/**/
type ObjectOK struct {
	ts time.Time

	SnapshotID [32]byte
	Checksum   [32]byte
}

func ObjectOKEvent(snapshotID [32]byte, checksum [32]byte) ObjectOK {
	return ObjectOK{ts: time.Now(), SnapshotID: snapshotID, Checksum: checksum}
}
func (e ObjectOK) Timestamp() time.Time {
	return e.ts
}

/**/
type ObjectMissing struct {
	ts time.Time

	SnapshotID [32]byte
	Checksum   [32]byte
}

func ObjectMissingEvent(snapshotID [32]byte, checksum [32]byte) ObjectMissing {
	return ObjectMissing{ts: time.Now(), SnapshotID: snapshotID, Checksum: checksum}
}
func (e ObjectMissing) Timestamp() time.Time {
	return e.ts
}

/**/
type ObjectCorrupted struct {
	ts time.Time

	SnapshotID [32]byte
	Checksum   [32]byte
}

func ObjectCorruptedEvent(snapshotID [32]byte, checksum [32]byte) ObjectCorrupted {
	return ObjectCorrupted{ts: time.Now(), SnapshotID: snapshotID, Checksum: checksum}
}
func (e ObjectCorrupted) Timestamp() time.Time {
	return e.ts
}

/**/
type ChunkOK struct {
	ts time.Time

	SnapshotID [32]byte
	Checksum   [32]byte
}

func ChunkOKEvent(snapshotID [32]byte, checksum [32]byte) ChunkOK {
	return ChunkOK{ts: time.Now(), SnapshotID: snapshotID, Checksum: checksum}
}
func (e ChunkOK) Timestamp() time.Time {
	return e.ts
}

/**/
type ChunkMissing struct {
	ts time.Time

	SnapshotID [32]byte
	Checksum   [32]byte
}

func ChunkMissingEvent(snapshotID [32]byte, checksum [32]byte) ChunkMissing {
	return ChunkMissing{ts: time.Now(), SnapshotID: snapshotID, Checksum: checksum}
}
func (e ChunkMissing) Timestamp() time.Time {
	return e.ts
}

/**/
type ChunkCorrupted struct {
	ts time.Time

	SnapshotID [32]byte
	Checksum   [32]byte
}

func ChunkCorruptedEvent(snapshotID [32]byte, checksum [32]byte) ChunkCorrupted {
	return ChunkCorrupted{ts: time.Now(), SnapshotID: snapshotID, Checksum: checksum}
}
func (e ChunkCorrupted) Timestamp() time.Time {
	return e.ts
}

/**/
type StartImporter struct {
	ts time.Time

	SnapshotID [32]byte
}

func StartImporterEvent() StartImporter {
	return StartImporter{ts: time.Now()}
}
func (e StartImporter) Timestamp() time.Time {
	return e.ts
}

/**/
type DoneImporter struct {
	ts time.Time

	SnapshotID     [32]byte
	NumFiles       uint64
	NumDirectories uint64
	Size           uint64
}

func DoneImporterEvent() DoneImporter {
	return DoneImporter{ts: time.Now()}
}
func (e DoneImporter) Timestamp() time.Time {
	return e.ts
}
