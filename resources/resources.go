package resources

type Type uint32

const (
	RT_STATE       Type = 1
	RT_PACKFILE    Type = 2
	RT_SNAPSHOT    Type = 3
	RT_CHUNK       Type = 4
	RT_OBJECT      Type = 5
	RT_VFS         Type = 6
	RT_VFS_ENTRY   Type = 7
	RT_INDEX       Type = 8
	RT_INDEX_ENTRY Type = 9
	RT_SIGNATURE   Type = 10
	RT_ERROR       Type = 11
)

func Types() []Type {
	return []Type{
		RT_STATE,
		RT_PACKFILE,
		RT_SNAPSHOT,
		RT_CHUNK,
		RT_OBJECT,
		RT_VFS,
		RT_VFS_ENTRY,
		RT_INDEX,
		RT_INDEX_ENTRY,
		RT_SIGNATURE,
		RT_ERROR,
	}
}

func (r Type) String() string {
	switch r {
	case RT_STATE:
		return "state"
	case RT_PACKFILE:
		return "packfile"
	case RT_SNAPSHOT:
		return "snapshot"
	case RT_CHUNK:
		return "chunk"
	case RT_OBJECT:
		return "object"
	case RT_VFS:
		return "vfs"
	case RT_VFS_ENTRY:
		return "vfs entry"
	case RT_INDEX:
		return "index"
	case RT_INDEX_ENTRY:
		return "index entry"
	case RT_SIGNATURE:
		return "signature"
	case RT_ERROR:
		return "error"
	default:
		return "unknown"
	}
}
