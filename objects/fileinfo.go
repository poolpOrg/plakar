//go:build !windows
// +build !windows

package objects

import (
	"errors"
	"io/fs"
	"os"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
)

type FileInfo struct {
	Lname      string      `json:"name" msgpack:"name"`
	Lsize      int64       `json:"size" msgpack:"size"`
	Lmode      fs.FileMode `json:"mode" msgpack:"mode"`
	LmodTime   time.Time   `json:"mod_time" msgpack:"mod_time"`
	Ldev       uint64      `json:"dev" msgpack:"dev"`
	Lino       uint64      `json:"ino" msgpack:"ino"`
	Luid       uint64      `json:"uid" msgpack:"uid"`
	Lgid       uint64      `json:"gid" msgpack:"gid"`
	Lnlink     uint16      `json:"nlink" msgpack:"nlink"`
	Lusername  string      `json:"username" msgpack:"username"`
	Lgroupname string      `json:"groupname" msgpack:"groupname"`
}

func (f FileInfo) Name() string {
	return f.Lname
}

func (f FileInfo) Size() int64 {
	return f.Lsize
}

func (f FileInfo) Mode() os.FileMode {
	return f.Lmode
}

func (f FileInfo) ModTime() time.Time {
	return f.LmodTime
}

func (f FileInfo) Dev() uint64 {
	return f.Ldev
}

func (f FileInfo) Ino() uint64 {
	return f.Lino
}

func (f FileInfo) Uid() uint64 {
	return f.Luid
}

func (f FileInfo) Gid() uint64 {
	return f.Lgid
}

func (f FileInfo) IsDir() bool {
	return f.Lmode.IsDir()
}

func (f FileInfo) Nlink() uint16 {
	return f.Lnlink
}

func (f FileInfo) Sys() any {
	return nil
}

func (f FileInfo) Username() string {
	return f.Lusername
}

func (f FileInfo) Groupname() string {
	return f.Lgroupname
}

func FileInfoFromStat(stat fs.FileInfo) FileInfo {
	Ldev := uint64(0)
	Lino := uint64(0)
	Luid := uint64(0)
	Lgid := uint64(0)
	Lnlink := uint16(0)

	if _, ok := stat.Sys().(*syscall.Stat_t); ok {
		Ldev = uint64(stat.Sys().(*syscall.Stat_t).Dev)
		Lino = uint64(stat.Sys().(*syscall.Stat_t).Ino)
		Luid = uint64(stat.Sys().(*syscall.Stat_t).Uid)
		Lgid = uint64(stat.Sys().(*syscall.Stat_t).Gid)
		Lnlink = uint16(stat.Sys().(*syscall.Stat_t).Nlink)
	}

	return FileInfo{
		Lname:    stat.Name(),
		Lsize:    stat.Size(),
		Lmode:    stat.Mode(),
		LmodTime: stat.ModTime(),
		Ldev:     Ldev,
		Lino:     Lino,
		Luid:     Luid,
		Lgid:     Lgid,
		Lnlink:   Lnlink,
	}
}

func NewFileInfo(name string, size int64, mode os.FileMode, modTime time.Time, dev uint64, ino uint64, uid uint64, gid uint64, nlink uint16) FileInfo {
	return FileInfo{
		Lname:    name,
		Lsize:    size,
		Lmode:    mode,
		LmodTime: modTime,
		Ldev:     dev,
		Lino:     ino,
		Luid:     uid,
		Lgid:     gid,
		Lnlink:   nlink,
	}
}

func (fileinfo *FileInfo) HumanSize() string {
	return humanize.Bytes(uint64(fileinfo.Size()))
}

func (fileinfo *FileInfo) Equal(fi *FileInfo) bool {
	return fileinfo.Lname == fi.Lname &&
		fileinfo.Lsize == fi.Lsize &&
		fileinfo.Lmode == fi.Lmode &&
		fileinfo.LmodTime == fi.LmodTime &&
		fileinfo.Ldev == fi.Ldev &&
		fileinfo.Lino == fi.Lino &&
		fileinfo.Luid == fi.Luid &&
		fileinfo.Lgid == fi.Lgid &&
		fileinfo.Lnlink == fi.Lnlink
}

var sortKeyMapping = map[string]string{
	"Name":      "Lname",
	"Size":      "Lsize",
	"Mode":      "Lmode",
	"ModTime":   "LmodTime",
	"Dev":       "Ldev",
	"Ino":       "Lino",
	"Uid":       "Luid",
	"Gid":       "Lgid",
	"Nlink":     "Lnlink",
	"Username":  "Lusername",
	"Groupname": "Lgroupname",
}

func ParseFileInfoSortKeys(sortKeysStr string) ([]string, error) {
	if sortKeysStr == "" {
		return nil, nil
	}
	keys := strings.Split(sortKeysStr, ",")
	uniqueKeys := make(map[string]bool)
	validKeys := []string{}

	for _, key := range keys {
		key = strings.TrimSpace(key)
		lookupKey := key
		if strings.HasPrefix(key, "-") {
			lookupKey = key[1:]
		}

		// Use the mapping to validate the key
		if _, found := sortKeyMapping[lookupKey]; !found {
			return nil, errors.New("invalid sort key: " + key)
		}
		if uniqueKeys[lookupKey] {
			return nil, errors.New("duplicate sort key: " + key)
		}
		uniqueKeys[lookupKey] = true
		validKeys = append(validKeys, key)
	}

	return validKeys, nil
}
func SortFileInfos(infos []FileInfo, sortKeys []string) error {
	var err error
	sort.Slice(infos, func(i, j int) bool {
		for _, key := range sortKeys {
			ascending := true
			if strings.HasPrefix(key, "-") {
				ascending = false
				key = key[1:]
			}

			// Use reflection with the mapped member variable
			field := sortKeyMapping[key]
			valI := reflect.ValueOf(infos[i]).FieldByName(field)
			valJ := reflect.ValueOf(infos[j]).FieldByName(field)

			if !valI.IsValid() || !valJ.IsValid() {
				err = errors.New("invalid sort key: " + key)
				return false
			}

			// Compare based on the field type
			switch valI.Kind() {
			case reflect.String:
				if valI.String() != valJ.String() {
					if ascending {
						return valI.String() < valJ.String()
					}
					return valI.String() > valJ.String()
				}
			case reflect.Int, reflect.Int64:
				if valI.Int() != valJ.Int() {
					if ascending {
						return valI.Int() < valJ.Int()
					}
					return valI.Int() > valJ.Int()
				}
			case reflect.Uint, reflect.Uint64:
				if valI.Uint() != valJ.Uint() {
					if ascending {
						return valI.Uint() < valJ.Uint()
					}
					return valI.Uint() > valJ.Uint()
				}
			default:
				err = errors.New("unsupported field type for sorting: " + key)
				return false
			}
		}
		return false
	})
	return err
}
