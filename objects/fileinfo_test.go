package objects

import (
	"syscall"
	"testing"
	"time"

	mocked_fileinfo "github.com/PlakarKorp/plakar/testing/fileinfo"
)

func TestParseFileInfoSortKeys(t *testing.T) {
	for _, test := range []struct {
		Sort  string
		Error string
		Keys  []string
	}{
		{
			Sort:  "",
			Error: "",
			Keys:  nil,
		},
		{
			Sort:  "Name,Name",
			Error: "duplicate sort key: Name",
			Keys:  nil,
		},
		{
			Sort:  "Name,Invalid",
			Error: "invalid sort key: Invalid",
			Keys:  nil,
		},
		{
			Sort:  "Mode,-Gid,Name",
			Error: "",
			Keys:  []string{"Mode", "-Gid", "Name"},
		},
	} {
		t.Run(test.Sort, func(t *testing.T) {
			keys, err := ParseFileInfoSortKeys(test.Sort)
			if err != nil {
				if err.Error() != test.Error {
					t.Fatalf("Expected %s but got %v", test.Error, err)
				}
			} else {
				if test.Error != "" {
					t.Fatalf("Expected %s but got nil", test.Error)
				}
				if len(keys) != len(test.Keys) {
					t.Fatalf("Expected %v but got %v", test.Keys, keys)
				}
				for i := range keys {
					if keys[i] != test.Keys[i] {
						t.Fatalf("Expected %v but got %v", test.Keys, keys)
					}
				}
			}
		})
	}
}

func TestSortFileInfos(t *testing.T) {
	infos := []FileInfo{
		{Lname: "file1", Lsize: 300, Lmode: 0644, LmodTime: time.Now(), Ldev: 0, Lino: 0, Luid: 0, Lgid: 0, Lnlink: 1},
		{Lname: "file2", Lsize: 400, Lmode: 0644, LmodTime: time.Now(), Ldev: 0, Lino: 0, Luid: 0, Lgid: 0, Lnlink: 1},
		{Lname: "file3", Lsize: 100, Lmode: 0644, LmodTime: time.Now(), Ldev: 0, Lino: 0, Luid: 0, Lgid: 0, Lnlink: 1},
		{Lname: "file4", Lsize: 100, Lmode: 0644, LmodTime: time.Now(), Ldev: 0, Lino: 0, Luid: 0, Lgid: uint64(42), Lnlink: 1},
	}

	for _, test := range []struct {
		Sort     string
		Expected []FileInfo
	}{
		{
			Sort:     "Name",
			Expected: []FileInfo{infos[0], infos[1], infos[2], infos[3]},
		},
		{
			Sort:     "Size",
			Expected: []FileInfo{infos[2], infos[3], infos[0], infos[1]},
		},
		{
			Sort:     "-Size",
			Expected: []FileInfo{infos[1], infos[0], infos[2], infos[3]},
		},
		{
			Sort:     "Size,-Name",
			Expected: []FileInfo{infos[3], infos[2], infos[0], infos[1]},
		},
		{
			Sort:     "Gid",
			Expected: []FileInfo{infos[2], infos[0], infos[1], infos[3]},
		},
		{
			Sort:     "-Gid",
			Expected: []FileInfo{infos[3], infos[2], infos[0], infos[1]},
		},
	} {
		t.Run(test.Sort, func(t *testing.T) {
			keys, err := ParseFileInfoSortKeys(test.Sort)
			if err != nil {
				t.Fatalf("Expected nil but got %v", keys)
			}
			err = SortFileInfos(infos, keys)
			if err != nil {
				t.Fatalf("Expected nil but got %v", err)
			}
			for i := range test.Expected {
				if infos[i] != test.Expected[i] {
					t.Fatalf("Expected %v but got %v", test.Expected[i], infos[i])
				}
			}
		})
	}
}

func TestSortFileInfosErrors(t *testing.T) {
	infos := []FileInfo{
		{Lname: "file1", Lsize: 300, Lmode: 0644, LmodTime: time.Now(), Ldev: 0, Lino: 0, Luid: 0, Lgid: 0, Lnlink: 1},
		{Lname: "file2", Lsize: 400, Lmode: 0644, LmodTime: time.Now(), Ldev: 0, Lino: 0, Luid: 0, Lgid: 0, Lnlink: 1},
	}

	for _, test := range []struct {
		name     string
		SortKeys []string
		Error    string
	}{
		{
			name:     "should fail for unknown field",
			SortKeys: []string{"Unknown"},
			Error:    "invalid sort key: Unknown",
		},
		{
			name:     "should fail to default because of type",
			SortKeys: []string{"ModTime"},
			Error:    "unsupported field type for sorting: ModTime",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := SortFileInfos(infos, test.SortKeys)
			if err == nil {
				t.Fatalf("Expected error but got %v", err)
			}
			if err.Error() != test.Error {
				t.Fatalf("Expected %v but got %v", test.Error, err)
			}
		})
	}
}

func TestNewFileInfo(t *testing.T) {
	now := time.Now().Local()

	file := NewFileInfo("file1", 300000, 0644, now, 1, 2, 3, 4, 5)
	reference := FileInfo{
		Lname:    "file1",
		Lsize:    300000,
		Lmode:    0644,
		LmodTime: now,
		Ldev:     1,
		Lino:     2,
		Luid:     3,
		Lgid:     4,
		Lnlink:   5,
	}

	if !file.Equal(&reference) {
		t.Errorf("expected %#v but got %#v", reference, file)
	}

	if file.HumanSize() != "300 kB" {
		t.Errorf("expected %#v but got %#v", "300 kB", file.HumanSize())
	}

}

func TestFileInfoFromStat(t *testing.T) {
	stat := mocked_fileinfo.New()

	username := "plakar"
	groupname := "plakarkorp"
	fileInfo := FileInfoFromStat(stat)
	fileInfo.Lusername = username
	fileInfo.Lgroupname = groupname

	if fileInfo.Name() != stat.Name() {
		t.Errorf("expected name %q, got %q", stat.Name(), fileInfo.Lname)
	}

	if fileInfo.Size() != stat.Size() {
		t.Errorf("expected size %d, got %d", stat.Size(), fileInfo.Lsize)
	}

	if fileInfo.Mode() != stat.Mode() {
		t.Errorf("expected mode %o, got %o", stat.Mode(), fileInfo.Lmode)
	}

	if fileInfo.ModTime() != stat.ModTime() {
		t.Errorf("expected mod time %v, got %v", stat.ModTime(), fileInfo.LmodTime)
	}

	if fileInfo.Dev() != uint64(stat.Sys().(*syscall.Stat_t).Dev) {
		t.Errorf("expected dev %d, got %d", stat.Sys().(*syscall.Stat_t).Dev, fileInfo.Ldev)
	}

	if fileInfo.Ino() != stat.Sys().(*syscall.Stat_t).Ino {
		t.Errorf("expected ino %d, got %d", stat.Sys().(*syscall.Stat_t).Ino, fileInfo.Lino)
	}

	if fileInfo.IsDir() != stat.IsDir() {
		t.Errorf("expected IsDir %v, got %v", stat.IsDir(), fileInfo.Lmode)
	}

	if fileInfo.Uid() != uint64(stat.Sys().(*syscall.Stat_t).Uid) {
		t.Errorf("expected uid %d, got %d", stat.Sys().(*syscall.Stat_t).Uid, fileInfo.Luid)
	}

	if fileInfo.Gid() != uint64(stat.Sys().(*syscall.Stat_t).Gid) {
		t.Errorf("expected gid %d, got %d", stat.Sys().(*syscall.Stat_t).Gid, fileInfo.Lgid)
	}

	if fileInfo.Nlink() != uint16(stat.Sys().(*syscall.Stat_t).Nlink) {
		t.Errorf("expected nlink %d, got %d", stat.Sys().(*syscall.Stat_t).Nlink, fileInfo.Lnlink)
	}

	if fileInfo.Username() != username {
		t.Errorf("expected Username %v, got %v", username, fileInfo.Lusername)
	}

	if fileInfo.Groupname() != groupname {
		t.Errorf("expected Groupname %v, got %v", groupname, fileInfo.Lgroupname)
	}
}
