package api

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/search"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/alecthomas/chroma/formatters"
	"github.com/alecthomas/chroma/lexers"
	"github.com/alecthomas/chroma/styles"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"go.omarpolo.com/ttlmap"
)

type downloadSignedUrl struct {
	snapshotID [32]byte
	rebase     bool
	files      []string
}

var downloadSignedUrls = ttlmap.New[string, downloadSignedUrl](1 * time.Hour)

func init() {
	downloadSignedUrls.AutoExpire()
}

func snapshotHeader(w http.ResponseWriter, r *http.Request) error {
	snapshotID32, err := PathParamToID(r, "snapshot")
	if err != nil {
		return err
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(Item[*header.Header]{Item: snap.Header})
}

func snapshotReader(w http.ResponseWriter, r *http.Request) error {
	snapshotID32, path, err := SnapshotPathParam(r, lrepository, "snapshot_path")
	if err != nil {
		return err
	}

	do_highlight := false
	do_download := false

	download := r.URL.Query().Get("download")
	if download == "true" {
		do_download = true
	}

	render := r.URL.Query().Get("render")
	if render == "highlight" {
		do_highlight = true
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	entry, err := fs.GetEntry(path)
	if err != nil {
		return err
	}

	file := entry.Open(fs, path)
	defer file.Close()

	if !entry.Stat().Mode().IsRegular() {
		return nil
	}

	if do_download {
		w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(filepath.Base(path)))
	}

	if do_highlight {
		lexer := lexers.Match(path)
		if lexer == nil {
			lexer = lexers.Get(entry.Object.ContentType)
		}
		if lexer == nil {
			lexer = lexers.Fallback // Fallback if no lexer is found
		}
		formatter := formatters.Get("html")
		style := styles.Get("dracula")

		w.Header().Set("Content-Type", "text/html")
		if _, err := w.Write([]byte("<!DOCTYPE html>")); err != nil {
			return err
		}

		reader := bufio.NewReader(file)
		buffer := make([]byte, 4096) // Fixed-size buffer for chunked reading
		for {
			n, err := reader.Read(buffer) // Read up to the size of the buffer
			if n > 0 {
				chunk := string(buffer[:n])

				// Tokenize the chunk and apply syntax highlighting
				iterator, errTokenize := lexer.Tokenise(nil, chunk)
				if errTokenize != nil {
					break
				}

				errFormat := formatter.Format(w, style, iterator)
				if errFormat != nil {
					break
				}
			}

			// Check for end of file (EOF)
			if err == io.EOF {
				break
			} else if err != nil {
				break
			}
		}
	} else {
		http.ServeContent(w, r, filepath.Base(path), entry.Stat().ModTime(), file.(io.ReadSeeker))
	}

	return nil
}

type SnapshotReaderURLSigner struct {
	token string
}

func NewSnapshotReaderURLSigner(token string) SnapshotReaderURLSigner {
	return SnapshotReaderURLSigner{token}
}

type SnapshotSignedURLClaims struct {
	SnapshotID string `json:"snapshot_id"`
	Path       string `json:"path"`
	jwt.RegisteredClaims
}

func (signer SnapshotReaderURLSigner) Sign(w http.ResponseWriter, r *http.Request) error {
	snapshotID32, path, err := SnapshotPathParam(r, lrepository, "snapshot_path")
	if err != nil {
		return err
	}
	snapshotId := fmt.Sprintf("%0x", snapshotID32[:])

	now := time.Now()
	jwtToken := jwt.NewWithClaims(jwt.SigningMethodHS256, SnapshotSignedURLClaims{
		SnapshotID: snapshotId,
		Path:       path,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(2 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
			Issuer:    "plakar-api",
		},
	})

	signature, err := jwtToken.SignedString([]byte(signer.token))
	if err != nil {
		return err
	}

	type Signature struct {
		Signature string `json:"signature"`
	}

	return json.NewEncoder(w).Encode(Item[Signature]{
		Signature{signature},
	})
}

// VerifyMiddleware is a middleware that checks if the request to read the file
// content is authorized. It checks if the ?signature query parameter is valid.
// If it is not valid, it falls back to the Authorization header.
func (signer SnapshotReaderURLSigner) VerifyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signature := r.URL.Query().Get("signature")

		// No signature provided, fall back to Authorization header
		if signature == "" {
			TokenAuthMiddleware(signer.token)(next).ServeHTTP(w, r)
			return
		}

		snapshotID32, path, err := SnapshotPathParam(r, lrepository, "snapshot_path")
		if err != nil {
			handleError(w, r, parameterError("snapshot_path", InvalidArgument, err))
			return
		}
		snapshotId := fmt.Sprintf("%0x", snapshotID32[:])

		jwtToken, err := jwt.ParseWithClaims(signature, &SnapshotSignedURLClaims{}, func(jwtToken *jwt.Token) (interface{}, error) {
			if _, ok := jwtToken.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, authError(fmt.Sprintf("unexpected signing method: %v", jwtToken.Header["alg"]))
			}
			return []byte(signer.token), nil
		})

		if err != nil {
			if errors.Is(err, jwt.ErrTokenExpired) {
				handleError(w, r, authError("token expired"))
				return
			}
			handleError(w, r, authError(fmt.Sprintf("unable to parse JWT token: %v", err)))
			return
		}

		if claims, ok := jwtToken.Claims.(*SnapshotSignedURLClaims); ok {
			if claims.Path != path {
				handleError(w, r, authError("invalid URL path"))
				return
			}
			if claims.SnapshotID != snapshotId {
				handleError(w, r, authError("invalid URL snapshot"))
				return
			}
		} else {
			handleError(w, r, authError("invalid URL signature"))
			return
		}

		next.ServeHTTP(w, r)
	})
}

func snapshotVFSBrowse(w http.ResponseWriter, r *http.Request) error {
	snapshotID32, path, err := SnapshotPathParam(r, lrepository, "snapshot_path")
	if err != nil {
		return err
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	if path == "" {
		path = "/"
	}
	entry, err := fs.GetEntry(path)
	if err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(Item[*vfs.Entry]{Item: entry})
}

func snapshotVFSChildren(w http.ResponseWriter, r *http.Request) error {
	snapshotID32, path, err := SnapshotPathParam(r, lrepository, "snapshot_path")
	if err != nil {
		return err
	}

	offset, _, err := QueryParamToInt64(r, "offset")
	if err != nil {
		return err
	}

	limit, _, err := QueryParamToInt64(r, "limit")
	if err != nil {
		return err
	}

	sortKeysStr := r.URL.Query().Get("sort")
	if sortKeysStr == "" {
		sortKeysStr = "Name"
	}
	sortKeys, err := objects.ParseFileInfoSortKeys(sortKeysStr)
	if err != nil {
		return parameterError("sort", InvalidArgument, err)
	}
	_ = sortKeys

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	if path == "" {
		path = "/"
	}
	fsinfo, err := fs.GetEntry(path)
	if err != nil {
		return err
	}

	if !fsinfo.Stat().Mode().IsDir() {
		http.Error(w, "not a directory", http.StatusBadRequest)
		return nil
	}

	items := Items[*vfs.Entry]{
		Total: int(fsinfo.Summary.Directory.Children),
		Items: make([]*vfs.Entry, 0),
	}
	iter, err := fsinfo.Getdents(fs)
	if err != nil {
		return err
	}

	if limit == 0 {
		limit = int64(fsinfo.Summary.Directory.Children)
	}

	i := int64(0)
	for child := range iter {
		if child == nil {
			break
		}
		if i < offset {
			i++
			continue
		}
		if i >= limit+offset {
			break
		}
		items.Items = append(items.Items, child)
		i++
	}
	return json.NewEncoder(w).Encode(items)
}

func snapshotVFSErrors(w http.ResponseWriter, r *http.Request) error {
	snapshotID32, path, err := SnapshotPathParam(r, lrepository, "snapshot_path")
	if err != nil {
		return err
	}

	sortKeysStr := r.URL.Query().Get("sort")
	if sortKeysStr == "" {
		sortKeysStr = "Name"
	}
	if sortKeysStr != "Name" && sortKeysStr != "-Name" {
		return parameterError("sort", InvalidArgument, ErrInvalidSortKey)
	}

	offset, _, err := QueryParamToInt64(r, "offset")
	if err != nil {
		return err
	}

	limit, _, err := QueryParamToInt64(r, "limit")
	if err != nil {
		return err
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	if path == "" {
		path = "/"
	}

	errorList, err := snap.Errors(path)
	if err != nil {
		return err
	}

	var i int64
	items := Items[snapshot.ErrorItem]{
		Items: []snapshot.ErrorItem{},
	}
	for errorEntry := range errorList {
		if i < offset {
			i++
			continue
		}
		if limit > 0 && i >= limit+offset {
			i++
			continue
		}
		items.Items = append(items.Items, errorEntry)
		i++
	}
	items.Total = int(i)
	return json.NewEncoder(w).Encode(items)
}

func snapshotSearch(w http.ResponseWriter, r *http.Request) error {
	snapshotID32, path, err := SnapshotPathParam(r, lrepository, "snapshot_path")
	if err != nil {
		return err
	}

	queryStr := r.URL.Query().Get("q")

	offset, _, err := QueryParamToInt64(r, "offset")
	if err != nil {
		return err
	}

	limit, _, err := QueryParamToInt64(r, "limit")
	if err != nil {
		return err
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	results, err := snap.Search(path, queryStr)
	if err != nil {
		return err
	}

	items := Items[search.FileEntry]{
		Total: 0,
		Items: make([]search.FileEntry, 0),
	}
	i := int64(0)
	for result := range results {
		if i >= offset {
			if limit != 0 {
				if i >= limit+offset {
					break
				}
			}
			if entry, isFilename := result.(search.FileEntry); isFilename {
				items.Total += 1
				items.Items = append(items.Items, entry)
			}
		}
		i++
	}

	return json.NewEncoder(w).Encode(items)
}

type DownloadItem struct {
	Pathname string `json:"pathname"`
}
type DownloadQuery struct {
	Name   string         `json:"name"`
	Items  []DownloadItem `json:"items"`
	Rebase bool           `json:"rebase,omitempty"`
}

func snapshotVFSDownloader(w http.ResponseWriter, r *http.Request) error {
	snapshotID32, _, err := SnapshotPathParam(r, lrepository, "snapshot_path")
	if err != nil {
		return err
	}

	var query DownloadQuery
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		return parameterError("BODY", InvalidArgument, err)
	}

	if _, err = snapshot.Load(lrepository, snapshotID32); err != nil {
		return nil
	}

	for {
		id := uuid.New().String()
		if _, ok := downloadSignedUrls.Get(id); ok {
			continue
		}

		url := downloadSignedUrl{
			snapshotID: snapshotID32,
			rebase:     query.Rebase,
		}

		for _, item := range query.Items {
			url.files = append(url.files, item.Pathname)
		}

		downloadSignedUrls.Add(id, url)
		res := struct {
			Id string `json:"id"`
		}{id}

		json.NewEncoder(w).Encode(&res)
		return nil
	}
}

func snapshotVFSDownloaderSigned(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")

	link, ok := downloadSignedUrls.Get(id)
	if !ok {
		return &ApiError{
			HttpCode: 404,
			ErrCode:  "signed-link-not-found",
			Message:  "Signed Link Not Found",
		}
	}

	snap, err := snapshot.Load(lrepository, link.snapshotID)
	if err != nil {
		return err
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		name = fmt.Sprintf("snapshot-%x-%s", link.snapshotID[:4], time.Now().Format("2006-01-02-15-04-05"))
	}

	format := r.URL.Query().Get("format")
	var mime string
	switch format {
	case snapshot.ArchiveTar:
		mime = "application/x-tar"
	case snapshot.ArchiveTarball:
		mime = "application/x-gzip"
	case snapshot.ArchiveZip:
		mime = "application/zip"
	default:
		return &ApiError{
			HttpCode: 400,
			ErrCode: "unknown-archive-format",
			Message: "Unknown Archive Format",
		}
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", name))
	w.Header().Set("Content-Type", mime)

	return snap.Archive(w, format, link.files, link.rebase)
}
