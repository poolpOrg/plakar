package snapshot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"mime"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/classifier"
	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository/state"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/gabriel-vasile/mimetype"
	"github.com/gobwas/glob"
)

type BackupContext struct {
	aborted        atomic.Bool
	abortedReason  error
	imp            importer.Importer
	maxConcurrency chan bool
	scanCache      *caching.ScanCache

	erridx   *btree.BTree[string, int, *vfs.ErrorItem]
	muerridx sync.Mutex

	xattridx   *btree.BTree[string, int, *vfs.Xattr]
	muxattridx sync.Mutex
}

type BackupOptions struct {
	MaxConcurrency uint64
	Name           string
	Tags           []string
	Excludes       []glob.Glob
}

func (bc *BackupContext) recordEntry(entry *vfs.Entry) error {
	path := entry.Path()

	bytes, err := entry.ToBytes()
	if err != nil {
		return err
	}

	if entry.FileInfo.IsDir() {
		return bc.scanCache.PutDirectory(path, bytes)
	}
	return bc.scanCache.PutFile(path, bytes)
}

func (bc *BackupContext) recordError(path string, err error) error {
	bc.muerridx.Lock()
	e := bc.erridx.Insert(path, vfs.NewErrorItem(path, err.Error()))
	bc.muerridx.Unlock()
	return e
}

func (bc *BackupContext) recordXattr(record *importer.ScanRecord, objectMAC objects.MAC, size int64) error {
	xattr := vfs.NewXattr(record, objectMAC, size)
	bc.muxattridx.Lock()
	err := bc.xattridx.Insert(xattr.ToPath(), xattr)
	bc.muxattridx.Unlock()
	return err
}

func (snapshot *Snapshot) skipExcludedPathname(options *BackupOptions, record *importer.ScanResult) bool {
	var pathname string
	switch {
	case record.Record != nil:
		pathname = record.Record.Pathname
	case record.Error != nil:
		pathname = record.Error.Pathname
	}

	if pathname == "/" {
		return false
	}

	doExclude := false
	for _, exclude := range options.Excludes {
		if exclude.Match(pathname) {
			doExclude = true
			break
		}
	}
	return doExclude
}

func (snap *Snapshot) importerJob(backupCtx *BackupContext, options *BackupOptions) (chan *importer.ScanRecord, error) {
	scanner, err := backupCtx.imp.Scan()
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	filesChannel := make(chan *importer.ScanRecord, 1000)

	go func() {
		startEvent := events.StartImporterEvent()
		startEvent.SnapshotID = snap.Header.Identifier
		snap.Event(startEvent)

		nFiles := uint64(0)
		nDirectories := uint64(0)
		size := uint64(0)
		for _record := range scanner {
			if backupCtx.aborted.Load() {
				break
			}
			if snap.skipExcludedPathname(options, _record) {
				continue
			}

			backupCtx.maxConcurrency <- true
			wg.Add(1)
			go func(record *importer.ScanResult) {
				defer func() {
					<-backupCtx.maxConcurrency
					wg.Done()
				}()

				switch {
				case record.Error != nil:
					record := record.Error
					if record.Pathname == backupCtx.imp.Root() || len(record.Pathname) < len(backupCtx.imp.Root()) {
						backupCtx.aborted.Store(true)
						backupCtx.abortedReason = record.Err
						return
					}
					backupCtx.recordError(record.Pathname, record.Err)
					snap.Event(events.PathErrorEvent(snap.Header.Identifier, record.Pathname, record.Err.Error()))

				case record.Record != nil:
					record := record.Record
					snap.Event(events.PathEvent(snap.Header.Identifier, record.Pathname))

					if !record.FileInfo.Mode().IsDir() {
						filesChannel <- record
						if !record.IsXattr {
							atomic.AddUint64(&nFiles, +1)
							if record.FileInfo.Mode().IsRegular() {
								atomic.AddUint64(&size, uint64(record.FileInfo.Size()))
							}
							// if snapshot root is a file, then reset to the parent directory
							if snap.Header.GetSource(0).Importer.Directory == record.Pathname {
								snap.Header.GetSource(0).Importer.Directory = filepath.Dir(record.Pathname)
							}
						}
					} else {
						atomic.AddUint64(&nDirectories, +1)
						entry := vfs.NewEntry(path.Dir(record.Pathname), record)
						if err := backupCtx.recordEntry(entry); err != nil {
							backupCtx.recordError(record.Pathname, err)
							return
						}
					}
				}
			}(_record)
		}
		wg.Wait()
		close(filesChannel)
		doneEvent := events.DoneImporterEvent()
		doneEvent.SnapshotID = snap.Header.Identifier
		doneEvent.NumFiles = nFiles
		doneEvent.NumDirectories = nDirectories
		doneEvent.Size = size
		snap.Event(doneEvent)
	}()

	return filesChannel, nil
}

func (snap *Snapshot) Backup(scanDir string, imp importer.Importer, options *BackupOptions) error {
	snap.Event(events.StartEvent())
	defer snap.Event(events.DoneEvent())

	imp, err := importer.NewImporter(scanDir)
	if err != nil {
		return err
	}
	defer imp.Close()

	vfsCache, err := snap.AppContext().GetCache().VFS(imp.Type(), imp.Origin())
	if err != nil {
		return err
	}

	cf, err := classifier.NewClassifier(snap.AppContext())
	if err != nil {
		return err
	}
	defer cf.Close()

	snap.Header.GetSource(0).Importer.Origin = imp.Origin()
	snap.Header.GetSource(0).Importer.Type = imp.Type()
	snap.Header.Tags = append(snap.Header.Tags, options.Tags...)

	if options.Name == "" {
		snap.Header.Name = scanDir + " @ " + snap.Header.GetSource(0).Importer.Origin
	} else {
		snap.Header.Name = options.Name
	}

	if !strings.Contains(scanDir, "://") {
		scanDir, err = filepath.Abs(scanDir)
		if err != nil {
			snap.Logger().Warn("%s", err)
			return err
		}
	} else {
		scanDir = imp.Root()
	}
	snap.Header.GetSource(0).Importer.Directory = scanDir

	maxConcurrency := options.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = uint64(snap.AppContext().MaxConcurrency)
	}

	backupCtx := &BackupContext{
		imp:            imp,
		maxConcurrency: make(chan bool, maxConcurrency),
		scanCache:      snap.scanCache,
	}

	errstore := caching.DBStore[string, *vfs.ErrorItem]{
		Prefix: "__error__",
		Cache:  snap.scanCache,
	}
	backupCtx.erridx, err = btree.New(&errstore, strings.Compare, 50)
	if err != nil {
		return err
	}

	xattrstore := caching.DBStore[string, *vfs.Xattr]{
		Prefix: "__xattr__",
		Cache:  snap.scanCache,
	}
	backupCtx.xattridx, err = btree.New(&xattrstore, vfs.PathCmp, 50)
	if err != nil {
		return err
	}

	ctstore := caching.DBStore[string, objects.MAC]{
		Prefix: "__contenttype__",
		Cache:  snap.scanCache,
	}
	ctidx, err := btree.New(&ctstore, strings.Compare, 50)
	if err != nil {
		return err
	}
	var muctidx sync.Mutex

	/* backup starts now */
	beginTime := time.Now()

	/* importer */
	filesChannel, err := snap.importerJob(backupCtx, options)
	if err != nil {
		return err
	}

	/* scanner */
	scannerWg := sync.WaitGroup{}
	for _record := range filesChannel {
		select {
		case <-snap.AppContext().GetContext().Done():
			return snap.AppContext().GetContext().Err()
		default:
		}

		backupCtx.maxConcurrency <- true
		scannerWg.Add(1)
		go func(record *importer.ScanRecord) {
			defer func() {
				<-backupCtx.maxConcurrency
				scannerWg.Done()
			}()

			snap.Event(events.FileEvent(snap.Header.Identifier, record.Pathname))

			var fileEntry *vfs.Entry
			var object *objects.Object
			var objectMAC objects.MAC

			var cachedFileEntry *vfs.Entry
			var cachedFileEntryMAC objects.MAC

			// Check if the file entry and underlying objects are already in the cache
			if data, err := vfsCache.GetFilename(record.Pathname); err != nil {
				snap.Logger().Warn("VFS CACHE: Error getting filename: %v", err)
			} else if data != nil {
				cachedFileEntry, err = vfs.EntryFromBytes(data)
				if err != nil {
					snap.Logger().Warn("VFS CACHE: Error unmarshaling filename: %v", err)
				} else {
					cachedFileEntryMAC = snap.repository.ComputeMAC(data)
					if cachedFileEntry.Stat().Equal(&record.FileInfo) {
						fileEntry = cachedFileEntry
						if fileEntry.FileInfo.Mode().IsRegular() {
							data, err := vfsCache.GetObject(cachedFileEntry.Object)
							if err != nil {
								snap.Logger().Warn("VFS CACHE: Error getting object: %v", err)
							} else if data != nil {
								objectMAC = snap.Repository().ComputeMAC(data)
								cachedObject, err := objects.NewObjectFromBytes(data)
								if err != nil {
									snap.Logger().Warn("VFS CACHE: Error unmarshaling object: %v", err)
								} else {
									object = cachedObject
								}
							}
						}
					}
				}
			}

			// Chunkify the file if it is a regular file and we don't have a cached object
			if record.FileInfo.Mode().IsRegular() {
				if object == nil || !snap.BlobExists(resources.RT_OBJECT, objectMAC) {
					object, err = snap.chunkify(imp, cf, record)
					if err != nil {
						backupCtx.recordError(record.Pathname, err)
						return
					}

					serializedObject, err := object.Serialize()
					if err != nil {
						backupCtx.recordError(record.Pathname, err)
						return
					}
					objectMAC = snap.repository.ComputeMAC(serializedObject)
					if err := vfsCache.PutObject(objectMAC, serializedObject); err != nil {
						backupCtx.recordError(record.Pathname, err)
						return
					}
				}
			}

			if object != nil {
				if !snap.BlobExists(resources.RT_OBJECT, objectMAC) {
					data, err := object.Serialize()
					if err != nil {
						backupCtx.recordError(record.Pathname, err)
						return
					}
					objectMAC = snap.repository.ComputeMAC(data)
					err = snap.PutBlob(resources.RT_OBJECT, objectMAC, data)
					if err != nil {
						backupCtx.recordError(record.Pathname, err)
						return
					}
				}
			}

			// xattrs are a special case
			if record.IsXattr {
				backupCtx.recordXattr(record, objectMAC, object.Size())
				return
			}

			var fileEntryMAC objects.MAC
			if fileEntry != nil && snap.BlobExists(resources.RT_VFS_ENTRY, cachedFileEntryMAC) {
				fileEntryMAC = cachedFileEntryMAC
			} else {
				fileEntry = vfs.NewEntry(path.Dir(record.Pathname), record)
				if object != nil {
					fileEntry.Object = objectMAC
				}

				classifications := cf.Processor(record.Pathname).File(fileEntry)
				for _, result := range classifications {
					fileEntry.AddClassification(result.Analyzer, result.Classes)
				}

				serialized, err := fileEntry.ToBytes()
				if err != nil {
					backupCtx.recordError(record.Pathname, err)
					return
				}

				fileEntryMAC = snap.repository.ComputeMAC(serialized)
				err = snap.PutBlob(resources.RT_VFS_ENTRY, fileEntryMAC, serialized)
				if err != nil {
					backupCtx.recordError(record.Pathname, err)
					return
				}

				// Store the newly generated FileEntry in the cache for future runs
				err = vfsCache.PutFilename(record.Pathname, serialized)
				if err != nil {
					backupCtx.recordError(record.Pathname, err)
					return
				}

				fileSummary := &vfs.FileSummary{
					Size:    uint64(record.FileInfo.Size()),
					Mode:    record.FileInfo.Mode(),
					ModTime: record.FileInfo.ModTime().Unix(),
				}
				if object != nil {
					fileSummary.Objects++
					fileSummary.Chunks += uint64(len(object.Chunks))
					fileSummary.ContentType = object.ContentType
					fileSummary.Entropy = object.Entropy
				}

				seralizedFileSummary, err := fileSummary.Serialize()
				if err != nil {
					backupCtx.recordError(record.Pathname, err)
					return
				}

				err = vfsCache.PutFileSummary(record.Pathname, seralizedFileSummary)
				if err != nil {
					backupCtx.recordError(record.Pathname, err)
					return
				}
			}

			if object != nil {
				parts := strings.SplitN(object.ContentType, ";", 2)
				mime := parts[0]

				k := fmt.Sprintf("/%s%s", mime, fileEntry.Path())
				bytes, err := fileEntry.ToBytes()
				if err != nil {
					backupCtx.recordError(record.Pathname, err)
					return
				}
				muctidx.Lock()
				err = ctidx.Insert(k, snap.repository.ComputeMAC(bytes))
				muctidx.Unlock()
				if err != nil {
					backupCtx.recordError(record.Pathname, err)
					return
				}
			}

			if err := backupCtx.recordEntry(fileEntry); err != nil {
				backupCtx.recordError(record.Pathname, err)
				return
			}

			snap.Event(events.FileOKEvent(snap.Header.Identifier, record.Pathname, record.FileInfo.Size()))
		}(_record)
	}
	scannerWg.Wait()

	errcsum, err := persistIndex(snap, backupCtx.erridx, resources.RT_ERROR_BTREE, resources.RT_ERROR_NODE, func(e *vfs.ErrorItem) (csum objects.MAC, err error) {
		serialized, err := e.ToBytes()
		if err != nil {
			return
		}
		csum = snap.repository.ComputeMAC(serialized)
		if !snap.BlobExists(resources.RT_ERROR_ENTRY, csum) {
			err = snap.PutBlob(resources.RT_ERROR_ENTRY, csum, serialized)
		}
		return
	})
	if err != nil {
		return err
	}

	filestore := caching.DBStore[string, *vfs.Entry]{
		Prefix: "__path__",
		Cache:  snap.scanCache,
	}
	fileidx, err := btree.New(&filestore, vfs.PathCmp, 50)
	if err != nil {
		return err
	}

	var rootSummary *vfs.Summary

	diriter := backupCtx.scanCache.EnumerateKeysWithPrefix("__directory__:", true)
	for dirPath, bytes := range diriter {
		select {
		case <-snap.AppContext().GetContext().Done():
			return snap.AppContext().GetContext().Err()
		default:
		}

		dirEntry, err := vfs.EntryFromBytes(bytes)
		if err != nil {
			return err
		}

		prefix := dirPath
		if prefix != "/" {
			prefix += "/"
		}

		childiter := backupCtx.scanCache.EnumerateKeysWithPrefix("__file__:"+prefix, false)

		for relpath, bytes := range childiter {
			if strings.Contains(relpath, "/") {
				continue
			}

			childEntry, err := vfs.EntryFromBytes(bytes)
			if err != nil {
				return err
			}

			childPath := prefix + relpath

			if err := fileidx.Insert(childPath, childEntry); err != nil && err != btree.ErrExists {
				return err
			}

			data, err := vfsCache.GetFileSummary(childPath)
			if err != nil {
				continue
			}

			fileSummary, err := vfs.FileSummaryFromBytes(data)
			if err != nil {
				continue
			}

			dirEntry.Summary.Directory.Children++
			dirEntry.Summary.UpdateWithFileSummary(fileSummary)
		}

		subDirIter := backupCtx.scanCache.EnumerateKeysWithPrefix("__directory__:"+prefix, false)
		for relpath, _ := range subDirIter {
			if relpath == "" || strings.Contains(relpath, "/") {
				continue
			}

			childPath := prefix + relpath
			data, err := snap.scanCache.GetSummary(childPath)
			if err != nil {
				continue
			}

			childSummary, err := vfs.SummaryFromBytes(data)
			if err != nil {
				continue
			}
			dirEntry.Summary.Directory.Children++
			dirEntry.Summary.UpdateBelow(childSummary)
		}

		erriter, err := backupCtx.erridx.ScanFrom(prefix)
		if err != nil {
			return err
		}
		for erriter.Next() {
			_, errentry := erriter.Current()
			if !strings.HasPrefix(errentry.Name, prefix) {
				break
			}
			if strings.Index(errentry.Name[len(prefix):], "/") != -1 {
				break
			}
			dirEntry.Summary.Below.Errors++
		}
		if err := erriter.Err(); err != nil {
			return err
		}

		dirEntry.Summary.UpdateAverages()

		classifications := cf.Processor(dirPath).Directory(dirEntry)
		for _, result := range classifications {
			dirEntry.AddClassification(result.Analyzer, result.Classes)
		}

		serializedSummary, err := dirEntry.Summary.ToBytes()
		if err != nil {
			backupCtx.recordError(dirPath, err)
			return err
		}

		err = snap.scanCache.PutSummary(dirPath, serializedSummary)
		if err != nil {
			backupCtx.recordError(dirPath, err)
			return err
		}

		snap.Event(events.DirectoryOKEvent(snap.Header.Identifier, dirPath))
		if dirPath == "/" {
			if rootSummary != nil {
				panic("double /!")
			}
			rootSummary = dirEntry.Summary
		}

		if err := fileidx.Insert(dirPath, dirEntry); err != nil && err != btree.ErrExists {
			return err
		}

		if err := backupCtx.recordEntry(dirEntry); err != nil {
			return err
		}
	}

	rootcsum, err := persistIndex(snap, fileidx, resources.RT_VFS_BTREE, resources.RT_VFS_NODE, func(entry *vfs.Entry) (csum objects.MAC, err error) {
		serialized, err := entry.ToBytes()
		if err != nil {
			return
		}
		csum = snap.repository.ComputeMAC(serialized)
		if !snap.BlobExists(resources.RT_VFS_ENTRY, csum) {
			err = snap.PutBlob(resources.RT_VFS_ENTRY, csum, serialized)
		}
		return
	})
	if err != nil {
		return err
	}

	xattrcsum, err := persistIndex(snap, backupCtx.xattridx, resources.RT_XATTR_BTREE, resources.RT_XATTR_NODE, func(xattr *vfs.Xattr) (csum objects.MAC, err error) {
		serialized, err := xattr.ToBytes()
		if err != nil {
			return
		}
		csum = snap.repository.ComputeMAC(serialized)
		if !snap.BlobExists(resources.RT_XATTR_ENTRY, csum) {
			err = snap.PutBlob(resources.RT_XATTR_ENTRY, csum, serialized)
		}
		return
	})
	if err != nil {
		return err
	}

	ctmac, err := persistIndex(snap, ctidx, resources.RT_BTREE_ROOT, resources.RT_BTREE_NODE, func(mac objects.MAC) (objects.MAC, error) {
		return mac, nil
	})
	if err != nil {
		return err
	}

	if backupCtx.aborted.Load() {
		return backupCtx.abortedReason
	}

	snap.Header.GetSource(0).VFS = header.VFS{
		Root:   rootcsum,
		Xattrs: xattrcsum,
		Errors: errcsum,
	}
	snap.Header.Duration = time.Since(beginTime)
	snap.Header.GetSource(0).Summary = *rootSummary
	snap.Header.GetSource(0).Indexes = []header.Index{
		{
			Name:  "content-type",
			Type:  "btree",
			Value: ctmac,
		},
	}

	/*
		for _, key := range snap.Metadata.ListKeys() {
			objectType := strings.Split(key, ";")[0]
			objectKind := strings.Split(key, "/")[0]
			if objectType == "" {
				objectType = "unknown"
				objectKind = "unknown"
			}
			if _, exists := snap.Header.FileKind[objectKind]; !exists {
				snap.Header.FileKind[objectKind] = 0
			}
			snap.Header.FileKind[objectKind] += uint64(len(snap.Metadata.ListValues(key)))

			if _, exists := snap.Header.FileType[objectType]; !exists {
				snap.Header.FileType[objectType] = 0
			}
			snap.Header.FileType[objectType] += uint64(len(snap.Metadata.ListValues(key)))
		}

		for key, value := range snap.Header.FileType {
			snap.Header.FilePercentType[key] = math.Round((float64(value)/float64(snap.Header.FilesCount)*100)*100) / 100
		}
		for key, value := range snap.Header.FileKind {
			snap.Header.FilePercentKind[key] = math.Round((float64(value)/float64(snap.Header.FilesCount)*100)*100) / 100
		}
		for key, value := range snap.Header.FileExtension {
			snap.Header.FilePercentExtension[key] = math.Round((float64(value)/float64(snap.Header.FilesCount)*100)*100) / 100
		}
	*/
	return snap.Commit()
}

func entropy(data []byte) (float64, [256]float64) {
	if len(data) == 0 {
		return 0.0, [256]float64{}
	}

	// Count the frequency of each byte value
	var freq [256]float64
	for _, b := range data {
		freq[b]++
	}

	// Calculate the entropy
	entropy := 0.0
	dataSize := float64(len(data))
	for _, f := range freq {
		if f > 0 {
			p := f / dataSize
			entropy -= p * math.Log2(p)
		}
	}
	return entropy, freq
}

func (snap *Snapshot) chunkify(imp importer.Importer, cf *classifier.Classifier, record *importer.ScanRecord) (*objects.Object, error) {
	var rd io.ReadCloser
	var err error

	if record.IsXattr {
		rd, err = imp.NewExtendedAttributeReader(record.Pathname, record.XattrName)
	} else {
		rd, err = imp.NewReader(record.Pathname)
	}

	if err != nil {
		return nil, err
	}
	defer rd.Close()

	object := objects.NewObject()
	object.ContentType = mime.TypeByExtension(path.Ext(record.Pathname))

	objectHasher := snap.repository.GetMACHasher()

	var firstChunk = true
	var cdcOffset uint64
	var object_t32 objects.MAC

	var totalEntropy float64
	var totalFreq [256]float64
	var totalDataSize uint64

	// Helper function to process a chunk
	processChunk := func(data []byte) error {
		var chunk_t32 objects.MAC
		chunkHasher := snap.repository.GetMACHasher()

		if firstChunk {
			if object.ContentType == "" {
				object.ContentType = mimetype.Detect(data).String()
			}
			firstChunk = false
		}
		objectHasher.Write(data)

		chunkHasher.Reset()
		chunkHasher.Write(data)
		copy(chunk_t32[:], chunkHasher.Sum(nil))

		entropyScore, freq := entropy(data)
		if len(data) > 0 {
			for i := 0; i < 256; i++ {
				totalFreq[i] += freq[i]
			}
		}
		chunk := objects.NewChunk()
		chunk.ContentMAC = chunk_t32
		chunk.Length = uint32(len(data))
		chunk.Entropy = entropyScore

		object.Chunks = append(object.Chunks, *chunk)
		cdcOffset += uint64(len(data))

		totalEntropy += chunk.Entropy * float64(len(data))
		totalDataSize += uint64(len(data))

		if !snap.BlobExists(resources.RT_CHUNK, chunk.ContentMAC) {
			return snap.PutBlob(resources.RT_CHUNK, chunk.ContentMAC, data)
		}
		return nil
	}

	if record.FileInfo.Size() == 0 {
		// Produce an empty chunk for empty file
		if err := processChunk([]byte{}); err != nil {
			return nil, err
		}
	} else if record.FileInfo.Size() < int64(snap.repository.Configuration().Chunking.MinSize) {
		// Small file case: read entire file into memory
		buf, err := io.ReadAll(rd)
		if err != nil {
			return nil, err
		}
		if err := processChunk(buf); err != nil {
			return nil, err
		}
	} else {
		// Large file case: chunk file with chunker
		chk, err := snap.repository.Chunker(rd)
		if err != nil {
			return nil, err
		}
		for {
			cdcChunk, err := chk.Next()
			if err != nil && err != io.EOF {
				return nil, err
			}
			if cdcChunk == nil {
				break
			}
			if err := processChunk(cdcChunk); err != nil {
				return nil, err
			}
			if err == io.EOF {
				break
			}
		}
	}

	if totalDataSize > 0 {
		object.Entropy = totalEntropy / float64(totalDataSize)
	} else {
		object.Entropy = 0.0
	}

	copy(object_t32[:], objectHasher.Sum(nil))
	object.ContentMAC = object_t32
	return object, nil
}

func (snap *Snapshot) PutPackfile(packer *Packer) error {

	repo := snap.repository

	serializedData, err := packer.Packfile.SerializeData()
	if err != nil {
		panic("could not serialize pack file data" + err.Error())
	}
	serializedIndex, err := packer.Packfile.SerializeIndex()
	if err != nil {
		panic("could not serialize pack file index" + err.Error())
	}
	serializedFooter, err := packer.Packfile.SerializeFooter()
	if err != nil {
		panic("could not serialize pack file footer" + err.Error())
	}

	encryptedIndex, err := repo.EncodeBuffer(serializedIndex)
	if err != nil {
		return err
	}

	encryptedFooter, err := repo.EncodeBuffer(serializedFooter)
	if err != nil {
		return err
	}

	serializedPackfile := append(serializedData, encryptedIndex...)
	serializedPackfile = append(serializedPackfile, encryptedFooter...)

	/* it is necessary to track the footer _encrypted_ length */
	encryptedFooterLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(encryptedFooterLength, uint32(len(encryptedFooter)))
	serializedPackfile = append(serializedPackfile, encryptedFooterLength...)

	mac := snap.repository.ComputeMAC(serializedPackfile)

	repo.Logger().Trace("snapshot", "%x: PutPackfile(%x, ...)", snap.Header.GetIndexShortID(), mac)
	err = snap.repository.PutPackfile(mac, bytes.NewBuffer(serializedPackfile))
	if err != nil {
		panic("could not write pack file")
	}

	for _, Type := range packer.Types() {
		for blobMAC := range packer.Blobs[Type] {
			for idx, blob := range packer.Packfile.Index {
				if blob.MAC == blobMAC && blob.Type == Type {
					delta := state.DeltaEntry{
						Type:    blob.Type,
						Version: packer.Packfile.Index[idx].Version,
						Blob:    blobMAC,
						Location: state.Location{
							Packfile: mac,
							Offset:   packer.Packfile.Index[idx].Offset,
							Length:   packer.Packfile.Index[idx].Length,
						},
					}

					if err := snap.deltaState.PutDelta(delta); err != nil {
						return err
					}

					break
				}
			}
		}
	}

	if err := snap.deltaState.PutPackfile(snap.Header.Identifier, mac); err != nil {
		return err
	}

	return nil
}

func (snap *Snapshot) Commit() error {
	repo := snap.repository

	serializedHdr, err := snap.Header.Serialize()
	if err != nil {
		return err
	}

	if kp := snap.AppContext().Keypair; kp != nil {
		serializedHdrMAC := snap.repository.ComputeMAC(serializedHdr)
		signature := kp.Sign(serializedHdrMAC[:])
		if err := snap.PutBlob(resources.RT_SIGNATURE, snap.Header.Identifier, signature); err != nil {
			return err
		}
	}

	if err := snap.PutBlob(resources.RT_SNAPSHOT, snap.Header.Identifier, serializedHdr); err != nil {
		return err
	}

	close(snap.packerChan)
	<-snap.packerChanDone

	stateDelta := snap.buildSerializedDeltaState()
	err = repo.PutState(snap.Header.Identifier, stateDelta)
	if err != nil {
		snap.Logger().Warn("Failed to push the state to the repository %s", err)
		return err
	}

	snap.Logger().Trace("snapshot", "%x: Commit()", snap.Header.GetIndexShortID())
	return nil
}

func (snap *Snapshot) buildSerializedDeltaState() io.Reader {
	pr, pw := io.Pipe()

	/* By using a pipe and a goroutine we bound the max size in memory. */
	go func() {
		defer pw.Close()
		if err := snap.deltaState.SerializeToStream(pw); err != nil {
			pw.CloseWithError(err)
		}
	}()

	return pr
}
