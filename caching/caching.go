package caching

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/google/uuid"
)

const CACHE_VERSION = "1.0.0"

type Manager struct {
	cacheDir string

	repositoryCache      map[uuid.UUID]*_RepositoryCache
	repositoryCacheMutex sync.Mutex

	vfsCache      map[string]*_VFSCache
	vfsCacheMutex sync.Mutex

	maintainanceCache      map[uuid.UUID]*MaintainanceCache
	maintainanceCacheMutex sync.Mutex
}

func NewManager(cacheDir string) *Manager {
	return &Manager{
		cacheDir: filepath.Join(cacheDir, CACHE_VERSION),

		repositoryCache:   make(map[uuid.UUID]*_RepositoryCache),
		vfsCache:          make(map[string]*_VFSCache),
		maintainanceCache: make(map[uuid.UUID]*MaintainanceCache),
	}
}

func (m *Manager) Close() error {
	m.vfsCacheMutex.Lock()
	defer m.vfsCacheMutex.Unlock()

	for _, cache := range m.repositoryCache {
		cache.Close()
	}

	for _, cache := range m.vfsCache {
		cache.Close()
	}

	// we may rework the interface later to allow for error handling
	// at this point closing is best effort
	return nil
}

func (m *Manager) VFS(scheme string, origin string) (*_VFSCache, error) {
	m.vfsCacheMutex.Lock()
	defer m.vfsCacheMutex.Unlock()

	key := fmt.Sprintf("%s://%s", scheme, origin)

	if cache, ok := m.vfsCache[key]; ok {
		return cache, nil
	}

	if cache, err := newVFSCache(m, scheme, origin); err != nil {
		return nil, err
	} else {
		m.vfsCache[key] = cache
		return cache, nil
	}
}

func (m *Manager) Repository(repositoryID uuid.UUID) (*_RepositoryCache, error) {
	m.repositoryCacheMutex.Lock()
	defer m.repositoryCacheMutex.Unlock()

	if cache, ok := m.repositoryCache[repositoryID]; ok {
		return cache, nil
	}

	if cache, err := newRepositoryCache(m, repositoryID); err != nil {
		return nil, err
	} else {
		m.repositoryCache[repositoryID] = cache
		return cache, nil
	}
}

func (m *Manager) Maintainance(repositoryID uuid.UUID) (*MaintainanceCache, error) {
	m.maintainanceCacheMutex.Lock()
	defer m.maintainanceCacheMutex.Unlock()

	if cache, ok := m.maintainanceCache[repositoryID]; ok {
		return cache, nil
	}

	if cache, err := newMaintainanceCache(m, repositoryID); err != nil {
		return nil, err
	} else {
		m.maintainanceCache[repositoryID] = cache
		return cache, nil
	}
}

// XXX - beware that caller has responsibility to call Close() on the returned cache
func (m *Manager) Scan(snapshotID objects.MAC) (*ScanCache, error) {
	return newScanCache(m, snapshotID)
}
