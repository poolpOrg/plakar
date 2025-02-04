/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package classifier

import (
	"fmt"
	"log"
	"sync"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

const VERSION string = "0.6.0"

type Classification struct {
	Analyzer string
	Classes  []string
}

type Backend interface {
	Processor(backend Backend, pathname string) ProcessorBackend
	Close() error
}

type ProcessorBackend interface {
	Name() string
	File(fileEntry *vfs.Entry) []string
	Directory(dirEntry *vfs.Entry) []string
	Write(buf []byte) bool
	Finalize() []string
}

var muBackends sync.Mutex
var backends map[string]func() Backend = make(map[string]func() Backend)

type Classifier struct {
	backend    []Backend
	appcontext *appcontext.AppContext
}

func NewClassifier(ctx *appcontext.AppContext) (*Classifier, error) {
	muBackends.Lock()
	defer muBackends.Unlock()

	cf := &Classifier{}
	cf.appcontext = ctx

	for name, backend := range backends {
		if inst := backend(); inst == nil {
			return nil, fmt.Errorf("backend '%s' failed to initialize", name)
		} else {
			cf.backend = append(cf.backend, inst)
		}
	}
	return cf, nil
}

func Register(name string, backend func() Backend) {
	muBackends.Lock()
	defer muBackends.Unlock()

	if _, ok := backends[name]; ok {
		log.Fatalf("backend '%s' registered twice", name)
	}
	backends[name] = backend
}

func (cf *Classifier) Close() error {
	for _, backend := range cf.backend {
		_ = backend.Close()
	}
	return nil
}

func (cf *Classifier) Processor(pathname string) *Processor {
	backends := make([]ProcessorBackend, 0)
	for _, backend := range cf.backend {
		backends = append(backends, backend.Processor(backend, pathname))
	}
	return &Processor{
		cf:        cf,
		backends:  backends,
		writeDone: make(map[string]struct{}),
	}
}

type Processor struct {
	cf        *Classifier
	backends  []ProcessorBackend
	writeDone map[string]struct{}
	Pathname  string
}

func (p *Processor) File(fileEntry *vfs.Entry) []Classification {
	ret := []Classification{}
	for _, backend := range p.backends {
		result := Classification{
			Analyzer: backend.Name(),
		}
		classes := backend.File(fileEntry)
		result.Classes = append(result.Classes, classes...)
		if len(result.Classes) > 0 {
			ret = append(ret, result)
		}
	}
	return ret
}

func (p *Processor) Directory(dirEntry *vfs.Entry) []Classification {
	ret := []Classification{}
	for _, backend := range p.backends {
		result := Classification{
			Analyzer: backend.Name(),
		}
		classes := backend.Directory(dirEntry)
		result.Classes = append(result.Classes, classes...)
		if len(result.Classes) > 0 {
			ret = append(ret, result)
		}
	}

	return ret
}

func (p *Processor) Write(buf []byte) {
	for _, backend := range p.backends {
		// if this backend has already returned false, don't call it again
		if _, done := p.writeDone[backend.Name()]; done {
			ok := backend.Write(buf)
			if !ok {
				// if the backend returns false, don't call it again
				p.writeDone[backend.Name()] = struct{}{}
			}
		}
	}
}

func (p *Processor) Finalize() []Classification {
	ret := []Classification{}
	for _, backend := range p.backends {
		result := Classification{
			Analyzer: backend.Name(),
		}
		classes := backend.Finalize()
		result.Classes = append(result.Classes, classes...)
		if len(result.Classes) > 0 {
			ret = append(ret, result)
		}
	}
	return ret
}
