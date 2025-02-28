/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
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

package sftp

import (
	"os"

	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/pkg/xattr"
)

func getExtendedAttributes(path string) ([]importer.ExtendedAttributes, error) {

	attrs := make([]importer.ExtendedAttributes, 0)

	// Get the list of attribute names
	attributes, err := xattr.List(path)
	if err != nil {
		return nil, err
	}

	// Iterate over each attribute and retrieve its value
	for _, attr := range attributes {
		value, err := xattr.Get(path, attr)
		if err != nil {
			// Log the error and continue instead of failing
			if os.IsPermission(err) {
				continue
			}
			return nil, err
		}
		attrs = append(attrs, importer.ExtendedAttributes{Name: attr, Value: value})
	}

	return attrs, nil
}
