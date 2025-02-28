#!/bin/sh

mandoc -I os=Plakar -T markdown ../plakar.1 > "help/docs/plakar.md"
find . -type f -iname \*.1 -exec sh -c '
	for file; do
		base=$(basename "$file" .1)
		mandoc -I os=Plakar -T markdown "$file" > "help/docs/$base.md"
	done
' sh {} +
