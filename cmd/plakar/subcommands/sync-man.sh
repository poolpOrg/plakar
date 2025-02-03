#!/bin/sh

find . -type f -iname \*.1 -exec sh -c '
	for file; do
		base=$(basename "$file" .1)
		mandoc -T markdown "$file" > "help/docs/$base.md"
	done
' sh {} +
