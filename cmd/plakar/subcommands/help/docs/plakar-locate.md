PLAKAR-LOCATE(1) - General Commands Manual

# NAME

**plakar locate** - Find filenames in a Plakar snapshot

# SYNOPSIS

**plakar locate**
\[**-snapshot**&nbsp;*snapshotID*]
*patterns&nbsp;...*

# DESCRIPTION

The
**plakar locate**
command search all the snapshots to find file names matching any of
the given
*patterns*
and prints the abbreviated snapshot ID and the full path of the
matched files.
Matching works according to the shell globbing rules.

The options are as follows:

**-snapshot** *snapshotID*

> Limit the search to the given snapshot.

# EXAMPLES

Search for files ending in
"wd":

	$ plakar locate '*wd'
	abc123:/etc/master.passwd
	abc123:/etc/passwd

# DIAGNOSTICS

The **plakar locate** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as invalid parameters, inability to create the
> repository, or configuration issues.

# SEE ALSO

plakar(1),
plakar-backup(1)

# CAVEATS

The patterns may have to be quote to avoid the shell attempting to
expand them.

Nixpkgs - February 1, 2025
