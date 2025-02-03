PLAKAR-FIND(1) - General Commands Manual

# NAME

**plakar find** - Search for files or directories in Plakar snapshots

# SYNOPSIS

**plakar find**
*pattern&nbsp;...*

# DESCRIPTION

The
**plakar find**
command searches for files or directories across all snapshots in a
Plakar repository that match a given pattern.
It supports searching by full pathname or filename within snapshots
and lists the results chronologically by snapshot creation time.

*pattern*

> One or more search patterns specifying filenames or pathnames to
> search for in the snapshots.
> Patterns can be a full pathname or simply a file or directory name.

# EXAMPLES

Find a file by full pathname:

	plakar find /path/to/file.txt

Find all snapshots containing files or directories named "backup":

	plakar find backup

# DIAGNOSTICS

The **plakar find** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as failure to load snapshots or an invalid
> pattern.

# SEE ALSO

plakar(1)

macOS 15.0 - November 12, 2024
