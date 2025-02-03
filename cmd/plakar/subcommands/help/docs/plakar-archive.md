PLAKAR-ARCHIVE(1) - General Commands Manual

# NAME

**plakar archive** - Create an archive from a Plakar snapshot

# SYNOPSIS

**plakar archive**
\[**-format**&nbsp;*type*]
\[**-output**&nbsp;*archive*]
\[**-rebase**]
*snapshotID*:*path*

# DESCRIPTION

The
**plakar archive**
command creates an
*archive*
of the given
*type*
from the contents at
*path*
of a specified Plakar snapshot, or all the files if no
*path*
is given.

The options are as follows:

**-format** *type*

> Specify the archive format.
> Supported formats are:

> **tar**

> > Creates a tar file.

> **tarball**

> > Creates a compressed tar.gz file.

> **zip**

> > Creates a zip archive.

**-output** *pathname*

> Specify the output path for the archive file.
> If omitted, the archive is created with a default name based on the
> current date and time.

**-rebase**

> Strip the leading path from archived files, useful for creating "flat"
> archives without nested directories.

# EXAMPLES

Create a tarball of the entire snapshot:

	plakar archive -output backup.tar.gz -format tarball abc123

Create a zip archive of a specific directory within a snapshot:

	plakar archive -output dir.zip -format zip abc123:/var/www

Archive with rebasing to remove directory structure:

	plakar archive -rebase -format tar abc123

# DIAGNOSTICS

The **plakar archive** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as unsupported format, missing files, or
> permission issues.

# SEE ALSO

plakar(1),
plakar-backup(1)

Nixpkgs - January 29, 2025
