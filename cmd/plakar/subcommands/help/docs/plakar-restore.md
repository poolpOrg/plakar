PLAKAR-RESTORE(1) - General Commands Manual

# NAME

**plakar restore** - Restore files from a Plakar snapshot

# SYNOPSIS

**plakar restore**
\[**-concurrency**&nbsp;*number*]
\[**-quiet**]
\[**-rebase**]
\[**-to**&nbsp;*directory*]
\[*snapshotID*:*path&nbsp;...*]

# DESCRIPTION

The
**plakar restore**
command is used to restore files and directories at
*path*
from a specified Plakar snapshot to the local file system.
If
*path*
is omitted, then all the files in the specified
*snapshotID*
are restored.
If no
*snapshotID*
is provided, the command attempts to restore the current working
directory from the last matching snapshot.

The options are as follows:

**-concurrency** *number*

> Set the maximum number of parallel tasks for faster
> processing.
> Defaults to
> `8 * CPU count + 1`.

**-to** *directory*

> Specify the base directory to which the files will be restored.
> If omitted, files are restored to the current working directory.

**-rebase**

> Strip the original path from each restored file, placing files
> directly in the specified directory (or the current working directory
> if
> **-to**
> is omitted).

**-quiet**

> Suppress output to standard input, only logging errors and warnings.

# EXAMPLES

Restore all files from a specific snapshot to the current directory:

	plakar restore abc123

Restore to a specific directory:

	plakar restore -to /path/to/restore abc123

Restore with rebase option, placing files directly in the target directory:

	plakar restore -rebase -to /path/to/restore abc123

# DIAGNOSTICS

The **plakar restore** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as a failure to locate the snapshot or a
> destination directory issue.

# SEE ALSO

plakar(1),
plakar-backup(1)

Nixpkgs - January 29, 2025
