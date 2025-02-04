PLAKAR-CAT(1) - General Commands Manual

# NAME

**plakar cat** - Display the contents of a file from a Plakar snapshot

# SYNOPSIS

**plakar cat**
\[**-no-decompress**]
\[**-highlight**]
*snapshotID*:*filepath&nbsp;...*

# DESCRIPTION

The
**plakar cat**
command outputs the contents of
*filepath*
within Plakar snapshots to the
standard output.
It can decompress compressed files and optionally apply syntax
highlighting based on the file type.

The options are as follows:

**-no-decompress**

> Display the file content as-is, without attempting to decompress it,
> even if it is compressed.

**-highlight**

> Apply syntax highlighting to the output based on the file type.

# EXAMPLES

Display a file's contents from a snapshot:

	$ plakar cat abc123:/etc/passwd

Display a file with syntax highlighting:

	$ plakar cat -highlight abc123:/home/op/korpus/driver.sh

# DIAGNOSTICS

The **plakar cat** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as failure to retrieve a file or decompress
> content.

# SEE ALSO

plakar(1),
plakar-backup(1)

Nixpkgs - February 3, 2025
