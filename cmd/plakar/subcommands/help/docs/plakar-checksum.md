PLAKAR-CHECKSUM(1) - General Commands Manual

# NAME

**plakar checksum** - Calculate checksums for files in a Plakar snapshot

# SYNOPSIS

**plakar checksum**
\[**-fast**]
*snapshotID*:*filepath*&nbsp;\[...]

# DESCRIPTION

The
**plakar checksum**
command calculates and displays checksums for specified
*filepath*
in a the given
*snapshotID*.
Multiple
*snapshotID*
and
*filepath*
may be given.
By default, the command computes the checksum by reading the file
contents.

The options are as follows:

**-fast**

> Return the pre-recorded checksum for the file without re-computing it
> from the file contents.
> It's faster, but it does not verify the integrity against the current
> contents.

# EXAMPLES

Calculate the checksum of a file within a snapshot:

	plakar checksum abc123:/path/to/file.txt

Retrieve the pre-recorded checksum for faster output:

	plakar checksum -fast abc123:/path/to/file.txt

# DIAGNOSTICS

The **plakar checksum** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as failure to retrieve a file checksum or
> invalid snapshot ID.

# SEE ALSO

plakar(1)

Nixpkgs - January 28, 2025
