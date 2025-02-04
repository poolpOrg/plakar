PLAKAR-DIFF(1) - General Commands Manual

# NAME

**plakar diff** - Show differences between files in Plakar snapshots

# SYNOPSIS

**plakar diff**
\[**-highlight**]
*snapshotID1*\[:*path1*]
*snapshotID2*\[:*path2*]

# DESCRIPTION

The
**plakar diff**
command compares two Plakar snapshots, optionally restricting to
specific files within them.
If only snapshot IDs are provided, it compares the root directories of
each snapshot.
If file paths are specified, the command compares the individual
files.
The diff output is shown in unified diff format, with an option to
highlight differences.

The options are as follows:

**-highlight**

> Apply syntax highlighting to the diff output for readability.

# EXAMPLES

Compare root directories of two snapshots:

	$ plakar diff abc123 def456

Compare
across snapshots with highlighting:
*/etc/passwd*

	$ plakar diff -highlight abc123:/etc/passwd def456:/etc/passwd

# DIAGNOSTICS

The **plakar diff** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as invalid snapshot IDs, missing files, or an
> unsupported file type.

# SEE ALSO

plakar(1),
plakar-backup(1)

Nixpkgs - February 3, 2025
