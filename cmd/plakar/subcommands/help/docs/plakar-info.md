PLAKAR-INFO(1) - General Commands Manual

# NAME

**plakar info** - Display detailed information about a Plakar repository, snapshot, or other objects

# SYNOPSIS

**plakar info**
\[**errors**&nbsp;|&nbsp;**object**&nbsp;|&nbsp;**packfile**&nbsp;|&nbsp;**snapshot**&nbsp;|&nbsp;**state**&nbsp;|&nbsp;**vfs**]

# DESCRIPTION

The
**plakar info**
command provides detailed information about a Plakar repository,
snapshots, objects, and various internal data structures.
The type of information displayed depends on the specified argument.
Without any arguents, display information about the repository.

The sub-commands are as follows:

**errors** *snapshotID*

> Display the list of errors in the given snapshot.

**object** *objectID*

> Display information about a specific object, including its checksum,
> type, tags, and associated data chunks.

**packfile** *packfileID*

> Show details of packfiles, including entries and checksums, which
> store object data within the repository.

**snapshot** *snapshotID*

> Show detailed information about a specific snapshot, including its
> metadata, directory and file count, and size.

**state**

> List or describe the states in the repository.

**vfs** *snapshotID*:*path*

> Show filesystem (VFS) details for a specific path within a snapshot,
> listing directory or file attributes, including permissions,
> ownership, and custom metadata.

# EXAMPLES

Show repository information:

	plakar info

Show detailed information for a snapshot:

	plakar info snapshot abc123

List all states in the repository:

	plakar info state

Display a specific object within a snapshot:

	plakar info object 1234567890abcdef

Display filesystem details for a path within a snapshot:

	plakar info vfs abc123:/etc/passwd

# DIAGNOSTICS

The **plakar info** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as an invalid snapshot or object ID, or a
> failure to retrieve the requested data.

# SEE ALSO

plakar(1),
plakar-snapshot(1)

Nixpkgs - February 1, 2025
