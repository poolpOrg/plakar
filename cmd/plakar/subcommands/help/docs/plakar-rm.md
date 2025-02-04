PLAKAR-RM(1) - General Commands Manual

# NAME

**plakar rm** - Remove snapshots from the Plakar repository

# SYNOPSIS

**plakar rm**
\[**-older**&nbsp;*date*]
\[**-tag**&nbsp;*tag*]
\[*snapshotID&nbsp;...*]

# DESCRIPTION

The
**plakar rm**
command deletes snapshots from a Plakar repository.
Snapshots can be filtered for deletion by age, by tag, or by
specifying the snapshot IDs to remove.
If no
*snapshotID*
are provided, either
**-older**
or
**-tag**
must be specified to filter the snapshots to delete.

The arguments are as follows:

**-older** *date*

> Remove snapshots older than the specified date.
> Accepted formats include relative durations
> (e.g. 2d for two days, 1w for one week)
> or specific dates in various formats
> (e.g. 2006-01-02 15:04:05).

**-tag** *tag*

> Filter snapshots by tag, deleting only those that contain the specified tag.

# EXAMPLES

Remove a specific snapshot by ID:

	$ plakar rm abc123

Remove snapshots older than 30 days:

	$ plakar rm -older 30d

Remove snapshots with a specific tag:

	$ plakar rm -tag daily-backup

Remove snapshots older than 1 year with a specific tag:

	$ plakar rm -older 1y -tag daily-backup

# DIAGNOSTICS

The **plakar rm** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as invalid date format or failure to delete a
> snapshot.

# SEE ALSO

plakar(1),
plakar-backup(1)

Nixpkgs - February 3, 2025
