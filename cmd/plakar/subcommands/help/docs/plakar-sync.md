PLAKAR-SYNC(1) - General Commands Manual

# NAME

**plakar sync** - Synchronize snapshots between Plakar repositories

# SYNOPSIS

**plakar sync**
\[*snapshotID*]
**to**&nbsp;|&nbsp;**from**&nbsp;|&nbsp;**with**
*repository*

# DESCRIPTION

The
**plakar sync**
command syncronize snapshots between two Plakar repositories.
If a specific snapshot ID is provided, only snapshots with matching
IDs will be synchronized.

The arguments are as follows:

**to** | **from** | **with**

> Specifies the direction of synchronization:

> **to**

> > Synchronize snapshots from the local repository to the specified peer
> > repository.

> **from**

> > Synchronize snapshots from the specified peer repository to the local
> > repository.

> **with**

> > Synchronize snapshots in both directions, ensuring both repositories
> > are fully synchronized.

*repository*

> Path to the peer repository to synchronize with.

# EXAMPLES

Bi-directional synchronization with peer repository:

	$ plakar sync with /path/to/peer/repo

# DIAGNOSTICS

The **plakar sync** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> General failure occurred, such as an invalid repository path, snapshot
> ID mismatch, or network error.

# SEE ALSO

plakar(1)

Nixpkgs - February 1, 2025
