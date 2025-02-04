PLAKAR-CREATE(1) - General Commands Manual

# NAME

**plakar create** - Create a new Plakar repository

# SYNOPSIS

**plakar create**
\[**-no-encryption**]
\[**-no-compression**]
\[*path*]

# DESCRIPTION

The
**plakar create**
command creates a new Plakar repository at the specified
*path*,
which defaults to
*~/.plakar*.

The options are as follows:

**-no-encryption**

> Disable transparent encryption for the repository.
> If specified, the repository will not use encryption.

**-no-compression**

> Disable transparent compression for the repository.
> If specified, the repository will not use compression.

# ENVIRONMENT

`PLAKAR_PASSPHRASE`

> Repository encryption password.

# DIAGNOSTICS

The **plakar create** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as invalid parameters, inability to create the
> repository, or configuration issues.

# SEE ALSO

plakar(1),
plakar-backup(1)

Nixpkgs - February 3, 2025
