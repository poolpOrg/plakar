PLAKAR(ID) - ID (1)

# NAME

**plakar id** - manage identities for Plakar repositories

# SYNOPSIS

**plakar id**
\[**-&lt;option&gt;**]
\[*subcommand*]
\[*arguments*]

# DESCRIPTION

The
**plakar id**
command is used to manage identities in the Plakar system. Identities are associated with email addresses, passphrases, and cryptographic key pairs to enable secure operations in repositories.

This command provides subcommands to create new identities, retrieve information about an identity, and list all identities available in the keyring directory.

**-create**

> Creates a new identity by prompting the user for an email address and passphrase. Generates a key pair for secure operations.

**-info**

> Retrieves and displays information about a specific identity using its identifier.

**-list**, **-ls**

> Lists all available identities in the keyring directory, displaying their metadata.

# ARGUMENTS

*subcommand*

> Specifies the action to perform. Available subcommands are:

> create

> > Prompts the user to create a new identity.

> info

> > Displays information about a specific identity. Requires an identity identifier as an argument.

> list, ls

> > Lists all stored identities.

*arguments*

> Additional arguments specific to the chosen subcommand. For example, the identifier of an identity for the
> **info**
> subcommand.

# EXAMPLES

To illustrate usage:

Create a new identity:

> > plakar id create

Display information about a specific identity:

> > plakar id info &lt;identity\_id&gt;

List all identities:

> > plakar id list

# DIAGNOSTICS

The **plakar id** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> General failure occurred. Detailed error messages are displayed when failures occur.

# SEE ALSO

plakar(1)

macOS 15.0 - November 27, 2024
