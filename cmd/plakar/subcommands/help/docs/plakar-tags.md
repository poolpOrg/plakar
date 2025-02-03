PLAKAR(TAGS) - TAGS (1)

# NAME

**plakar\_tags** - Display tags or related information in a Plakar repository

# SYNOPSIS

**plakar\_tags**
\[**-display**]
\[*tags|count|snapshots*]

# DESCRIPTION

The
**plakar\_tags**
command is used to display information about tags in a Plakar repository, including a list of tags, a count of their occurrences, or associated snapshots.

**-display**

> Specifies the type of output for the tag display. Valid options are:

> tags

> > Lists all tags (default).

> count

> > Displays each tag with a count of associated snapshots.

> snapshots

> > Shows each tag with a list of associated snapshot IDs.

# ARGUMENTS

*tags|count|snapshots*

> This argument, when used with the
> **-display**
> flag, controls the output type:

> tags

> > Outputs all tags in alphabetical order.

> count

> > Displays tags with a count of associated snapshots.

> snapshots

> > Lists each tag with associated snapshot IDs.

# EXAMPLES

To illustrate usage:

Basic example:

> > plakar tags -display tags

Example showing tag counts:

> > plakar tags -display count

Example with tag snapshots:

> > plakar tags -display snapshots

# DIAGNOSTICS

The **plakar\_tags** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

1

> An unsupported display option was specified.

# SEE ALSO

plakar(1)

macOS 15.0 - November 13, 2024
