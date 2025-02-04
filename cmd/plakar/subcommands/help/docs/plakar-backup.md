PLAKAR-BACKUP(1) - General Commands Manual

# NAME

**plakar backup** - Create a new snapshot of a directory in a Plakar repository

# SYNOPSIS

**plakar backup**
\[**-concurrency**&nbsp;*number*]
\[**-exclude**&nbsp;*pattern*]
\[**-excludes**&nbsp;*file*]
\[**-quiet**]
\[**-stdio**]
\[**-tag**&nbsp;*tag*]
\[*directory*]

# DESCRIPTION

The
**plakar backup**
command creates a new snapshot of
*directory*,
or the current directory,
in a Plakar repository.
Snapshots can be filtered to exclude specific files or directories
based on patterns provided through options.

The options are as follows:

**-concurrency** *number*

> Set the maximum number of parallel tasks for faster processing.
> Defaults to
> `8 * CPU count + 1`.

**-exclude** *pattern*

> Specify individual exclusion patterns to ignore files or directories
> in the backup.
> This option can be repeated.

**-excludes** *file*

> Specify a file containing exclusion patterns, one per line, to ignore
> files or directories in the backup.

**-quiet**

> Suppress output to standard input, only logging errors and warnings.

**-stdio**

> Output one line per file to standard output instead of the default
> interactive output.

**-tag** *tag*

> Specify a tag to assign to the snapshot for easier identification.

# EXAMPLES

Create a snapshot of the current directory with a tag:

	$ plakar backup -tag daily-backup

Backup a specific directory with exclusion patterns from a file:

	$ plakar backup -excludes ~/my-excludes-file /var/www

Backup a directory with specific file exclusions:

	$ plakar backup -exclude "*.tmp" -exclude "*.log" /var/www

# DIAGNOSTICS

The **plakar backup** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully, snapshot created.

&gt;0

> An error occurred, such as failure to access the repository or issues
> with exclusion patterns.

# SEE ALSO

plakar(1)

Nixpkgs - February 3, 2025
