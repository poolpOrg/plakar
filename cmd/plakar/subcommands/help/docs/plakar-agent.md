PLAKAR-AGENT(1) - General Commands Manual

# NAME

**plakar agent** - Run the Plakar agent

# SYNOPSIS

**plakar agent**
\[**-prometheus**&nbsp;*address*]

# DESCRIPTION

The
**plakar agent**
command starts the Plakar agent which will execute subsequent
plakar
commands on their behalfs for faster processing.
**plakar agent**
continues running indefinitely.

The options are as follows:

**-prometheus** *address*

> Expose a prometheus server at
> *address*.
> Metrics are available at the
> */metrics*
> endpoint.

# DIAGNOSTICS

The **plakar agent** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as invalid parameters, inability to create the
> repository, or configuration issues.

# SEE ALSO

plakar(1)

Nixpkgs - February 1, 2025
