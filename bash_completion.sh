#!/bin/bash

PLAKAR=./plakar

_plakar_get_global_opts() {
    "$PLAKAR" -h 2>&1 | awk '
      /^OPTIONS:/ {in_opts=1; next}
      /^[A-Z]/ {in_opts=0}
      in_opts && $1 ~ /^-/ {print $1}
    '
}

_plakar_get_commands() {
    "$PLAKAR" -h 2>&1 | awk '
      /^COMMANDS:/ {in_cmds=1; next}
      /^[[:space:]]*$/ {in_cmds=0}
      in_cmds {print $1}
    '
}

_plakar_get_command_opts() {
    local subcmd="$1"
    "$PLAKAR" "$subcmd" -h 2>&1 | awk '
      /^OPTIONS:/ {in_opts=1; next}
      /^[A-Z]/ {in_opts=0}
      in_opts && $1 ~ /^-/ {print $1}
    '
}

_plakar() {
    local cur prev words cword
    # Use _init_completion if available (from bash-completion), else do basic setup.
    if type _init_completion &>/dev/null; then
        _init_completion || return
    else
        cur="${COMP_WORDS[COMP_CWORD]}"
        prev="${COMP_WORDS[COMP_CWORD-1]}"
        cword=$COMP_CWORD
        words=("${COMP_WORDS[@]}")
    fi

    # Fetch global options and command list dynamically.
    local global_opts cmds
    global_opts="$(_plakar_get_global_opts)"
    cmds="$(_plakar_get_commands)"

    # Determine if a subcommand has been specified.
    local subcmd=""
    for ((i = 1; i < ${#words[@]}; i++)); do
        # If the word does not start with a dash and is one of the known commands, use it.
        if [[ "${words[i]}" != -* ]] && grep -qw "${words[i]}" <<< "$cmds"; then
            subcmd="${words[i]}"
            break
        fi
    done

    # If no subcommand is found yet, complete with global options and commands.
    if [[ -z "$subcmd" ]]; then
        COMPREPLY=( $(compgen -W "$global_opts $cmds" -- "$cur") )
        return
    fi

    # For a recognized subcommand, complete command-specific options.
    local sub_opts
    sub_opts="$(_plakar_get_command_opts "$subcmd")"
    COMPREPLY=( $(compgen -W "$sub_opts" -- "$cur") )
}

complete -F _plakar plakar