# The rule that approves an .envrc for direnv, sourced by land-host.sh (a workspace's) and land-install.sh (the build
# shed's), each of which sets here to this directory first. direnv trusts an .envrc by its path and its content: the
# operator approved main's, and another tree's is approved here only when it is byte-identical to main's. One that
# differs (a change to the .envrc) runs only when the operator has already approved it by hand, so nothing runs an
# environment nobody approved. direnv reports an approved .envrc as "Found RC allowed 0"; tooling/land-decide.ts holds
# the decision.
#
# allow_envrc <main> <checkout> returns 0 when <checkout>'s .envrc is approved afterwards. It returns 1 when it is not,
# and $envrc_refusal then says why, naming the file. It runs where a failing command must not stop the script, so each
# command that can fail is checked here.
allow_envrc() {
  envrc_same=differs
  envrc_status=
  if cmp -s "$1/.envrc" "$2/.envrc"; then
    envrc_same=same
  else
    envrc_status=$(cd "$2" && direnv status) || envrc_status=
  fi
  envrc_verdict=$(bun "$here/land-decide.ts" envrc "$envrc_same" "$envrc_status") || {
    envrc_refusal="land-decide.ts envrc exited $?, so $2/.envrc is not approved"
    return 1
  }
  case $envrc_verdict in
  allow)
    direnv allow "$2" || {
      envrc_refusal="direnv allow $2 exited $?"
      return 1
    }
    ;;
  approved) ;;
  *)
    envrc_refusal="$2/.envrc differs from main's approved .envrc; review it and run 'direnv allow $2'"
    return 1
    ;;
  esac
}