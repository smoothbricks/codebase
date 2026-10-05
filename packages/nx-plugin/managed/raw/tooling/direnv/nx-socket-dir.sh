# Managed by `smoo monorepo`. Sourced by devenv.smoo.nix's enterShell prologue
# from the workspace root, naming the directory that holds cowshed's short
# runtime links:
#
#   . "$DEVENV_ROOT/nx-socket-dir.sh" /tmp
#
# Exports NX_WORKSPACE_ROOT_PATH and NX_SOCKET_DIR for this Nx workspace.
#
# One socket dir per Nx workspace. DEVENV_RUNTIME is keyed to the devenv ROOT,
# so every workspace sharing one devenv - a sibling repository, a
# copy-on-write clone, a scratch workspace created inside this shell - would
# land on one socket. The first daemon to claim it then refuses every message
# from the others ("received a message from a different workspace"), which
# reads as a hung Nx in a workspace that did nothing wrong.
#
# In a cowshed checkout, host shells, sandboxed jobs and land checks share the
# checkout's one Nx daemon record (cowshed specs/cowshed/04_sandbox.md), and
# the daemon that record names may run inside the sandbox. Every one of them
# must name the same socket dir, as the same literal path:
# - Nx's daemon adopts the environment of each client that connects, so its
#   plugin workers bind their sockets under the client's NX_SOCKET_DIR.
# - The sandbox admits Unix sockets by literal path. It grants the `nx` leaf of
#   <checkout>/.cowshed/run under the name cowshed hands its jobs,
#   <links>/cs-<port base>/nx, and refuses the same leaf reached through any
#   other link: the worker's socket gets EPERM and its plugin fails to load.
# So a host shell takes cowshed's own short link. A checkout path is too long
# for the plugin workers' sockets ("exceeds the maximum socket length"), and
# Nx refuses a symlinked leaf, so only the parent is a link. The port base
# comes from the checkout's .cowshed/env, which cowshed rewrites from the
# workspace's record at every supervisor start; a COWSHED_PORT_BASE in the
# caller's environment may be another workspace's. A host shell creates the
# link when cowshed has not yet, exactly as cowshed would. A link that leads
# elsewhere is reported and left alone.
#
# Without that link, a socket dir that already resolves inside this checkout
# is cowshed's binding for the job and stays. An inherited one that resolves
# elsewhere belongs to another workspace, so the workspace takes its own.
smoo_nx_links="$1"
export NX_WORKSPACE_ROOT_PATH="$PWD"
smoo_nx_checkout="$(pwd -P)"
smoo_nx_socket=""
smoo_nx_port=""
if [ -d .cowshed/run ] && [ -f .cowshed/env ]; then
  smoo_nx_port="$(sed -n 's/^export COWSHED_PORT_BASE=\([0-9][0-9]*\)$/\1/p' .cowshed/env)"
fi
if [ -n "$smoo_nx_port" ]; then
  smoo_nx_link="$smoo_nx_links/cs-$smoo_nx_port"
  smoo_nx_run="$(cd "$smoo_nx_checkout/.cowshed/run" && pwd -P)"
  if [ ! -e "$smoo_nx_link" ] && [ ! -L "$smoo_nx_link" ]; then
    ln -s "$smoo_nx_run" "$smoo_nx_link"
  fi
  if [ "$(cd "$smoo_nx_link" >/dev/null 2>&1 && pwd -P)" = "$smoo_nx_run" ]; then
    smoo_nx_socket="$smoo_nx_link/nx"
  else
    echo "devenv: $smoo_nx_link does not lead to this checkout's $smoo_nx_run; Nx here cannot reach the workspace's sandboxed daemon" >&2
  fi
  unset smoo_nx_link smoo_nx_run
fi
if [ -z "$smoo_nx_socket" ]; then
  case "$(cd "${NX_SOCKET_DIR:-/nonexistent}" >/dev/null 2>&1 && pwd -P)/" in
    "$smoo_nx_checkout"/*) smoo_nx_socket="$NX_SOCKET_DIR" ;;
    *) smoo_nx_socket="$DEVENV_RUNTIME/nx-$(printf '%s' "$PWD" | cksum | cut -d' ' -f1)" ;;
  esac
fi
export NX_SOCKET_DIR="$smoo_nx_socket"
mkdir -p "$NX_SOCKET_DIR"
unset smoo_nx_links smoo_nx_checkout smoo_nx_socket smoo_nx_port
