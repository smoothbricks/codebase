# Managed by `smoo monorepo`. Sourced by devenv.smoo.nix's enterShell prologue
# from the workspace root, naming the machine's shared caches root:
#
#   . "$DEVENV_ROOT/shared-caches.sh" /private/cowshed/caches
#
# Where the Go and ttsc caches live. Every checkout on a cowshed host — the
# host's own and every sandboxed workspace — reaches them through the one
# literal path under the caches root. Keying the choice on $HOME split them:
# a sandbox's HOME is private to the workspace, so each sandbox fell back to a
# cold per-checkout cache while the host filled another one. Both Go caches
# are content-addressed, so sharing them needs no patch — unlike Rust, Go keys
# on content and flags rather than on where the files live. Without a caches
# root, ttsc stays in-tree for the CI cache action while Go keeps its normal
# per-user defaults. An inherited value always wins so CI can place any of
# these caches itself. GOFLAGS carries -trimpath because it is Go's stable way
# to keep absolute build paths out of the artifact, and unlike Rust's
# trim-paths it costs no cache reuse.
#
# ttsc's plugin builds use the Go it bundles rather than ours, so the
# repository's own Go work and ttsc's plugin builds are two toolchains by
# construction. Placing both caches in the shared root is what keeps that from
# costing a rebuild per workspace.
#
# TTSC_GO_CACHE_DIR and GOCACHE stay separate for ownership, not correctness:
# both caches are content-addressed, so one directory would compile the same
# bytes to the same entries. What sharing loses is an owner. ttsc reclaims a Go
# build cache only when it resolved that directory itself — `ttsc clean` takes
# the cache whose source is TTSC_GO_CACHE_DIR and never one whose source is a
# user GOCACHE, which it treats as someone else's property. Pointing ttsc at
# GOCACHE therefore produced a directory ttsc filled and no repository verb
# could empty, measured at 35G. A dedicated directory gives ttsc's half back to
# `ttsc clean` and leaves GOCACHE holding only Go work this repository does
# itself.
#
# That buys ownership, not automatic GC: ttsc prunes opportunistically only
# when it owns the whole cache root, which requires TTSC_CACHE_DIR unset, and a
# pinned shared root is the point of this file. The trade is deliberate — one
# warm cache every workspace shares, reclaimed by an explicit verb, over
# per-workspace caches that self-trim. Go's own build cache trimming belongs to
# the go command and still applies to both.
smoo_caches_root="$1"
if [ -d "$smoo_caches_root" ]; then
  export TTSC_CACHE_DIR="${TTSC_CACHE_DIR:-$smoo_caches_root/ttsc}"
  export GOCACHE="${GOCACHE:-$smoo_caches_root/go/build}"
  export GOMODCACHE="${GOMODCACHE:-$smoo_caches_root/go/mod}"
  mkdir -p "$GOCACHE" "$GOMODCACHE"
else
  export TTSC_CACHE_DIR="${TTSC_CACHE_DIR:-$PWD/.cache/ttsc}"
fi
export TTSC_GO_CACHE_DIR="${TTSC_GO_CACHE_DIR:-$TTSC_CACHE_DIR/go-build}"
mkdir -p "$TTSC_CACHE_DIR" "$TTSC_GO_CACHE_DIR"
export GOFLAGS="${GOFLAGS:--trimpath}"
unset smoo_caches_root
