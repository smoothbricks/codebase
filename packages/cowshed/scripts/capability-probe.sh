#!/bin/sh
# Writes rustc's verdict on the cowshed-core capability probe (cowshed:capability-probe) to the file named by $1.
#
# crates/cowshed-core/tests/capability_probe/ holds one file per case: code a caller without coordinator
# authority must not be able to compile. The verdict is one JSON object, case name -> the error diagnostics
# rustc gave that case's crate, one short-form line each. cowshed-core's test
# `lesser_capabilities_fail_to_compile_with_coordinator_authority` reads it and judges it; compiling is this
# target's work, so that test spawns no cargo and no bound of a test runner is spent on a dependency build.
#
# This script judges no case: a case that compiles is a verdict too, and the test names it. It refuses a run
# in which rustc gave NO verdict for some case (a dependency that did not check, a lockfile or offline
# resolution refusal, a rustc that was killed): Nx caches what a successful target wrote, and a cached
# non-answer would fail the test on every run until an input changed. cargo's own exit status is not that
# judgement: every case is meant to fail, so cargo exits nonzero when the probe works.
#
# Each case is its own crate, so each is checked in a rustc session of its own: rustc skips its privacy pass
# once type checking failed, so cases sharing a crate would hide each other's later-phase errors. The crates
# are members of ONE workspace checked by ONE cargo with --keep-going: a cargo per case pays resolution and
# the fingerprint walk of cowshed-core's whole tree once per case. The workspace is staged beside the result,
# inside the checkout, so Cargo reads the checkout's own `.cargo/config.toml` files (linker and `[env]`)
# like every other Cargo run here. The staged lockfile is the checkout's own: it pins the probe to the
# versions the rest of the build compiled and that `cargo fetch` already holds, so
# resolution is a lookup and needs no network. Cargo adds the case crates to the staged copy, never to the
# checkout's.
#
# The check shares the checkout's own target directory, the one `cargo metadata` reports. A private one costs a
# full cold check of cowshed-core's whole tree and 927 MB of duplicated artifacts (measured on an idle M5 Max:
# 48 s, ~110 s of CPU); sharing reuses the build scripts and proc macros the test archive already built and
# keeps what one probe check compiled for the next.
set -eu

result=${1:?usage: capability-probe.sh <verdict file>}
cases=crates/cowshed-core/tests/capability_probe
project=$PWD

metadata=$(cargo metadata --no-deps --offline --format-version 1)
workspace=$(printf '%s\n' "$metadata" | jq -r .workspace_root)
target=$(printf '%s\n' "$metadata" | jq -r .target_directory)

mkdir -p "$(dirname "$result")"
# A verdict that exists was published by a run that completed: a refused run leaves none, not the last run's.
rm -f "$result"
staging=$(mktemp -d "$(dirname "$result")/workspace.XXXXXX")
trap 'rm -rf "$staging"' EXIT
trap 'exit 1' HUP INT TERM

names=
members=
for source in "$cases"/*.rs; do
  [ -f "$source" ] || { echo "capability-probe: no case file $source (run it from the package root)" >&2; exit 1; }
  name=$(basename "$source" .rs)
  mkdir -p "$staging/$name/src"
  cp "$source" "$staging/$name/src/lib.rs"
  # `serde` is here because one case names a serde trait to prove a sealed token does not implement it;
  # without it that case would fail on an unresolved crate and prove nothing.
  cat >"$staging/$name/Cargo.toml" <<MANIFEST
[package]
name = "$name"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
cowshed-core = { path = '$project/crates/cowshed-core' }
serde = { version = "1", features = ["derive"] }
MANIFEST
  names="$names $name"
  members="$members\"$name\", "
done
# The probe's own `[workspace]` detaches it from the checkout's workspace, whose members it must not join and
# whose lints and profiles it must not inherit: each case is meant to fail on visibility, and nothing else.
printf '[workspace]\nresolver = "3"\nmembers = [%s]\n' "${members%, }" >"$staging/Cargo.toml"
cp "$workspace/Cargo.lock" "$staging/Cargo.lock"

status=0
(cd "$staging" && cargo check --workspace --keep-going --offline --message-format=json-diagnostic-short --target-dir "$target" >messages.jsonl 2>cargo.log) || status=$?

# A case's verdict is the errors rustc located in its crate, or the artifact it built when there were none.
# rustc's "aborting due to" summary has no span and says nothing the located errors do not.
# shellcheck disable=SC2086 # the case names are crate names: no whitespace or globs
if ! jq -S -s '
  . as $messages
  | $ARGS.positional
  | map(
      . as $case
      | [ $messages[]
          | select(.reason == "compiler-message" and .target.name == $case and .message.level == "error" and (.message.spans | length) > 0)
          | .message.rendered
          | rtrimstr("\n") ] as $errors
      | if ($errors | length) > 0 or any($messages[]; .reason == "compiler-artifact" and .target.name == $case)
        then { ($case): $errors }
        else error("rustc gave no verdict for case \($case)")
        end)
  | add' --args $names <"$staging/messages.jsonl" >"$staging/verdict.json"; then
  cat "$staging/cargo.log" >&2
  echo "capability-probe: cargo check exited $status and gave no verdict for every case in $cases" >&2
  exit 1
fi
mv -f "$staging/verdict.json" "$result"