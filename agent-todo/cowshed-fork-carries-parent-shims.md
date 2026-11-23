# A fork of a shed carries build-script shims that name the parent's mount

Seen 2026-11-23 in a consumer repository: a shed forked from another shed (not from main) kept, in its build volume,
`target/debug/.mbx-launcher-root` and every `target/debug/build/*/out/build_script_build` shim with the shebang
`/…/<parent shed>/target/debug/.mbx-build-script-shims/<hash>/mbx`, a path inside the parent shed's mount. The fork's
sandbox cannot exec it and `cowshed grant` refuses to cover another workspace's mount, so every cargo build script that
reran failed "Operation not permitted", and every Nx target that compiles Rust failed before compiling.

A fork's build volume must hold no path into another workspace: either mbx writes the shim's interpreter path relative
to the volume (or through the launcher root the sandbox maps), or the fork rewrites the shims it carries. Red: fork a
shed whose target holds mbx build-script shims, rerun one build script in the fork's sandbox, and it runs.
