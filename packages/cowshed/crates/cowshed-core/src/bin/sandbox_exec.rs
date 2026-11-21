//! `cowshed-sandbox-exec`: the sandbox launcher of a program that links `cowshed-core` and is not
//! the cowshed binary (04_sandbox.md, "Linux enforcement", "The launcher"). It is the launcher
//! and nothing else: one thread, no argument of its own, the command line of a launch.

fn main() {
    std::process::exit(cowshed_core::sandbox::launcher_main());
}
