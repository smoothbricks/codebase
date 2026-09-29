//! A dedicated warm exec host (see `cowshed_core::runtime::shell_host`).

fn main() {
    std::process::exit(cowshed_core::runtime::shell_host::serve());
}
