//! A dedicated warm exec host (see `cowshed_shell`).

fn main() {
    std::process::exit(cowshed_shell::serve());
}
