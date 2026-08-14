use std::ffi::OsString;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let arguments: Vec<OsString> = std::env::args_os().skip(1).collect();
    std::process::exit(cowshed_cli::run::run(arguments).await);
}
