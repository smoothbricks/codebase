//! A trusted N-API embedder (07_api.md, "cowshed-napi"): Node runs one ES module against the
//! built TypeScript package (`nx run cowshed:tsc-js`) and the host's Node-API addon
//! (`nx run cowshed:napi-native`), on a coordinator connection it inherits as descriptor 3, the
//! way a trusted spawner hands an embedder its controller endpoint.

use std::os::fd::OwnedFd;
use std::path::Path;

use cowshed_os::fork_lock::Spawn as _;
use cowshed_os::test_backstop::ChildBackstop as _;

/// What `body` returns, decoded into the same canonical Rust type the controller returns.
///
/// `body` is the body of an async function run with `coordinator`, the embedder's coordinator for
/// the project at `checkout`, connected through `endpoint`. Node must exit 0; any other end fails
/// the test with both of its streams.
pub async fn answer<T: serde::de::DeserializeOwned>(
    endpoint: OwnedFd,
    checkout: &Path,
    body: &str,
) -> T {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    // Every crate of the package is two directories below it.
    let package = Path::new(&manifest).join("../..");
    let module = url::Url::from_file_path(package.join("dist/ts/index.js").canonicalize().expect(
        "the built TypeScript package (nx run cowshed:tsc-js) under packages/cowshed/dist/ts",
    ))
    .expect("a module URL");
    let client = format!(
        "import {{ connectCoordinator, coordinatorEndpoint }} from {module};\n\
         const coordinator = await connectCoordinator(coordinatorEndpoint(3), {checkout});\n\
         const document = JSON.stringify(await (async () => {{\n{body}\n}})());\n\
         await new Promise((resolve, reject) => process.stdout.write(document + '\\n', error => error ? reject(error) : resolve()));\n\
         process.exit(0);\n",
        module = serde_json::json!(module.as_str()),
        checkout = serde_json::json!(checkout),
    );
    let node = std::env::var_os("PATH")
        .and_then(|path| {
            std::env::split_paths(&path)
                .map(|directory| directory.join("node"))
                .find(|node| node.is_file())
        })
        .expect("node on the test's PATH");
    let output = tokio::task::spawn_blocking(move || {
        // The endpoint arrives as stdin; the shell hands it to Node as descriptor 3.
        std::process::Command::new("/bin/sh")
            .args([
                "-c",
                r#"exec "$0" --input-type=module --eval "$1" 3<&0 </dev/null"#,
            ])
            .arg(node)
            .arg(client)
            .env_remove("NAPI_DEBUG_ADDON")
            .env_remove("FORCE_COLOR")
            .current_dir(&package)
            .stdin(std::process::Stdio::from(endpoint))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn_locked()?
            .wait_with_output_bounded()
    })
    .await
    .expect("node task")
    .expect("run node");
    assert!(
        output.status.success(),
        "node: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("the SDK answers the canonical value")
}
