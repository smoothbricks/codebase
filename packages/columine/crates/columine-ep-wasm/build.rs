fn main() {
    // event_processor.wasm ABI (build.zig ep_wasm): memory is EXPORTED
    // ("Don't import memory - let Zig export it so wasm_allocator works
    // correctly"), initial 48 pages (3 MiB, build.zig columine_wasm), max 4096 pages (256 MiB). Rust's default
    // wasm32-unknown-unknown cdylib exports memory, so only the sizes need
    // pinning here (the workspace deliberately sets no target-wide
    // --import-memory rustflag — memory policy is per artifact).
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.starts_with("wasm32") {
        println!("cargo::rustc-link-arg=--initial-memory=3145728");
        println!("cargo::rustc-link-arg=--max-memory=268435456");
    }
}
