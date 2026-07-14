fn main() {
    // reducer_vm.wasm ABI (build.zig vm_wasm): memory is EXPORTED (JS reads it from
    // instance.exports.memory), initial 64 pages (4 MiB), max 4096 pages (256 MiB).
    // Layout contract shared with the Zig artifact and wasm-backend.ts / wasm-loader.ts:
    // stack [0, 1 MiB) with JS state at 64 KiB inside its lower band, module data/BSS
    // from 1 MiB, JS input/output regions from 8 MiB (MIN_INPUT_REGION_OFFSET). The
    // Rust heap (dlmalloc from __heap_base, ~1.1 MiB) must stay below 8 MiB — heap
    // growth past that is a documented cutover hazard, same class as deep Zig stacks.
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.starts_with("wasm32") {
        println!("cargo::rustc-link-arg=--initial-memory=4194304");
        println!("cargo::rustc-link-arg=--max-memory=268435456");
    }
}
