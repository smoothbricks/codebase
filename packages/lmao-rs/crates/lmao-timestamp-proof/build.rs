fn main() {
    // Features reach build scripts as env vars, not cfg; napi_build is an
    // optional build-dep enabled by the napi feature.
    if std::env::var_os("CARGO_FEATURE_NAPI").is_some() {
        #[cfg(feature = "napi")]
        napi_build::setup();
    }

    // The AxE proof artifact deliberately keeps the original shared,
    // host-owned memory ABI. Ordinary workspace WASM builds stay unshared.
    println!("cargo::rerun-if-env-changed=LMAO_TIMESTAMP_PROOF_SHARED_MEMORY");
    let builds_shared_proof = std::env::var_os("LMAO_TIMESTAMP_PROOF_SHARED_MEMORY").is_some();
    if builds_shared_proof
        && std::env::var("TARGET").is_ok_and(|target| target.starts_with("wasm32"))
    {
        println!("cargo::rustc-link-arg=--import-memory");
        println!("cargo::rustc-link-arg=--shared-memory");
        println!("cargo::rustc-link-arg=-zstack-size=65536");
        println!("cargo::rustc-link-arg=--initial-memory=1048576");
        println!("cargo::rustc-link-arg=--max-memory=1048576");
        for export in [
            "init_trace_root",
            "span_start",
            "span_end_ok",
            "span_end_err",
            "write_log_entry",
            "get_performance_now",
            "debug_compute_timestamp",
        ] {
            println!("cargo::rustc-link-arg=--export={export}");
        }
    }
}
