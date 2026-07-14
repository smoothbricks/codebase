fn main() {
    // Features reach build scripts as env vars, not cfg; napi_build is an
    // optional build-dep enabled by the napi feature.
    if std::env::var_os("CARGO_FEATURE_NAPI").is_some() {
        #[cfg(feature = "napi")]
        napi_build::setup();
    }
}
