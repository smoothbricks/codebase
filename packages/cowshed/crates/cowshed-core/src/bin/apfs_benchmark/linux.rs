//! Linux has no APFS disk images to clone or attach: the benchmark refuses by name and measures
//! nothing.

pub fn run() {
    cowshed_os::eprintln!(
        "apfs-benchmark: it times APFS disk images through hdiutil and diskutil, which only macOS has; \
         nothing was measured"
    );
    std::process::exit(1);
}
