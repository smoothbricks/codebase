const std = @import("std");

pub fn build(b: *std.Build) void {
    const optimize = b.standardOptimizeOption(.{});

    // ==========================================================================
    // allocator WASM - Freelist allocator per spec 01q
    // Uses regular (non-shared) memory that can grow dynamically
    // ==========================================================================
    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
    });

    const wasm = b.addExecutable(.{
        .name = "allocator",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lib/wasm/allocator.zig"),
            .target = wasm_target,
            .optimize = if (optimize == .Debug) .Debug else .ReleaseSmall,
        }),
    });
    wasm.entry = .disabled; // No _start, just exports
    wasm.rdynamic = true; // Export all pub functions
    wasm.import_memory = true; // JS provides memory (can grow)
    // Note: NO shared_memory - we want growable ArrayBuffer

    b.installArtifact(wasm);

    // Copy to dist/allocator-zig.wasm. The shipped default (dist/allocator.wasm)
    // is built from Rust (packages/lmao-rs, `nx run lmao-rs:cargo-wasm`); this
    // Zig artifact is an opt-in reference build loaded via LMAO_WASM_ALLOCATOR=zig.
    // The step is deliberately NOT named "wasm": the nx plugin turns build.zig
    // steps into `zig-<step>` targets, and a `*-wasm` name would put Zig back
    // into the default `build` dependency chain.
    const copy_wasm = b.addInstallFileWithDir(
        wasm.getEmittedBin(),
        .{ .custom = "../dist" },
        "allocator-zig.wasm",
    );
    b.getInstallStep().dependOn(&copy_wasm.step);

    const wasm_step = b.step("zig-allocator", "Build reference Zig allocator WASM (opt-in)");
    wasm_step.dependOn(&copy_wasm.step);
}
