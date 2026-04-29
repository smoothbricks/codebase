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

    // Copy to dist/allocator.wasm
    const copy_wasm = b.addInstallFileWithDir(
        wasm.getEmittedBin(),
        .{ .custom = "../dist" },
        "allocator.wasm",
    );
    b.getInstallStep().dependOn(&copy_wasm.step);

    const wasm_step = b.step("wasm", "Build allocator WASM");
    wasm_step.dependOn(&copy_wasm.step);
}
