const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ==========================================================================
    // Dependencies - zig-msgpack
    // ==========================================================================

    // zig-msgpack for serializing data in the reducer pipeline
    const msgpack_dep = b.dependency("zig_msgpack", .{
        .target = target,
        .optimize = optimize,
    });

    // WASM target for msgpack dependency
    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
    });
    const msgpack_wasm_dep = b.dependency("zig_msgpack", .{
        .target = wasm_target,
        .optimize = .ReleaseSmall,
    });

    // ==========================================================================
    // Columine WASM - Reducer-only binary (no RETE)
    // ==========================================================================
    // This is the standalone columine binary for users who only need
    // Parse + Reduce + Compact + Undo without RETE rule engine.
    const columine_wasm = b.addExecutable(.{
        .name = "columine",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/vm/vm.zig"),
            .target = wasm_target,
            .optimize = if (optimize == .Debug) .Debug else .ReleaseSmall,
        }),
    });
    columine_wasm.entry = .disabled;
    columine_wasm.rdynamic = true;
    // Let Zig export memory so wasm_allocator works correctly
    columine_wasm.export_memory = true;
    // 8MB initial (128 pages of 64KB), 64MB max (1024 pages)
    columine_wasm.initial_memory = 128 * 64 * 1024;
    columine_wasm.max_memory = 1024 * 64 * 1024;

    // msgpack available for WASM (future Parse/Compact stages)
    columine_wasm.root_module.addImport("msgpack", msgpack_wasm_dep.module("msgpack"));

    b.installArtifact(columine_wasm);

    const copy_wasm = b.addInstallFileWithDir(
        columine_wasm.getEmittedBin(),
        .{ .custom = "../dist" },
        "columine.wasm",
    );
    b.getInstallStep().dependOn(&copy_wasm.step);

    // ==========================================================================
    // Columine FFI dylib - Reducer-only native library for Bun FFI
    // ==========================================================================
    const columine_ffi = b.addLibrary(.{
        .name = "columine",
        .linkage = .dynamic,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/vm/vm.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // msgpack available for native FFI (future Parse/Compact stages)
    columine_ffi.root_module.addImport("msgpack", msgpack_dep.module("msgpack"));

    b.installArtifact(columine_ffi);

    const copy_ffi = b.addInstallFileWithDir(
        columine_ffi.getEmittedBin(),
        .{ .custom = "../dist" },
        "libcolumine.dylib",
    );
    b.getInstallStep().dependOn(&copy_ffi.step);

    // ==========================================================================
    // Test step - runs VM tests on native target
    // ==========================================================================
    const test_step = b.step("test", "Run unit tests");

    const vm_test = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/vm/vm.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_vm_test = b.addRunArtifact(vm_test);
    test_step.dependOn(&run_vm_test.step);
}
