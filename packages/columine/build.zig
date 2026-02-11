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
    // and get the reducer VM's public API (vm.zig).
    // ==========================================================================
    _ = b.addModule("columine", .{
        .root_source_file = b.path("src/vm/vm.zig"),
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
    // 4MB initial (linker needs ~3.5MB for static data)
    columine_wasm.initial_memory = 64 * 64 * 1024;
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
    // EventProcessor WASM - Parse + Compact pipeline (no dedup)
    // ==========================================================================
    // Standalone event processor for JSON-to-Arrow-IPC conversion.
    const ep_wasm = b.addExecutable(.{
        .name = "event_processor",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/event_processor.zig"),
            .target = wasm_target,
            .optimize = if (optimize == .Debug) .Debug else .ReleaseSmall,
        }),
    });
    ep_wasm.entry = .disabled;
    ep_wasm.rdynamic = true;
    ep_wasm.export_memory = true;
    // 160 pages (10MB) — event_processor needs ~9.5MB for msgpack + schema
    ep_wasm.initial_memory = 160 * 64 * 1024;
    ep_wasm.max_memory = 1024 * 64 * 1024;

    // msgpack needed for json_extractor's undeclared field serialization
    ep_wasm.root_module.addImport("msgpack", msgpack_wasm_dep.module("msgpack"));

    b.installArtifact(ep_wasm);

    const copy_ep_wasm = b.addInstallFileWithDir(
        ep_wasm.getEmittedBin(),
        .{ .custom = "../dist" },
        "event_processor.wasm",
    );
    b.getInstallStep().dependOn(&copy_ep_wasm.step);

    // ==========================================================================
    // EventProcessor FFI dylib - Parse + Compact native library for Bun FFI
    // ==========================================================================
    const ep_ffi = b.addLibrary(.{
        .name = "event_processor",
        .linkage = .dynamic,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/event_processor.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    ep_ffi.root_module.addImport("msgpack", msgpack_dep.module("msgpack"));

    b.installArtifact(ep_ffi);

    const copy_ep_ffi = b.addInstallFileWithDir(
        ep_ffi.getEmittedBin(),
        .{ .custom = "../dist" },
        "libevent_processor.dylib",
    );
    b.getInstallStep().dependOn(&copy_ep_ffi.step);

    // ==========================================================================
    // Test step - runs VM and EventProcessor tests on native target
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

    // EventProcessor tests (Parse + Compact pipeline)
    const ep_test = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/event_processor.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    ep_test.root_module.addImport("msgpack", msgpack_dep.module("msgpack"));

    const run_ep_test = b.addRunArtifact(ep_test);
    test_step.dependOn(&run_ep_test.step);
}
