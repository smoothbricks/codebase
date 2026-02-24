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

    const rawr_native_dep = b.dependency("rawr", .{
        .target = target,
        .optimize = optimize,
    });
    const rawr_wasm_dep = b.dependency("rawr", .{
        .target = wasm_target,
        .optimize = .ReleaseSmall,
    });

    // ==========================================================================
    // Dependencies - simdjzon (SIMD JSON parser, native targets only)
    // ==========================================================================

    // simdjzon SIMD JSON parser - native targets only (x86_64, aarch64).
    // WASM targets use std.json.Scanner fallback (simdjzon requires 64-bit SIMD).
    const is_native_simdjzon_target = target.query.cpu_arch != .wasm32 and target.query.cpu_arch != .wasm64;

    var simdjzon_mod: ?*std.Build.Module = null;
    if (is_native_simdjzon_target) {
        const simdjzon_dep = b.dependency("simdjzon", .{
            .target = target,
            .optimize = optimize,
        });
        simdjzon_mod = simdjzon_dep.module("simdjzon");
    }

    const build_opts_simdjzon = b.addOptions();
    build_opts_simdjzon.addOption(bool, "use_simdjzon", is_native_simdjzon_target);

    const build_opts_no_simdjzon = b.addOptions();
    build_opts_no_simdjzon.addOption(bool, "use_simdjzon", false);

    // ==========================================================================
    // and get the reducer VM's public API (vm.zig).
    // ==========================================================================
    const columine_module = b.addModule("columine", .{
        .root_source_file = b.path("src/vm/vm.zig"),
        .target = target,
        .optimize = optimize,
    });
    columine_module.addImport("rawr", rawr_native_dep.module("rawr"));

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
    // 3MB initial; grow on demand at runtime.
    columine_wasm.initial_memory = 48 * 64 * 1024;
    columine_wasm.max_memory = 4096 * 64 * 1024;

    // msgpack available for WASM (future Parse/Compact stages)
    columine_wasm.root_module.addImport("msgpack", msgpack_wasm_dep.module("msgpack"));
    columine_wasm.root_module.addImport("rawr", rawr_wasm_dep.module("rawr"));

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
    columine_ffi.root_module.addImport("rawr", rawr_native_dep.module("rawr"));

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
    // Start small and grow on demand.
    ep_wasm.initial_memory = 64 * 64 * 1024;
    ep_wasm.max_memory = 4096 * 64 * 1024;

    // msgpack needed for json_extractor's undeclared field serialization
    ep_wasm.root_module.addImport("msgpack", msgpack_wasm_dep.module("msgpack"));
    // WASM: no simdjzon, use Scanner fallback
    ep_wasm.root_module.addOptions("build_options", build_opts_no_simdjzon);
    // each package wires its own columns.zig and dynamic_schema.zig.
    addParsingModules(b, ep_wasm.root_module, build_opts_no_simdjzon, null);

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
    // Native: simdjzon SIMD backend + build option
    if (simdjzon_mod) |mod| {
        ep_ffi.root_module.addImport("simdjzon", mod);
    }
    ep_ffi.root_module.addOptions("build_options", build_opts_simdjzon);
    addParsingModules(b, ep_ffi.root_module, build_opts_simdjzon, simdjzon_mod);

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
    vm_test.root_module.addImport("rawr", rawr_native_dep.module("rawr"));

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
    // Native: simdjzon SIMD backend + build option for tests
    if (simdjzon_mod) |mod| {
        ep_test.root_module.addImport("simdjzon", mod);
    }
    ep_test.root_module.addOptions("build_options", build_opts_simdjzon);
    addParsingModules(b, ep_test.root_module, build_opts_simdjzon, simdjzon_mod);

    const run_ep_test = b.addRunArtifact(ep_test);
    test_step.dependOn(&run_ep_test.step);
}

// =============================================================================
// Helpers
// =============================================================================

/// Wire all named modules that event_processor.zig and its transitive
/// dependencies import. Zig 0.15 requires each file to belong to exactly
/// one module, so every cross-file @import in the EP tree is a named module.
/// package wires its own columns.zig and dynamic_schema.zig.
fn addParsingModules(
    b: *std.Build,
    root: *std.Build.Module,
    build_opts: *std.Build.Step.Options,
    maybe_simdjzon: ?*std.Build.Module,
) void {
    // --- leaf: dynamic_schema has no cross-file deps ---
    const ds = b.createModule(.{
        .root_source_file = b.path("src/arrow/dynamic_schema.zig"),
    });
    root.addImport("dynamic_schema", ds);

    // --- dynamic_record_batch depends on dynamic_schema ---
    const drb = b.createModule(.{
        .root_source_file = b.path("src/arrow/dynamic_record_batch.zig"),
    });
    drb.addImport("dynamic_schema", ds);
    root.addImport("dynamic_record_batch", drb);

    // --- columns depends on dynamic_schema ---
    const cols = b.createModule(.{
        .root_source_file = b.path("src/parsing/columns.zig"),
    });
    cols.addImport("dynamic_schema", ds);
    root.addImport("columns", cols);

    // --- json_parser depends on build_options + optional simdjzon ---
    const jp = b.createModule(.{
        .root_source_file = b.path("src/parsing/json_parser.zig"),
    });
    jp.addOptions("build_options", build_opts);
    if (maybe_simdjzon) |mod| {
        jp.addImport("simdjzon", mod);
    }
    root.addImport("json_parser", jp);

    // --- json_scanner depends on columns ---
    const js = b.createModule(.{
        .root_source_file = b.path("src/parsing/json_scanner.zig"),
    });
    js.addImport("columns", cols);
    root.addImport("json_scanner", js);

    // --- json_extractor depends on json_parser, columns, dynamic_schema ---
    const je = b.createModule(.{
        .root_source_file = b.path("src/parsing/json_extractor.zig"),
    });
    je.addImport("json_parser", jp);
    je.addImport("columns", cols);
    je.addImport("dynamic_schema", ds);
    root.addImport("json_extractor", je);

    // --- msgpack_scanner depends on columns ---
    const ms = b.createModule(.{
        .root_source_file = b.path("src/parsing/msgpack_scanner.zig"),
    });
    ms.addImport("columns", cols);
    root.addImport("msgpack_scanner", ms);

    // --- msgpack_extractor depends on json_extractor, columns, dynamic_schema, msgpack_scanner ---
    const me = b.createModule(.{
        .root_source_file = b.path("src/parsing/msgpack_extractor.zig"),
    });
    me.addImport("json_extractor", je);
    me.addImport("columns", cols);
    me.addImport("dynamic_schema", ds);
    me.addImport("msgpack_scanner", ms);
    root.addImport("msgpack_extractor", me);

    // --- ipc_writer depends on columns, dynamic_schema, dynamic_record_batch ---
    const iw = b.createModule(.{
        .root_source_file = b.path("src/arrow/ipc_writer.zig"),
    });
    iw.addImport("columns", cols);
    iw.addImport("dynamic_schema", ds);
    iw.addImport("dynamic_record_batch", drb);
    root.addImport("ipc_writer", iw);
}
