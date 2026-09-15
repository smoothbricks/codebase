# watch-cost fixture

The `emit-excluded` shape (a governing program over `src/` + `scripts/`, an emit program over `src/` alone, an operator
entry point the emit program excludes) spread over eight source directories, because ttsc opens one directory watcher
per project directory and per host-input directory to prove a transform generation reusable.

That count is the whole point. Under Bun 1.4.2 on macOS each `fs.watch` registration after the first blocks for seconds,
so loading this entry point through ttsc's own watch fallback took **204.87 s** wall for 1.8 s of CPU. With the polling
seam `src/bun/preload.ts` supplies, the same load is **2.36 s**.

`src/mod*/index.ts` are deliberately trivial: the directories, not their contents, are what the watch cost scales with.
