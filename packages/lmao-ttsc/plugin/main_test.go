// Unit tests for the pure transform logic, per ttsc's AGENTS.md §2.2 testing
// shape (one Go unit test for pure logic + one e2e spawning ttsc against a
// fixture — the e2e lives with the consumer fixture once ttsc is a dev dep).
//
// NOTE: cannot run in this repo (no Go toolchain in devenv); run in a
// Go-enabled environment: cd plugin && go test ./...
package main

import (
	"path/filepath"
	"testing"
)

func TestNearestPackage(t *testing.T) {
	// This file lives inside @smoothbricks/lmao-ttsc.
	abs, err := filepath.Abs("main.go")
	if err != nil {
		t.Fatal(err)
	}
	name, rel := nearestPackage(abs)
	if name != "@smoothbricks/lmao-ttsc" {
		t.Fatalf("nearestPackage name = %q, want @smoothbricks/lmao-ttsc", name)
	}
	if rel != "plugin/main.go" {
		t.Fatalf("nearestPackage rel = %q, want plugin/main.go", rel)
	}
}

func TestGitLastCommitUnknownOutsideRepo(t *testing.T) {
	if sha := gitLastCommit("/definitely/not/a/file.ts", "/tmp"); sha != "unknown" {
		t.Fatalf("expected unknown, got %q", sha)
	}
}

func TestReadFlags(t *testing.T) {
	cwd, tsconfig := readFlags([]string{"--cwd=/x", "--tsconfig=custom.json"})
	if cwd != "/x" {
		t.Fatalf("cwd = %q", cwd)
	}
	if tsconfig != filepath.Join("/x", "custom.json") {
		t.Fatalf("tsconfig = %q", tsconfig)
	}
}
