// Unit tests for the pure transform logic, per ttsc's AGENTS.md §2.2 testing
// shape (one Go unit test for pure logic + one e2e spawning ttsc against a
// fixture — the e2e lives with the consumer fixture once ttsc is a dev dep).
//
// NOTE: cannot run in this repo (no Go toolchain in devenv); run in a
// Go-enabled environment: cd plugin && go test ./...
package lmao

import (
	"path/filepath"
	"testing"
)

func TestNearestPackage(t *testing.T) {
	// This file lives inside @smoothbricks/lmao-ttsc.
	abs, err := filepath.Abs("lmao.go")
	if err != nil {
		t.Fatal(err)
	}
	name, rel := nearestPackage(abs)
	if name != "@smoothbricks/lmao-ttsc" {
		t.Fatalf("nearestPackage name = %q, want @smoothbricks/lmao-ttsc", name)
	}
	if rel != "plugin/driver/lmao.go" {
		t.Fatalf("nearestPackage rel = %q, want plugin/driver/lmao.go", rel)
	}
}

func TestGitLastCommitUnknownOutsideRepo(t *testing.T) {
	if sha := gitLastCommit("/definitely/not/a/file.ts", "/tmp"); sha != "unknown" {
		t.Fatalf("expected unknown, got %q", sha)
	}
}

func TestValidateEntryConfigAcceptsReservedNativePluginTransportConfig(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]any
	}{
		{
			name:   "transform only",
			config: map[string]any{"transform": "@smoothbricks/lmao-ttsc/ttsc-plugin"},
		},
		{
			name:   "transform and enabled",
			config: map[string]any{"transform": "@smoothbricks/lmao-ttsc/ttsc-plugin", "enabled": true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateEntryConfig(tc.config); err != nil {
				t.Fatalf("reserved native plugin transport config was rejected: %v", err)
			}
		})
	}
}

func TestValidateEntryConfigRejectsArbitraryNativePluginOption(t *testing.T) {
	err := validateEntryConfig(map[string]any{
		"transform": "@smoothbricks/lmao-ttsc/ttsc-plugin",
		"cache":     true,
	})
	want := `LMAO1010 @smoothbricks/lmao-ttsc unsupported configuration option "cache"`
	if err == nil || err.Error() != want {
		t.Fatalf("arbitrary plugin option error = %v, want %q", err, want)
	}
}

// The offender named must not depend on Go's randomized map iteration: a
// diagnostic that alternates between two keys across runs is unreproducible.
func TestValidateEntryConfigNamesTheLowestSortingUnsupportedOption(t *testing.T) {
	config := map[string]any{
		"transform": "@smoothbricks/lmao-ttsc/ttsc-plugin",
		"zebra":     1,
		"cache":     true,
		"mode":      "x",
	}
	want := `LMAO1010 @smoothbricks/lmao-ttsc unsupported configuration option "cache"`
	for range 32 {
		err := validateEntryConfig(config)
		if err == nil || err.Error() != want {
			t.Fatalf("diagnostic = %v, want stable %q", err, want)
		}
	}
}
