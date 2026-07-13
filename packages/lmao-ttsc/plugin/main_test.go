// Unit tests for the pure transform logic, per ttsc's AGENTS.md §2.2 testing
// shape (one Go unit test for pure logic + one e2e spawning ttsc against a
// fixture — the e2e lives with the consumer fixture once ttsc is a dev dep).
//
// NOTE: cannot run in this repo (no Go toolchain in devenv); run in a
// Go-enabled environment: cd plugin && go test ./...
package main

import (
	"path/filepath"
	"strings"
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

func TestReadOptionsResolvesExplicitTsconfigFromCwd(t *testing.T) {
	options, err := readOptions([]string{
		"--cwd=/x",
		"--tsconfig=config/custom.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if options.cwd != "/x" {
		t.Fatalf("cwd = %q, want /x", options.cwd)
	}
	if options.tsconfig != filepath.Join("/x", "config", "custom.json") {
		t.Fatalf("tsconfig = %q, want explicit path resolved from cwd", options.tsconfig)
	}
}

func TestReadOptionsAcceptsReservedNativePluginTransportConfig(t *testing.T) {
	cases := []struct {
		name   string
		config string
	}{
		{
			name:   "transform only",
			config: `{"transform":"@smoothbricks/lmao-ttsc/ttsc-plugin"}`,
		},
		{
			name:   "transform and enabled",
			config: `{"transform":"@smoothbricks/lmao-ttsc/ttsc-plugin","enabled":true}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pluginsJSON := `[{"name":"@smoothbricks/lmao-ttsc","stage":"transform","config":` + tc.config + `}]`
			options, err := readOptions([]string{
				"--cwd=/x",
				"--tsconfig=config/custom.json",
				"--plugins-json=" + pluginsJSON,
			})
			if err != nil {
				t.Fatalf("reserved native plugin transport config was rejected: %v", err)
			}
			if options.cwd != "/x" || options.tsconfig != filepath.Join("/x", "config", "custom.json") {
				t.Fatalf("accepted transport config changed explicit compiler options: %+v", options)
			}
		})
	}
}

func TestReadOptionsRejectsArbitraryNativePluginOption(t *testing.T) {
	pluginsJSON := `[{"name":"@smoothbricks/lmao-ttsc","stage":"transform","config":{"transform":"@smoothbricks/lmao-ttsc/ttsc-plugin","cache":true}}]`
	_, err := readOptions([]string{"--cwd=/x", "--plugins-json=" + pluginsJSON})
	want := `LMAO1010 @smoothbricks/lmao-ttsc unsupported configuration option "cache"`
	if err == nil || err.Error() != want {
		t.Fatalf("arbitrary plugin option error = %v, want %q", err, want)
	}
}

func TestReadOptionsPreservesMalformedTrailingAndDuplicatePluginDiagnostics(t *testing.T) {
	valid := `{"name":"@smoothbricks/lmao-ttsc","stage":"transform","config":{"transform":"@smoothbricks/lmao-ttsc/ttsc-plugin"}}`
	cases := []struct {
		name    string
		payload string
		want    string
		exact   bool
	}{
		{name: "malformed", payload: `[`, want: "LMAO1010 malformed --plugins-json:"},
		{name: "trailing value", payload: `[] []`, want: "LMAO1010 malformed --plugins-json: trailing JSON value", exact: true},
		{name: "duplicate plugin", payload: `[` + valid + `,` + valid + `]`, want: "LMAO1010 --plugins-json contains multiple @smoothbricks/lmao-ttsc entries", exact: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readOptions([]string{"--cwd=/x", "--plugins-json=" + tc.payload})
			if err == nil {
				t.Fatalf("readOptions accepted %s plugin payload", tc.name)
			}
			if tc.exact && err.Error() != tc.want {
				t.Fatalf("diagnostic = %q, want %q", err, tc.want)
			}
			if !tc.exact && !strings.HasPrefix(err.Error(), tc.want) {
				t.Fatalf("diagnostic = %q, want prefix %q", err, tc.want)
			}
		})
	}
}
