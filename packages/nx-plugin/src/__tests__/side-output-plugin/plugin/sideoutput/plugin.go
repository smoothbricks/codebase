// Package sideoutput writes one file into the compile's outDir beside the compiler's outputs, as a census-writing lint
// plugin does: the typescript-emit executor's tests hold that such a file survives every emit.
package sideoutput

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/samchon/ttsc/packages/ttsc/driver"
)

// File is the side output's name in outDir.
const File = "side-output.json"

func init() {
	driver.RegisterPlugin(plugin{})
}

type plugin struct{}

func (plugin) ApplyProgram(prog *driver.Program, ctx driver.PluginContext) error {
	root := filepath.Dir(filepath.FromSlash(prog.ParsedConfig.ConfigName()))
	outDir := prog.ParsedConfig.CompilerOptions().OutDir
	if outDir == "" {
		return fmt.Errorf("side-output-plugin: %s names no outDir", ctx.Tsconfig)
	}
	if !filepath.IsAbs(outDir) {
		outDir = filepath.Join(root, outDir)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	target := filepath.Join(outDir, File)
	if err := os.WriteFile(target, []byte("{\"side\":true}\n"), 0o644); err != nil {
		return err
	}
	// A host lists the file with the compiler's outputs, so a build that keeps exactly what an emit lists keeps it.
	ctx.ReportHostOutput(target)
	return nil
}
