//go:build ttsc_typescript_7_1

package lmao

import shimast "github.com/microsoft/typescript-go/shim/ast"

// sourceFileHasIdentifier reports whether the parser saw name in sf. The version-neutral shim API (ttsc tsgo.mdx, Compiler Targets) declares no function for it: TypeScript 7.1 hides the identifier table behind HasIdentifier.
func sourceFileHasIdentifier(sf *shimast.SourceFile, name string) bool {
	return sf.HasIdentifier(name)
}
