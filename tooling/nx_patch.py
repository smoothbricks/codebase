#!/usr/bin/env python3
"""Regenerate the lock-selected Nx patch from a Bun-prepared package, without Bun installer metadata."""

import argparse
import base64
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parent.parent


def locked_nx(root: Path) -> tuple[str, str, Path]:
    manifest = json.loads((root / "package.json").read_text())
    version = manifest["overrides"]["nx"]
    key = f"nx@{version}"
    patch = root / manifest["patchedDependencies"][key]
    # Bun's text lock has one complete package array per line. Parse that JSON
    # row, not the lock's JSONC with a regex that could rewrite string values.
    rows = [line.strip().removesuffix(",") for line in (root / "bun.lock").read_text().splitlines()
            if line.strip().startswith('"nx": [')]
    if len(rows) != 1:
        raise ValueError(f"bun.lock must have exactly one Nx package row, found {len(rows)}")
    locked = json.loads("{" + rows[0] + "}")["nx"]
    if locked[0] != key or not locked[-1].startswith("sha512-"):
        raise ValueError(f"Nx override {key} does not match the lock-selected registry package: {locked}")
    return version, locked[-1], patch


def git(source: Path, *arguments: str) -> bytes:
    return subprocess.run(["git", "-C", str(source), *arguments], check=True, stdout=subprocess.PIPE).stdout


def generate(root: Path, prepared: Path, archive: bytes) -> bytes:
    version, integrity, _ = locked_nx(root)
    actual = "sha512-" + base64.b64encode(hashlib.sha512(archive).digest()).decode()
    if actual != integrity:
        raise ValueError(f"registry Nx archive integrity mismatch: expected {integrity}, got {actual}")
    package = json.loads((prepared / "package.json").read_text())
    if package["name"] != "nx" or package["version"] != version:
        raise ValueError(f"prepared package is not lock-selected nx@{version}: {prepared}")
    with tempfile.TemporaryDirectory(prefix="nx-patch-regenerate-") as temporary:
        source = Path(temporary) / "package"
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
            tar.extractall(temporary, filter="data")
        git(source, "init", "--quiet")
        git(source, "add", "--force", "--all")
        # Keep the registry files in Git's index, then replace only its worktree.
        # Tracked diffs preserve deletion bodies and relative paths; --no-index
        # in Bun 1.4.2 loses both for files absent from the prepared package.
        for child in source.iterdir():
            if child.name != ".git":
                if child.is_dir() and not child.is_symlink():
                    shutil.rmtree(child)
                else:
                    child.unlink()
        for child in prepared.iterdir():
            if child.name in {".git", "node_modules"} or child.name.startswith(".bun-tag-"):
                continue
            destination = source / child.name
            if child.is_dir() and not child.is_symlink():
                shutil.copytree(child, destination, symlinks=True)
            else:
                shutil.copy2(child, destination, follow_symlinks=False)
        # Stage only additions with intent-to-add: existing/deleted files remain
        # compared to the pristine registry index, with full blob identities.
        added = git(source, "ls-files", "--others", "-z").decode().split("\0")
        added = [path for path in added if path]
        if added:
            git(source, "add", "--intent-to-add", "--force", "--", *added)
        return git(source, "diff", "--binary", "--full-index", "--no-ext-diff", "--no-renames",
                   "--src-prefix=a/", "--dst-prefix=b/")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="refuse unless a no-op regeneration is byte-identical")
    parser.add_argument("--source", type=Path, default=ROOT / "node_modules/nx", help="Bun-prepared Nx package")
    arguments = parser.parse_args()
    version, _, patch = locked_nx(ROOT)
    with urlopen(f"https://registry.npmjs.org/nx/-/nx-{version}.tgz") as response:
        archive = response.read()
    generated = generate(ROOT, arguments.source, archive)
    digest = hashlib.sha256(generated).hexdigest()
    if arguments.check:
        if generated != patch.read_bytes():
            raise ValueError(f"no-op Nx regeneration differs from {patch}; refusing (generated sha256 {digest})")
        print(f"Nx no-op regeneration: zero diff, {len(generated)} bytes, sha256 {digest}")
    else:
        patch.write_bytes(generated)
        (ROOT / "packages/nx-plugin/managed/raw" / patch.relative_to(ROOT)).write_bytes(generated)
        print(f"Regenerated {patch.relative_to(ROOT)} and its managed copy: {len(generated)} bytes, sha256 {digest}")


if __name__ == "__main__":
    main()
