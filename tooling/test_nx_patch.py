import base64
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from nx_patch import generate


class NxPatchRegeneration(unittest.TestCase):
    def fixture(self, root: Path) -> tuple[Path, bytes]:
        package = {"name": "nx", "version": "23.2.1"}
        files = {"package.json": json.dumps(package), "deleted.js": "old\n", "changed.js": "before\n"}
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w:gz") as tar:
            for name, text in files.items():
                data = text.encode()
                entry = tarfile.TarInfo(f"package/{name}")
                entry.size = len(data)
                entry.mode = 0o644
                tar.addfile(entry, io.BytesIO(data))
        archive = stream.getvalue()
        integrity = "sha512-" + base64.b64encode(hashlib.sha512(archive).digest()).decode()
        (root / "package.json").write_text(json.dumps({
            "overrides": {"nx": "23.2.1"},
            "patchedDependencies": {"nx@23.2.1": "patches/nx@23.2.1.patch"},
        }))
        (root / "bun.lock").write_text('"nx": ' + json.dumps(["nx@23.2.1", "", {}, integrity]) + ',\n')
        prepared = root / "prepared"
        prepared.mkdir()
        for name, text in files.items():
            (prepared / name).write_text(text)
        return prepared, archive

    def test_regeneration_keeps_deletions_additions_and_relative_paths_but_not_installer_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prepared, archive = self.fixture(root)
            (prepared / "deleted.js").unlink()
            (prepared / "changed.js").write_text("after\n")
            (prepared / "added.js").write_text("new\n")
            (prepared / ".bun-tag-marker").touch()
            (prepared / "node_modules").mkdir()
            (prepared / "node_modules/dependency.js").write_text("not package source\n")
            patch = generate(root, prepared, archive).decode()
            self.assertIn("--- a/deleted.js\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\n", patch)
            self.assertIn("+++ b/added.js", patch)
            self.assertIn("-before\n+after\n", patch)
            self.assertNotIn(".bun-tag", patch)
            self.assertNotIn("node_modules", patch)
            self.assertNotIn(str(root), patch)
            self.assertEqual(patch.encode(), generate(root, prepared, archive))

    def test_extraction_keeps_the_data_filter_when_the_runtime_supports_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prepared, archive = self.fixture(root)
            extractall = tarfile.TarFile.extractall
            with patch.object(tarfile.TarFile, "extractall", autospec=True, side_effect=extractall) as extract:
                self.assertEqual(generate(root, prepared, archive), b"")
            self.assertEqual(extract.call_count, 1)
            expected = {"filter": "data"} if sys.version_info >= (3, 12) else {}
            self.assertEqual(extract.call_args.kwargs, expected)

    def test_archive_integrity_is_required(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prepared, archive = self.fixture(root)
            with self.assertRaisesRegex(ValueError, "archive integrity mismatch"):
                generate(root, prepared, archive + b"changed")

    def test_prepared_package_must_match_the_lock_selected_version(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prepared, archive = self.fixture(root)
            (prepared / "package.json").write_text(json.dumps({"name": "nx", "version": "other"}))
            with self.assertRaisesRegex(ValueError, "not lock-selected nx@23.2.1"):
                generate(root, prepared, archive)


if __name__ == "__main__":
    unittest.main()
