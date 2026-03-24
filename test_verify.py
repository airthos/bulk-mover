"""
Unit tests for verify.py — compare_file and build_dest_path_lookup.

Run with:  python -m pytest test_verify.py -v
"""
import pytest
from verify import compare_file, build_dest_path_lookup


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _file(name: str, size: int, xor_hash: str = "") -> dict:
    item = {"name": name, "size": size, "file": {}}
    if xor_hash:
        item["file"] = {"hashes": {"quickXorHash": xor_hash}}
    return item


def _delta_item(id_: str, name: str, parent_id: str | None, is_file: bool = True, size: int = 100, xor_hash: str = "abc") -> dict:
    item: dict = {
        "id": id_,
        "name": name,
        "size": size,
        "parentReference": {"id": parent_id} if parent_id else {},
    }
    if is_file:
        item["file"] = {"hashes": {"quickXorHash": xor_hash}}
    else:
        item["folder"] = {}
    return item


# ---------------------------------------------------------------------------
# compare_file — happy paths
# ---------------------------------------------------------------------------

class TestCompareFileOK:
    def test_identical_file(self):
        src = _file("report.pdf", 1000, "abc123")
        dst = _file("report.pdf", 1000, "abc123")
        status, notes = compare_file(src, dst)
        assert status == "OK"
        assert notes == ""

    def test_no_source_hash_still_ok(self):
        """If source hash is missing (not yet computed), result should be OK."""
        src = _file("report.pdf", 1000, "")
        dst = _file("report.pdf", 1000, "abc123")
        status, _ = compare_file(src, dst)
        assert status == "OK"


class TestCompareFileOfficeSPOverhead:
    """SP injects co-authoring XML into Office files — dest is slightly larger."""

    def test_docx_within_threshold(self):
        src = _file("doc.docx", 100_000, "aaa")
        dst = _file("doc.docx", 106_817, "bbb")  # +6817B
        status, notes = compare_file(src, dst)
        assert status == "OK_SP_OVERHEAD"
        assert "6817" in notes

    def test_dotx_template_within_threshold(self):
        """Regression: .dotx was missing from extension set, caused SIZE_MISMATCH."""
        src = _file("template.dotx", 41_425, "aaa")
        dst = _file("template.dotx", 48_240, "bbb")  # +6815B
        status, notes = compare_file(src, dst)
        assert status == "OK_SP_OVERHEAD"

    def test_dotm_template_within_threshold(self):
        src = _file("macro.dotm", 50_000, "aaa")
        dst = _file("macro.dotm", 56_000, "bbb")
        status, _ = compare_file(src, dst)
        assert status == "OK_SP_OVERHEAD"

    def test_xlsm_within_threshold(self):
        src = _file("data.xlsm", 200_000, "aaa")
        dst = _file("data.xlsm", 206_820, "bbb")
        status, _ = compare_file(src, dst)
        assert status == "OK_SP_OVERHEAD"

    def test_pptx_just_at_threshold(self):
        """Regression: .pptx with ~10KB overhead was exceeding old 10KB cap."""
        src = _file("deck.pptx", 48_388_832, "aaa")
        dst = _file("deck.pptx", 48_398_887, "bbb")  # +10055B
        status, _ = compare_file(src, dst)
        assert status == "OK_SP_OVERHEAD"

    def test_docx_overhead_exceeds_threshold_is_mismatch(self):
        """Overhead above 15KB is genuinely suspicious — should stay SIZE_MISMATCH."""
        src = _file("doc.docx", 100_000, "aaa")
        dst = _file("doc.docx", 116_000, "bbb")  # +16000B, over 15KB cap
        status, _ = compare_file(src, dst)
        assert status == "SIZE_MISMATCH"

    def test_dest_smaller_than_source_is_mismatch(self):
        """Truncated dest is data loss — must be flagged."""
        src = _file("doc.docx", 100_000, "aaa")
        dst = _file("doc.docx", 90_000, "bbb")
        status, notes = compare_file(src, dst)
        assert status == "SIZE_MISMATCH"
        assert "delta=-10000" in notes


class TestCompareFileImageMeta:
    """SP rewrites image metadata — hash or size may change slightly."""

    def test_same_size_different_hash(self):
        src = _file("photo.jpg", 500_000, "aaa")
        dst = _file("photo.jpg", 500_000, "bbb")
        status, notes = compare_file(src, dst)
        assert status == "OK_IMAGE_META"
        assert "hash differs" in notes

    def test_png_with_small_size_increase(self):
        """Regression: image with dest slightly larger had no handler, was SIZE_MISMATCH."""
        src = _file("logo.png", 311_787, "aaa")
        dst = _file("logo.png", 329_271, "bbb")  # +17484B
        status, notes = compare_file(src, dst)
        assert status == "OK_IMAGE_META"
        assert "17484" in notes

    def test_jpg_tiny_size_delta(self):
        src = _file("symbol.jpg", 725_788, "aaa")
        dst = _file("symbol.jpg", 725_814, "bbb")  # +26B
        status, _ = compare_file(src, dst)
        assert status == "OK_IMAGE_META"

    def test_image_overhead_exceeds_threshold_is_mismatch(self):
        """Image overhead above 25KB is suspicious."""
        src = _file("photo.jpg", 100_000, "aaa")
        dst = _file("photo.jpg", 130_000, "bbb")  # +30000B, over 25KB cap
        status, _ = compare_file(src, dst)
        assert status == "SIZE_MISMATCH"

    def test_png_same_hash(self):
        """PNG with same size and same hash is just OK."""
        src = _file("icon.png", 5000, "same")
        dst = _file("icon.png", 5000, "same")
        status, _ = compare_file(src, dst)
        assert status == "OK"


class TestCompareFileMismatches:
    def test_size_mismatch_unknown_type(self):
        src = _file("data.bin", 1000, "aaa")
        dst = _file("data.bin", 2000, "bbb")
        status, notes = compare_file(src, dst)
        assert status == "SIZE_MISMATCH"
        assert "source=1000" in notes
        assert "dest=2000" in notes
        assert "delta=+1000" in notes

    def test_hash_mismatch_non_image(self):
        src = _file("code.py", 500, "aaa")
        dst = _file("code.py", 500, "bbb")
        status, notes = compare_file(src, dst)
        assert status == "HASH_MISMATCH"
        assert "aaa" in notes
        assert "bbb" in notes

    def test_hash_pending(self):
        src = _file("doc.pdf", 500, "aaa")
        dst = {"name": "doc.pdf", "size": 500, "file": {}}  # no hash
        status, notes = compare_file(src, dst)
        assert status == "HASH_PENDING"


# ---------------------------------------------------------------------------
# build_dest_path_lookup
# ---------------------------------------------------------------------------

class TestBuildDestPathLookup:

    def _make_root_tree(self, root_id: str):
        """
        Returns items for this tree:
          root (root_id, folder, no parent)
          └── Engineering (folder)
              └── specs.pdf (file, hash="abc", size=500)
              └── sub (folder)
                  └── notes.txt (file, hash="xyz", size=100)
        """
        return [
            _delta_item("root-id", "Documents", None, is_file=False),
            _delta_item("eng-id", "Engineering", "root-id", is_file=False),
            _delta_item("pdf-id", "specs.pdf", "eng-id", size=500, xor_hash="abc"),
            _delta_item("sub-id", "sub", "eng-id", is_file=False),
            _delta_item("txt-id", "notes.txt", "sub-id", size=100, xor_hash="xyz"),
        ]

    def test_with_real_guid_root(self):
        items = self._make_root_tree("root-id")
        lookup = build_dest_path_lookup(items, "root-id")
        assert "Engineering/specs.pdf" in lookup
        assert "Engineering/sub/notes.txt" in lookup
        assert lookup["Engineering/specs.pdf"]["id"] == "pdf-id"

    def test_with_literal_root_string(self):
        """
        Regression: when dest_root_id is the literal string 'root', the
        function must auto-detect the actual root GUID from the delta items.
        Previously this caused all files to appear MISSING.
        """
        items = self._make_root_tree("root-id")
        lookup = build_dest_path_lookup(items, "root")
        # Paths should be relative to the drive root — no library folder prefix
        assert "Engineering/specs.pdf" in lookup, (
            "Got keys: " + str(list(lookup.keys()))
        )
        assert "Engineering/sub/notes.txt" in lookup

    def test_with_literal_root_string_root_item_absent(self):
        """
        Regression: Graph API often omits the root item from delta responses.
        When the root item is absent, the fallback detection fails and 'root'
        is never resolved — parent IDs never match and all files return None.
        The callers must resolve 'root' → real GUID via API before calling this
        function; if they do, items can be absent and paths still build correctly.
        """
        # Simulate: root item NOT in delta response (common in production)
        items = [
            # NO root item here
            _delta_item("eng-id", "Engineering", "root-id", is_file=False),
            _delta_item("pdf-id", "specs.pdf", "eng-id", size=500, xor_hash="abc"),
        ]
        # Callers should pass the resolved real GUID, not "root"
        lookup = build_dest_path_lookup(items, "root-id")
        assert "Engineering/specs.pdf" in lookup

    def test_literal_root_paths_match_manifest_format(self):
        """
        Source paths in the manifest look like 'FolderName/file.ext'.
        When dest_root_id='root', delta lookup keys must use the same format.
        """
        items = [
            _delta_item("root-id", "Standard Forms", None, is_file=False),
            _delta_item("batch-id", "Airtho - Holiday Schedule", "root-id", is_file=False),
            _delta_item("file-id", "Airtho Holidays 2023.pdf", "batch-id", size=100, xor_hash="abc"),
        ]
        lookup = build_dest_path_lookup(items, "root")
        assert "Airtho - Holiday Schedule/Airtho Holidays 2023.pdf" in lookup

    def test_excludes_deleted_items(self):
        items = self._make_root_tree("root-id")
        items.append({
            "id": "del-id", "name": "deleted.pdf", "deleted": {"state": "deleted"},
            "parentReference": {"id": "eng-id"},
            "file": {}, "size": 50,
        })
        lookup = build_dest_path_lookup(items, "root-id")
        assert "Engineering/deleted.pdf" not in lookup

    def test_excludes_folders_from_result(self):
        items = self._make_root_tree("root-id")
        lookup = build_dest_path_lookup(items, "root-id")
        # Folders should not appear as lookup keys
        assert "Engineering" not in lookup
        assert "Engineering/sub" not in lookup

    def test_empty_items(self):
        lookup = build_dest_path_lookup([], "root-id")
        assert lookup == {}

    def test_orphaned_item_excluded(self):
        """File whose parent is not in delta set should be skipped, not crash."""
        items = [
            _delta_item("root-id", "Root", None, is_file=False),
            # file whose parent GUID is not in the item list
            _delta_item("file-id", "orphan.pdf", "missing-parent-id"),
        ]
        lookup = build_dest_path_lookup(items, "root-id")
        assert "orphan.pdf" not in lookup
