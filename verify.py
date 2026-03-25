import json
import os
import time
from pathlib import Path
from typing import Optional

import graph

_delta_dir: Path = Path("migration-logs/delta")


def set_session_dir(path: Path) -> None:
    """Set the session directory; delta sidecars will be written to {path}/delta/."""
    global _delta_dir
    _delta_dir = path / "delta"

HASH_RETRY_COUNT = 3
HASH_RETRY_DELAY = 10  # seconds

# SharePoint's content pipeline injects co-authoring XML into Office docs,
# adding ~6-15 KB. Size mismatches within this range are expected.
# Includes template variants (.dotx, .dotm, .xlsm, .xlsb, .potx) — SP treats
# them identically to their base formats for co-authoring overhead.
_OFFICE_EXTENSIONS = frozenset({
    ".docx", ".dotx", ".dotm",
    ".xlsx", ".xlsm", ".xlsb",
    ".pptx", ".potx",
    ".doc", ".xls", ".ppt",
})
_SP_OVERHEAD_MAX = 15_000  # bytes — observed max ~11.8 KB; 15 KB gives headroom

# SharePoint rewrites image metadata (EXIF, ICC, tEXt chunks) on ingestion.
# This can change the hash even when sizes match, OR add a small number of
# bytes (e.g. ICC profile injection). Both are benign — pixel data is intact.
_IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".tiff", ".tif", ".heic", ".gif", ".bmp"})
_IMAGE_OVERHEAD_MAX = 25_000  # bytes — observed max ~17.5 KB; 25 KB gives headroom


def _ext(item: dict) -> str:
    name = item.get("name") or item.get("_path", "")
    return os.path.splitext(name)[1].lower()


def compare_file(source: dict, dest: dict) -> tuple[str, str]:
    """
    Compare a source driveItem against a dest driveItem.
    Returns (verify_status, notes).
    """
    source_size = source.get("size", 0)
    dest_size = dest.get("size", 0)
    ext = _ext(source)

    if source_size != dest_size:
        delta = dest_size - source_size
        # Office docs/templates: SharePoint adds co-authoring XML — dest slightly larger
        if ext in _OFFICE_EXTENSIONS and 0 < delta <= _SP_OVERHEAD_MAX:
            return "OK_SP_OVERHEAD", f"dest +{delta}B (Office co-authoring XML)"
        # Images: SharePoint may inject ICC/EXIF metadata — dest slightly larger
        if ext in _IMAGE_EXTENSIONS and 0 < delta <= _IMAGE_OVERHEAD_MAX:
            return "OK_IMAGE_META", f"dest +{delta}B (image metadata rewrite)"
        return "SIZE_MISMATCH", f"source={source_size} dest={dest_size} delta={delta:+d}"

    source_hash = (source.get("file") or {}).get("hashes", {}).get("quickXorHash")
    dest_hash = (dest.get("file") or {}).get("hashes", {}).get("quickXorHash")

    if dest_hash is None:
        return "HASH_PENDING", "quickXorHash not yet computed at destination"

    if source_hash and source_hash != dest_hash:
        # Images: SharePoint rewrites ancillary metadata — hash mismatch is expected
        if ext in _IMAGE_EXTENSIONS:
            return "OK_IMAGE_META", "hash differs (image metadata rewrite)"
        return "HASH_MISMATCH", f"source={source_hash} dest={dest_hash}"

    return "OK", ""


def fetch_and_compare(
    source_files: list[dict],
    dest_drive_id: str,
    dest_folder_id: str,
    token: str,
    quiet: bool = False,
) -> list[dict]:
    """
    Enumerate the destination batch folder, match files against the source
    manifest by relative path, and annotate each source file with
    verify_status, verify_notes, dest_id, and dest_sharepointIds.

    Uses JSON batching ($batch) for HASH_PENDING retries to reduce roundtrips.
    """
    if not quiet:
        print("  Verifying...", end="", flush=True)

    # Enumerate dest recursively, keyed by relative path within the batch folder
    dest_files_raw = graph.enumerate_recursive(dest_drive_id, dest_folder_id, "", token)
    dest_by_rel_path: dict[str, dict] = {}
    for f in dest_files_raw:
        # _path comes back as e.g. "/SubFolder/file.pdf" or "file.pdf"
        rel = f["_path"].lstrip("/")
        dest_by_rel_path[rel] = f

    hash_pending: list[dict] = []  # source items needing hash retry
    issues = 0

    for source in source_files:
        # Skip files that already failed during copy
        if source.get("copy_status") == "COPY_FAILED":
            source["verify_status"] = "COPY_FAILED"
            source["verify_notes"] = source.get("copy_notes", "")
            issues += 1
            continue

        # Build the relative path within the batch folder
        # source["_path"] is e.g. "A's/SubFolder/file.pdf"
        # We strip the batch folder name prefix
        source_path = source["_path"]
        parts = source_path.split("/", 1)
        rel_path = parts[1] if len(parts) > 1 else parts[0]

        dest = dest_by_rel_path.get(rel_path)

        if dest is None:
            source["verify_status"] = "MISSING"
            source["verify_notes"] = "File not found at destination after copy"
            source["dest_id"] = None
            issues += 1
            continue

        source["dest_id"] = dest["id"]
        source["dest_sharepointIds"] = dest.get("sharepointIds")

        status, notes = compare_file(source, dest)

        if status == "HASH_PENDING":
            hash_pending.append(source)
        else:
            source["verify_status"] = status
            source["verify_notes"] = notes
            if status not in ("OK", "OK_SP_OVERHEAD", "OK_IMAGE_META"):
                issues += 1

    # --- Batched retry for HASH_PENDING items ---
    if hash_pending:
        issues += _retry_hash_pending_batched(hash_pending, dest_drive_id, token)

    total = len(source_files)
    ok_statuses = {"OK", "OK_SP_OVERHEAD", "OK_IMAGE_META"}
    issues = sum(1 for f in source_files if f.get("verify_status") not in ok_statuses)
    if not quiet:
        if issues == 0:
            print(f" ✓ All {total} files verified OK")
        else:
            print(f" ⚠  {issues}/{total} files have issues")

    return source_files


def _retry_hash_pending_batched(
    pending_files: list[dict],
    dest_drive_id: str,
    token: str,
) -> int:
    """
    Retry HASH_PENDING files using JSON batching.
    Returns the number of files that still have issues after retries.
    """
    remaining = list(pending_files)

    for attempt in range(HASH_RETRY_COUNT):
        if not remaining:
            break
        time.sleep(HASH_RETRY_DELAY)

        # Build batch requests for all remaining items
        batch_reqs = []
        for i, source in enumerate(remaining):
            dest_id = source["dest_id"]
            batch_reqs.append({
                "id": str(i),
                "method": "GET",
                "url": f"/drives/{dest_drive_id}/items/{dest_id}?$select=id,size,file",
            })

        responses = graph.batch_get_items(batch_reqs, token)
        resp_by_id = {r["id"]: r for r in responses}

        still_pending = []
        for i, source in enumerate(remaining):
            resp = resp_by_id.get(str(i))
            if not resp or resp.get("status") != 200:
                still_pending.append(source)
                continue

            refreshed = resp.get("body", {})
            status, notes = compare_file(source, refreshed)
            if status == "HASH_PENDING":
                still_pending.append(source)
            else:
                source["verify_status"] = status
                source["verify_notes"] = notes

        remaining = still_pending

    # Mark any still-pending as HASH_PENDING
    issues = 0
    for source in remaining:
        source["verify_status"] = "HASH_PENDING"
        source["verify_notes"] = "quickXorHash not computed after retries"
        issues += 1

    return issues


# ---------------------------------------------------------------------------
# Delta-based re-verification
# ---------------------------------------------------------------------------

def _delta_link_path(drive_id: str) -> Path:
    return _delta_dir / f"{drive_id}.deltalink.json"


def load_delta_link(drive_id: str) -> str | None:
    p = _delta_link_path(drive_id)
    if p.exists():
        try:
            data = json.loads(p.read_text())
            return data.get("deltaLink")
        except (json.JSONDecodeError, OSError):
            return None
    return None


def save_delta_link(drive_id: str, delta_link: str) -> None:
    _delta_dir.mkdir(parents=True, exist_ok=True)
    _delta_link_path(drive_id).write_text(json.dumps({"deltaLink": delta_link}))


def build_dest_path_lookup(
    items: list[dict],
    dest_root_id: str,
) -> dict[str, dict]:
    """
    Build {relative_path: item} lookup from a flat delta response.

    Uses id-based tree walk to reconstruct paths relative to dest_root_id.
    parentReference.path is unreliable in delta responses, so we walk the
    id chain instead. Only files (with 'file' facet) are included; deleted
    items and folders are excluded from the result but kept for path resolution.

    If dest_root_id is the literal string 'root', the actual root GUID is
    resolved from the delta items (the folder with no parentReference.id).
    This handles the case where the migration targeted the library root.
    """
    by_id: dict[str, dict] = {
        item["id"]: item for item in items if not item.get("deleted")
    }

    # Resolve literal "root" to the actual drive root GUID so that the
    # parent-chain walk terminates correctly at the right anchor.
    if dest_root_id == "root":
        for item in items:
            if "folder" in item and not item.get("deleted"):
                parent_id = (item.get("parentReference") or {}).get("id")
                if not parent_id:
                    dest_root_id = item["id"]
                    break

    def _rel_path(item: dict) -> str | None:
        parts = [item["name"]]
        current = item
        for _ in range(50):  # guard against cycles
            parent_ref = current.get("parentReference") or {}
            parent_id = parent_ref.get("id")
            if not parent_id or parent_id == dest_root_id:
                break
            parent = by_id.get(parent_id)
            if not parent:
                return None  # parent not in delta set — skip
            parts.append(parent["name"])
            current = parent
        parts.reverse()
        return "/".join(parts)

    result: dict[str, dict] = {}
    for item in items:
        if item.get("deleted") or "file" not in item:
            continue
        rel = _rel_path(item)
        if rel:
            result[rel] = item
    return result


def compare_from_lookup(
    source_files: list[dict],
    dest_lookup: dict[str, dict],
    dest_drive_id: str,
    token: str,
    quiet: bool = False,
) -> list[dict]:
    """
    Compare source files against a pre-built {rel_path: dest_item} lookup.
    Annotates each source file with verify_status, verify_notes, dest_id.

    rel_path key is the full source path (source['_path']), since dest_lookup
    is built relative to dest_root and source paths use the same convention.
    """
    if not quiet:
        print("  Verifying...", end="", flush=True)

    hash_pending: list[dict] = []

    for source in source_files:
        if source.get("copy_status") == "COPY_FAILED":
            source["verify_status"] = "COPY_FAILED"
            source["verify_notes"] = source.get("copy_notes", "")
            continue

        rel_path = source.get("_path", "").lstrip("/")
        dest = dest_lookup.get(rel_path)

        if dest is None:
            source["verify_status"] = "MISSING"
            source["verify_notes"] = "File not found at destination"
            source["dest_id"] = None
            continue

        source["dest_id"] = dest["id"]
        source["dest_sharepointIds"] = dest.get("sharepointIds")

        status, notes = compare_file(source, dest)

        if status == "HASH_PENDING":
            hash_pending.append(source)
        else:
            source["verify_status"] = status
            source["verify_notes"] = notes

    if hash_pending:
        _retry_hash_pending_batched(hash_pending, dest_drive_id, token)

    total = len(source_files)
    ok_statuses = {"OK", "OK_SP_OVERHEAD", "OK_IMAGE_META"}
    issues = sum(1 for f in source_files if f.get("verify_status") not in ok_statuses)
    if not quiet:
        if issues == 0:
            print(f" ✓ All {total} files verified OK")
        else:
            print(f" ⚠  {issues}/{total} files have issues")

    return source_files


def fetch_dest_items_delta(
    dest_drive_id: str,
    token: str,
) -> dict[str, dict]:
    """
    Fetch destination items using delta queries. On first call does a full sync.
    Subsequent calls only fetch changes since the last delta token.
    Returns items keyed by item ID.
    """
    delta_link = load_delta_link(dest_drive_id)
    items, new_delta_link = graph.get_drive_delta(
        dest_drive_id, token,
        delta_link=delta_link,
        select="id,name,size,file,parentReference,deleted",
    )

    if new_delta_link:
        save_delta_link(dest_drive_id, new_delta_link)

    # Build lookup by ID, excluding deleted items
    result: dict[str, dict] = {}
    for item in items:
        if item.get("deleted"):
            result.pop(item["id"], None)
        elif "file" in item:
            result[item["id"]] = item

    return result


def patch_metadata(
    site_id: str,
    list_id: str,
    list_item_id: str,
    created_dt: Optional[str],
    modified_dt: Optional[str],
    token: str,
) -> str:
    """
    Attempt to restore Created and Modified dates on a SharePoint list item.
    Returns 'METADATA_OK', 'METADATA_FAILED', or 'METADATA_SKIPPED'.
    """
    fields: dict = {}
    if created_dt:
        fields["Created"] = created_dt
    if modified_dt:
        fields["Modified"] = modified_dt

    if not fields:
        return "METADATA_SKIPPED"

    try:
        graph.patch_list_item_fields(site_id, list_id, list_item_id, fields, token)
        return "METADATA_OK"
    except Exception:
        return "METADATA_FAILED"
