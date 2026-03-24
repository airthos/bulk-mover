import json
import os
import time
from pathlib import Path
from typing import Optional

import graph

DELTA_DIR = Path("migration-logs/delta")

HASH_RETRY_COUNT = 3
HASH_RETRY_DELAY = 10  # seconds

# SharePoint's content pipeline injects co-authoring XML into Office docs,
# adding ~6-8 KB. Size mismatches within this range are expected.
_OFFICE_EXTENSIONS = frozenset({".docx", ".xlsx", ".pptx", ".doc", ".xls", ".ppt"})
_SP_OVERHEAD_MAX = 10_000  # bytes

# SharePoint rewrites image metadata (EXIF, ICC, tEXt chunks) on ingestion.
# Same-size + different-hash on images is benign — pixel data is intact.
_IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".tiff", ".tif", ".heic", ".gif", ".bmp"})


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
        # Office docs: SharePoint adds co-authoring XML — dest is slightly larger
        if ext in _OFFICE_EXTENSIONS and 0 < (dest_size - source_size) <= _SP_OVERHEAD_MAX:
            return "OK_SP_OVERHEAD", f"dest +{dest_size - source_size}B (Office co-authoring XML)"
        return "SIZE_MISMATCH", f"source={source_size} dest={dest_size}"

    source_hash = (source.get("file") or {}).get("hashes", {}).get("quickXorHash")
    dest_hash = (dest.get("file") or {}).get("hashes", {}).get("quickXorHash")

    if dest_hash is None:
        return "HASH_PENDING", "quickXorHash not yet computed at destination"

    if source_hash and source_hash != dest_hash:
        # Images: SharePoint rewrites ancillary metadata — hash mismatch is expected
        if ext in _IMAGE_EXTENSIONS:
            return "OK_IMAGE_META", f"hash differs (image metadata rewrite)"
        return "HASH_MISMATCH", f"source={source_hash} dest={dest_hash}"

    return "OK", ""


def fetch_and_compare(
    source_files: list[dict],
    dest_drive_id: str,
    dest_folder_id: str,
    token: str,
) -> list[dict]:
    """
    Enumerate the destination batch folder, match files against the source
    manifest by relative path, and annotate each source file with
    verify_status, verify_notes, dest_id, and dest_sharepointIds.

    Uses JSON batching ($batch) for HASH_PENDING retries to reduce roundtrips.
    """
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
    return DELTA_DIR / f"{drive_id}.deltalink.json"


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
    DELTA_DIR.mkdir(parents=True, exist_ok=True)
    _delta_link_path(drive_id).write_text(json.dumps({"deltaLink": delta_link}))


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
