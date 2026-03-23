import time
from typing import Optional

import graph

HASH_RETRY_COUNT = 3
HASH_RETRY_DELAY = 10  # seconds


def compare_file(source: dict, dest: dict) -> tuple[str, str]:
    """
    Compare a source driveItem against a dest driveItem.
    Returns (verify_status, notes).
    """
    source_size = source.get("size", 0)
    dest_size = dest.get("size", 0)

    if source_size != dest_size:
        return "SIZE_MISMATCH", f"source={source_size} dest={dest_size}"

    source_hash = (source.get("file") or {}).get("hashes", {}).get("quickXorHash")
    dest_hash = (dest.get("file") or {}).get("hashes", {}).get("quickXorHash")

    if dest_hash is None:
        return "HASH_PENDING", "quickXorHash not yet computed at destination"

    if source_hash and source_hash != dest_hash:
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
    """
    print("  Verifying...", end="", flush=True)

    # Enumerate dest recursively, keyed by relative path within the batch folder
    dest_files_raw = graph.enumerate_recursive(dest_drive_id, dest_folder_id, "", token)
    dest_by_rel_path: dict[str, dict] = {}
    for f in dest_files_raw:
        # _path comes back as e.g. "/SubFolder/file.pdf" or "file.pdf"
        rel = f["_path"].lstrip("/")
        dest_by_rel_path[rel] = f

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

        # Retry if hash not yet computed
        if status == "HASH_PENDING":
            for _ in range(HASH_RETRY_COUNT):
                time.sleep(HASH_RETRY_DELAY)
                refreshed = graph.get_item(
                    dest_drive_id, dest["id"], token, select="id,size,file"
                )
                status, notes = compare_file(source, refreshed)
                if status != "HASH_PENDING":
                    break

        source["verify_status"] = status
        source["verify_notes"] = notes
        if status != "OK":
            issues += 1

    total = len(source_files)
    if issues == 0:
        print(f" ✓ All {total} files verified OK")
    else:
        print(f" ⚠  {issues}/{total} files have issues")

    return source_files


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
