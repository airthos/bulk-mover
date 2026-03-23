import graph
import verify
import report


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------

def scan_batches(drive_id: str, folder_id: str, token: str) -> list[dict]:
    """
    Enumerate one level deep inside the selected root folder.
    Each immediate child folder = one batch.
    Files sitting at the root = one "Root files" batch.

    Uses folder.childCount for the preview (fast — no recursion at scan time).
    Full recursive enumeration happens when each batch runs.
    """
    children = graph.list_children(
        drive_id, folder_id, token,
        select="id,name,size,file,folder",
    )

    batches: list[dict] = []
    root_files: list[dict] = []

    for item in children:
        if "folder" in item:
            batches.append({
                "name": item["name"],
                "item_id": item["id"],
                "is_root_files": False,
                "child_count": item.get("folder", {}).get("childCount", "?"),
                "file_count": None,  # filled during run
            })
        else:
            root_files.append(item)

    if root_files:
        batches.append({
            "name": "Root files",
            "item_id": None,
            "is_root_files": True,
            "child_count": len(root_files),
            "file_count": len(root_files),
            "_files_cache": root_files,
        })

    for i, b in enumerate(batches, start=1):
        b["number"] = i

    return batches


# ---------------------------------------------------------------------------
# Running a batch
# ---------------------------------------------------------------------------

def run_batch(
    batch: dict,
    source_drive_id: str,
    dest_drive_id: str,
    dest_root_id: str,
    token: str,
) -> list[dict]:
    """
    Execute one batch end-to-end:
      1. Enumerate source files (build manifest truth)
      2. Copy (folder copy or per-file for root files)
      3. Return annotated file list (verify happens in migrate.py after)

    Returns the list of source file dicts annotated with copy_status.
    """
    batch_name = batch["name"]
    batch_num = batch["number"]

    print(f"\n> Batch {batch_num:02d} — {batch_name}")

    # --- Enumerate source ---
    if batch.get("_files_cache") is not None:
        source_files = batch["_files_cache"]
    elif batch["is_root_files"]:
        source_files = batch.get("_files_cache", [])
    else:
        print("  Enumerating source...", end="", flush=True)
        source_files = graph.enumerate_recursive(
            source_drive_id, batch["item_id"], batch_name, token
        )
        print(f" {len(source_files)} files")

    batch["file_count"] = len(source_files)

    if not source_files:
        print("  No files found — skipping")
        return []

    # --- Copy ---
    if batch["is_root_files"]:
        _copy_root_files(source_files, source_drive_id, dest_drive_id, dest_root_id, token)
    else:
        _copy_folder(batch, source_drive_id, dest_drive_id, dest_root_id, source_files, token)

    return source_files


def _copy_folder(
    batch: dict,
    source_drive_id: str,
    dest_drive_id: str,
    dest_root_id: str,
    source_files: list[dict],
    token: str,
) -> None:
    """Trigger a single folder copy and poll to completion."""
    print(f"  Copying {len(source_files)} files...", end="", flush=True)

    try:
        location = graph.copy_item(
            source_drive_id, batch["item_id"], dest_drive_id, dest_root_id, token
        )
    except Exception as e:
        print(f" ERROR: {e}")
        _mark_all(source_files, "COPY_FAILED", str(e))
        return

    result = graph.poll_copy_job(location)
    status = result.get("status")

    if status == "completed":
        print(" done.")
        for f in source_files:
            f["copy_status"] = "COMPLETED"

    elif status == "timeout":
        print(" TIMED OUT after 10 min.")
        _mark_all(source_files, "COPY_FAILED", "Polling timed out after 10 minutes")

    else:
        error = result.get("error", {})
        msg = f"{error.get('code', 'unknown')}: {error.get('message', status)}"
        print(f" FAILED — {msg}")
        _mark_all(source_files, "COPY_FAILED", msg)


def _copy_root_files(
    source_files: list[dict],
    source_drive_id: str,
    dest_drive_id: str,
    dest_root_id: str,
    token: str,
) -> None:
    """Copy individual files (no enclosing folder). Used for root-level files."""
    from tqdm import tqdm

    for f in tqdm(source_files, desc="  Root files", unit="file"):
        try:
            location = graph.copy_item(
                source_drive_id, f["id"], dest_drive_id, dest_root_id, token
            )
            result = graph.poll_copy_job(location)
            if result.get("status") == "completed":
                f["copy_status"] = "COMPLETED"
                f["dest_resource_id"] = result.get("resourceId")
            else:
                error = result.get("error", {})
                msg = f"{error.get('code', result.get('status', 'unknown'))}: {error.get('message', '')}"
                f["copy_status"] = "COPY_FAILED"
                f["copy_notes"] = msg
        except Exception as e:
            f["copy_status"] = "COPY_FAILED"
            f["copy_notes"] = str(e)


def _mark_all(files: list[dict], status: str, notes: str) -> None:
    for f in files:
        f["copy_status"] = status
        f["copy_notes"] = notes
