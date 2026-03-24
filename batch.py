import json
from pathlib import Path

import graph
import verify
import report

PENDING_JOBS_FILE = Path("migration-logs/pending-jobs.json")


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
        for f in source_files:
            if "_path" not in f:
                f["_path"] = f.get("name", "")
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
    batch_name = batch["name"]

    # --- Verify-then-skip: if dest folder already exists with matching content, skip copy ---
    if _verify_already_copied(batch_name, source_files, dest_drive_id, dest_root_id, token):
        return

    # Check for a saved pending job from a previous timed-out run
    location = _load_pending_job(batch_name)
    if location:
        print(f"  Resuming poll for {len(source_files)} files (saved job)...", end="", flush=True)
    else:
        print(f"  Copying {len(source_files)} files...", end="", flush=True)
        try:
            location = graph.copy_item(
                source_drive_id, batch["item_id"], dest_drive_id, dest_root_id, token
            )
        except Exception as e:
            print(f" ERROR: {e}")
            _mark_all(source_files, "COPY_FAILED", str(e))
            return

    def _progress(pct, elapsed):
        mins = int(elapsed) // 60
        secs = int(elapsed) % 60
        print(f"\r  Copying {len(source_files)} files... {pct:.0f}% ({mins}m{secs:02d}s)", end="", flush=True)

    result = graph.poll_copy_job(location, progress_callback=_progress)
    status = result.get("status")

    if status == "completed":
        _clear_pending_job(batch_name)
        print(" done.")
        for f in source_files:
            f["copy_status"] = "COMPLETED"

    elif status == "timeout":
        _save_pending_job(batch_name, result.get("_location", location))
        print(f" TIMED OUT — location URL saved for resume.")
        _mark_all(source_files, "COPY_FAILED", "Polling timed out (URL saved for resume)")

    else:
        _clear_pending_job(batch_name)
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


def _verify_already_copied(
    batch_name: str,
    source_files: list[dict],
    dest_drive_id: str,
    dest_root_id: str,
    token: str,
) -> bool:
    """
    Check if the batch folder already exists at the destination with all files
    matching. If so, mark all source files as COMPLETED and return True.
    This prevents re-copying on re-runs of timed-out or interrupted batches.
    """
    # Find the dest folder by name
    children = graph.list_children(
        dest_drive_id, dest_root_id, token, select="id,name,folder"
    )
    dest_folder = next(
        (c for c in children if c.get("name") == batch_name and "folder" in c),
        None,
    )
    if not dest_folder:
        return False

    # Enumerate dest and compare
    print(f"  Dest folder exists — verifying before re-copy...", end="", flush=True)
    dest_files_raw = graph.enumerate_recursive(
        dest_drive_id, dest_folder["id"], "", token
    )
    dest_by_rel_path: dict[str, dict] = {}
    for f in dest_files_raw:
        dest_by_rel_path[f["_path"].lstrip("/")] = f

    all_ok = True
    for source in source_files:
        source_path = source["_path"]
        parts = source_path.split("/", 1)
        rel_path = parts[1] if len(parts) > 1 else parts[0]

        dest = dest_by_rel_path.get(rel_path)
        if dest is None:
            all_ok = False
            break

        status, _ = verify.compare_file(source, dest)
        if status not in ("OK", "OK_SP_OVERHEAD", "OK_IMAGE_META"):
            all_ok = False
            break

    if all_ok:
        print(f" all {len(source_files)} files already present — skipping copy.")
        for f in source_files:
            f["copy_status"] = "COMPLETED"
        return True

    print(f" incomplete — will re-copy.")
    return False


def _mark_all(files: list[dict], status: str, notes: str) -> None:
    for f in files:
        f["copy_status"] = status
        f["copy_notes"] = notes


# ---------------------------------------------------------------------------
# Pending-job persistence (for resuming timed-out copy polls)
# ---------------------------------------------------------------------------

def _load_pending_jobs() -> dict:
    if PENDING_JOBS_FILE.exists():
        try:
            return json.loads(PENDING_JOBS_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_pending_jobs(jobs: dict) -> None:
    PENDING_JOBS_FILE.parent.mkdir(exist_ok=True)
    PENDING_JOBS_FILE.write_text(json.dumps(jobs, indent=2))


def _load_pending_job(batch_name: str) -> str | None:
    return _load_pending_jobs().get(batch_name)


def _save_pending_job(batch_name: str, location: str) -> None:
    jobs = _load_pending_jobs()
    jobs[batch_name] = location
    _save_pending_jobs(jobs)


def _clear_pending_job(batch_name: str) -> None:
    jobs = _load_pending_jobs()
    if batch_name in jobs:
        del jobs[batch_name]
        _save_pending_jobs(jobs)
