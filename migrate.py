import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

import auth
import batch as batch_mod
import graph
import prompts
import report
import verify

load_dotenv()

# Module-level settings (set by CLI args)
_parallel_workers = 1


def main() -> None:
    global _parallel_workers
    parser = argparse.ArgumentParser(description="OneDrive → SharePoint Migration Tool")
    parser.add_argument(
        "--verify-only",
        metavar="MANIFEST",
        help="Re-verify all batches from a session manifest JSON (no copying)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        metavar="N",
        default=1,
        help="Run N folder copies concurrently (default: 1, max: 4)",
    )
    args = parser.parse_args()

    # Stash parallel setting for use by _run_batches
    _parallel_workers = min(max(args.parallel, 1), 4)

    if args.verify_only:
        _run_verify_only(args.verify_only)
        return
    print("\n=== OneDrive → SharePoint Migration Tool ===\n")

    # ------------------------------------------------------------------
    # [1] Auth
    # ------------------------------------------------------------------
    print("[1] Sign in")
    token = auth.get_access_token()
    graph.register_token_refresher(auth.get_access_token)

    # ------------------------------------------------------------------
    # Check for incomplete sessions
    # ------------------------------------------------------------------
    incomplete = report.find_incomplete_sessions()
    resumed_manifest = None
    if incomplete:
        resumed_manifest = prompts.prompt_resume_session(incomplete)

    if resumed_manifest:
        _run_resumed_session(resumed_manifest, token)
    else:
        _run_new_session(token)


def _run_new_session(token: str) -> None:
    """Full interactive flow: pick source, dest, scan, run."""

    # ------------------------------------------------------------------
    # [2] Source (OneDrive)
    # ------------------------------------------------------------------
    print("[2] Pick source (OneDrive)")
    upn = prompts.prompt_source_upn()

    print("  Fetching drive...", end="", flush=True)
    drive = graph.get_user_drive(upn, token)
    source_drive_id = drive["id"]
    print(" done")

    print("  Fetching top-level folders...", end="", flush=True)
    top_level = graph.list_children(source_drive_id, "root", token)
    own_folders = _parse_root_items(top_level)
    print(f" done ({len(own_folders)} folders)")

    source_folder_item = prompts.prompt_source_folder(own_folders)

    if source_folder_item.get("_use_root"):
        source_folder_id = "root"
        source_folder_name = drive.get("name", "root")
    elif "_search_query" in source_folder_item:
        query = source_folder_item["_search_query"]
        print(f"  Searching for '{query}'...", end="", flush=True)
        results = graph.search_drive_folders(source_drive_id, query, token)
        print(f" {len(results)} folder(s) found")
        if not results:
            print("  No folders found. Try a different name or use [Enter custom path].")
            sys.exit(1)
        source_folder_item = prompts.prompt_search_result(results)
        source_folder_id = source_folder_item["id"]
        source_folder_name = source_folder_item["name"]
    elif "_custom_path" in source_folder_item:
        print("  Resolving path...", end="", flush=True)
        resolved = graph.get_item_by_path(
            source_drive_id, "/" + source_folder_item["_custom_path"], token
        )
        print(" done")
        if "remoteItem" in resolved:
            remote = resolved["remoteItem"]
            source_folder_item = {
                "id": remote["id"],
                "name": resolved["name"],
                "_drive_id": remote.get("parentReference", {}).get("driveId"),
                "_shared": True,
            }
        else:
            source_folder_item = resolved
        source_folder_id = source_folder_item["id"]
        source_folder_name = source_folder_item["name"]
    else:
        source_folder_id = source_folder_item["id"]
        source_folder_name = source_folder_item["name"]

    if source_folder_item.get("_drive_id"):
        source_drive_id = source_folder_item["_drive_id"]

    # ------------------------------------------------------------------
    # [3] Destination (SharePoint)
    # ------------------------------------------------------------------
    print("\n[3] Pick destination (SharePoint)")
    site_input = prompts.prompt_dest_site()

    if "/" in site_input:
        idx = site_input.index("/")
        hostname = site_input[:idx]
        site_path = site_input[idx:]
    else:
        hostname = site_input
        site_path = ""

    print("  Resolving SharePoint site...", end="", flush=True)
    site = graph.get_site(hostname, site_path, token)
    site_id = site["id"]
    print(" done")

    print("  Fetching document libraries...", end="", flush=True)
    drives = graph.list_site_drives(site_id, token)
    print(" done")

    dest_library = prompts.prompt_dest_library(drives)
    dest_drive_id = dest_library["id"]

    print("  Fetching library root folders...", end="", flush=True)
    dest_root_children = graph.list_children(
        dest_drive_id, "root", token, select="id,name,folder"
    )
    dest_folders = [item for item in dest_root_children if "folder" in item]
    print(" done")

    dest_folder = prompts.prompt_dest_folder(dest_folders)
    dest_root_id = dest_folder["id"] if dest_folder else "root"

    # ------------------------------------------------------------------
    # [4] Scan batches
    # ------------------------------------------------------------------
    print("\n[4] Scanning batches...")
    batches = batch_mod.scan_batches(source_drive_id, source_folder_id, token)

    if not batches:
        print("  No files or folders found in source. Exiting.")
        sys.exit(0)

    if not prompts.confirm_batches(batches):
        print("Aborted.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # [5] Run migration
    # ------------------------------------------------------------------
    print("\n[5] Running migration")

    session_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest = report.init_manifest(
        session_id=session_id,
        source_upn=upn,
        source_folder=source_folder_name,
        dest_site=site_input,
        dest_library=dest_library["name"],
        source_drive_id=source_drive_id,
        source_folder_id=source_folder_id,
        dest_drive_id=dest_drive_id,
        dest_root_id=dest_root_id,
        batch_names=[b["name"] for b in batches],
    )
    manifest_path = report.save_manifest(manifest, source_folder_name, session_id)

    _run_batches(batches, source_drive_id, dest_drive_id, dest_root_id,
                 source_folder_name, manifest, session_id, token)

    report.mark_manifest_completed(manifest)
    report.save_manifest(manifest, source_folder_name, session_id)

    total = sum(b.get("file_count") or 0 for b in batches)
    print(f"\n=== Migration complete — {total} files processed ===")
    print(f"Manifest: {manifest_path}\n")


def _run_resumed_session(manifest: dict, token: str) -> None:
    """Resume an incomplete session — skip all prompts and completed batches."""
    session_id = manifest["session_id"]
    source_folder_name = manifest["source_folder"]
    source_drive_id = manifest["source_drive_id"]
    source_folder_id = manifest["source_folder_id"]
    dest_drive_id = manifest["dest_drive_id"]
    dest_root_id = manifest["dest_root_id"]

    completed_names = {b["batch_name"] for b in manifest.get("batches", [])}

    print(f"\nResuming session {session_id}")
    print(f"  {source_folder_name} → {manifest['dest_library']}")
    print(f"  {len(completed_names)}/{len(manifest.get('batch_names', []))} batches already done\n")

    # Re-scan to get current batch dicts (need item_id etc.)
    print("  Re-scanning batches...", end="", flush=True)
    batches = batch_mod.scan_batches(source_drive_id, source_folder_id, token)
    print(f" {len(batches)} found")

    # Filter to only incomplete batches
    remaining = [b for b in batches if b["name"] not in completed_names]

    if not remaining:
        print("  All batches already completed!")
        report.mark_manifest_completed(manifest)
        report.save_manifest(manifest, source_folder_name, session_id)
        return

    print(f"  {len(remaining)} batch(es) remaining\n")

    _run_batches(remaining, source_drive_id, dest_drive_id, dest_root_id,
                 source_folder_name, manifest, session_id, token)

    report.mark_manifest_completed(manifest)
    manifest_path = report.save_manifest(manifest, source_folder_name, session_id)

    total = sum(b.get("file_count") or 0 for b in remaining)
    print(f"\n=== Resume complete — {total} files processed ===")
    print(f"Manifest: {manifest_path}\n")


def _run_batches(
    batches: list[dict],
    source_drive_id: str,
    dest_drive_id: str,
    dest_root_id: str,
    source_folder_name: str,
    manifest: dict,
    session_id: str,
    token: str,
) -> None:
    """Execute the copy->verify->report loop for a list of batches."""
    if _parallel_workers > 1:
        _run_batches_parallel(
            batches, source_drive_id, dest_drive_id, dest_root_id,
            source_folder_name, manifest, session_id, token,
        )
    else:
        _run_batches_sequential(
            batches, source_drive_id, dest_drive_id, dest_root_id,
            source_folder_name, manifest, session_id, token,
        )


def _run_batches_sequential(
    batches: list[dict],
    source_drive_id: str,
    dest_drive_id: str,
    dest_root_id: str,
    source_folder_name: str,
    manifest: dict,
    session_id: str,
    token: str,
) -> None:
    """Sequential batch execution (default)."""
    session_start = time.monotonic()
    batch_times: list[float] = []

    for idx, b in enumerate(batches):
        _print_session_progress(idx, len(batches), batch_times, session_start)

        batch_start = time.monotonic()
        files = _execute_single_batch(
            b, source_drive_id, dest_drive_id, dest_root_id,
            source_folder_name, manifest, session_id, token,
        )
        batch_times.append(time.monotonic() - batch_start)


def _run_batches_parallel(
    batches: list[dict],
    source_drive_id: str,
    dest_drive_id: str,
    dest_root_id: str,
    source_folder_name: str,
    manifest: dict,
    session_id: str,
    token: str,
) -> None:
    """Parallel batch execution using ThreadPoolExecutor."""
    manifest_lock = threading.Lock()
    session_start = time.monotonic()
    completed_count = 0

    print(f"  Running {len(batches)} batches with {_parallel_workers} workers\n")

    def _worker(b: dict) -> None:
        nonlocal completed_count
        _execute_single_batch(
            b, source_drive_id, dest_drive_id, dest_root_id,
            source_folder_name, manifest, session_id, token,
            manifest_lock=manifest_lock,
        )
        completed_count += 1
        elapsed = _fmt_duration(time.monotonic() - session_start)
        remaining = len(batches) - completed_count
        print(f"\n--- {completed_count}/{len(batches)} done — {elapsed} elapsed — {remaining} remaining ---")

    # Root files batch must run alone (individual file copies, not parallelizable with folders)
    root_batches = [b for b in batches if b["is_root_files"]]
    folder_batches = [b for b in batches if not b["is_root_files"]]

    # Run root files first (sequential)
    for b in root_batches:
        _worker(b)

    # Run folder batches in parallel
    with ThreadPoolExecutor(max_workers=_parallel_workers) as executor:
        futures = {executor.submit(_worker, b): b for b in folder_batches}
        for future in as_completed(futures):
            exc = future.exception()
            if exc:
                b = futures[future]
                print(f"\n  ERROR in batch {b['number']:02d} ({b['name']}): {exc}")


def _execute_single_batch(
    b: dict,
    source_drive_id: str,
    dest_drive_id: str,
    dest_root_id: str,
    source_folder_name: str,
    manifest: dict,
    session_id: str,
    token: str,
    manifest_lock: "threading.Lock | None" = None,
) -> list[dict]:
    """Run copy + verify + report for a single batch. Thread-safe if manifest_lock provided."""
    files = batch_mod.run_batch(
        batch=b,
        source_drive_id=source_drive_id,
        dest_drive_id=dest_drive_id,
        dest_root_id=dest_root_id,
        token=token,
    )

    if not files:
        return []

    if b["is_root_files"]:
        _verify_root_files(files, dest_drive_id, token)
    else:
        dest_batch_folder = _find_dest_folder(
            dest_drive_id, dest_root_id, b["name"], token
        )
        if dest_batch_folder:
            files = verify.fetch_and_compare(
                files, dest_drive_id, dest_batch_folder["id"], token
            )
        else:
            print(f"  Warning: dest folder '{b['name']}' not found — marking all MISSING")
            for f in files:
                f["verify_status"] = "MISSING"
                f["verify_notes"] = "Dest folder not found after copy"

    csv_path = report.write_batch_csv(
        source_folder_name, b["number"], b["name"], files
    )
    print(f"  CSV written: {csv_path}")

    if manifest_lock:
        with manifest_lock:
            report.add_batch_to_manifest(manifest, b["name"], b["number"], files)
            report.save_manifest(manifest, source_folder_name, session_id)
    else:
        report.add_batch_to_manifest(manifest, b["name"], b["number"], files)
        report.save_manifest(manifest, source_folder_name, session_id)

    return files


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_duration(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m"
    return f"{m}m{s:02d}s"


def _print_session_progress(
    current_idx: int,
    total: int,
    batch_times: list[float],
    session_start: float,
) -> None:
    """Print session-level progress: batch N/total, elapsed, estimated remaining."""
    elapsed = time.monotonic() - session_start
    elapsed_str = _fmt_duration(elapsed)

    if batch_times:
        avg_time = sum(batch_times) / len(batch_times)
        remaining = avg_time * (total - current_idx)
        remaining_str = f" — ~{_fmt_duration(remaining)} remaining"
    else:
        remaining_str = ""

    print(f"\n--- Batch {current_idx + 1}/{total} — {elapsed_str} elapsed{remaining_str} ---")


def _parse_root_items(top_level: list[dict]) -> list[dict]:
    """
    Given the raw driveItems from the root children endpoint, return only
    folders and folder shortcuts, normalised into a consistent shape.

    Shortcut items have a remoteItem facet. The folder facet may live on the
    top-level item, inside remoteItem, or (for some Graph responses) neither —
    in that case we still include the item as long as it doesn't have a file
    facet (i.e. assume it is a folder shortcut).
    """
    folders: list[dict] = []
    for item in top_level:
        if "remoteItem" in item:
            remote = item["remoteItem"]
            # Exclude if it's clearly a file shortcut
            if "file" in remote or "file" in item:
                continue
            folders.append({
                "id": remote["id"],
                "name": item["name"],
                "folder": remote.get("folder") or item.get("folder", {}),
                "_drive_id": remote.get("parentReference", {}).get("driveId"),
                "_shared": True,
            })
        elif "folder" in item:
            folders.append(item)
    return folders


def _find_dest_folder(
    dest_drive_id: str,
    dest_root_id: str,
    batch_name: str,
    token: str,
) -> dict | None:
    """Find a child folder in the dest root by name."""
    children = graph.list_children(
        dest_drive_id, dest_root_id, token, select="id,name,folder"
    )
    return next(
        (c for c in children if c.get("name") == batch_name and "folder" in c),
        None,
    )


def _run_verify_only(manifest_path: str) -> None:
    """Re-verify all batches from an existing session manifest (no copying)."""
    path = Path(manifest_path)
    if not path.exists():
        print(f"Error: manifest not found: {manifest_path}")
        sys.exit(1)

    with open(path, encoding="utf-8") as fh:
        manifest = json.load(fh)

    print(f"\n=== Verify-Only Mode ===")
    print(f"Session: {manifest['session_id']}")
    print(f"Source: {manifest['source_folder']} → {manifest['dest_library']}\n")

    print("[1] Sign in")
    token = auth.get_access_token()
    graph.register_token_refresher(auth.get_access_token)

    dest_drive_id = manifest["dest_drive_id"]
    dest_root_id = manifest["dest_root_id"]
    source_folder_name = manifest["source_folder"]
    session_id = manifest["session_id"]

    total_issues = 0
    total_files = 0

    for batch_entry in manifest.get("batches", []):
        batch_name = batch_entry["batch_name"]
        batch_num = batch_entry["batch_number"]
        source_files = batch_entry.get("files", [])

        if not source_files:
            continue

        print(f"\n> Batch {batch_num:02d} — {batch_name} ({len(source_files)} files)")

        # Reconstruct source dicts with the fields verify needs
        source_dicts = []
        for f in source_files:
            item = {
                "id": f.get("source_id"),
                "_path": f.get("source_path", ""),
                "name": f.get("source_path", "").rsplit("/", 1)[-1],
                "size": f.get("size", 0),
                "file": {"hashes": {"quickXorHash": f.get("quickXorHash", "")}} if f.get("quickXorHash") else {},
            }
            source_dicts.append(item)

        # Find dest folder
        dest_batch_folder = _find_dest_folder(dest_drive_id, dest_root_id, batch_name, token)
        if dest_batch_folder:
            source_dicts = verify.fetch_and_compare(
                source_dicts, dest_drive_id, dest_batch_folder["id"], token
            )
        else:
            print(f"  Warning: dest folder '{batch_name}' not found")
            for f in source_dicts:
                f["verify_status"] = "MISSING"
                f["verify_notes"] = "Dest folder not found"

        # Write updated CSV
        csv_path = report.write_batch_csv(
            source_folder_name, batch_num, batch_name, source_dicts
        )
        print(f"  CSV written: {csv_path}")

        ok_statuses = {"OK", "OK_SP_OVERHEAD", "OK_IMAGE_META"}
        batch_issues = sum(1 for f in source_dicts if f.get("verify_status") not in ok_statuses)
        total_issues += batch_issues
        total_files += len(source_dicts)

    print(f"\n=== Verification complete — {total_files} files, {total_issues} issues ===\n")


def _verify_root_files(files: list[dict], dest_drive_id: str, token: str) -> None:
    """Verify individually copied root files using their dest_resource_id."""
    for f in files:
        if f.get("copy_status") == "COPY_FAILED":
            f["verify_status"] = "COPY_FAILED"
            continue

        dest_id = f.get("dest_resource_id")
        if not dest_id:
            f["verify_status"] = "MISSING"
            f["verify_notes"] = "No dest resource ID recorded after copy"
            continue

        dest_item = graph.get_item(dest_drive_id, dest_id, token, select="id,size,file")
        status, notes = verify.compare_file(f, dest_item)
        f["verify_status"] = status
        f["verify_notes"] = notes
        f["dest_id"] = dest_id


if __name__ == "__main__":
    main()
