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

# Module-level settings (set at runtime)
_parallel_override: "int | None" = None  # None = auto-select from batch count
_tui: "BatchTUI | None" = None


def main() -> None:
    global _parallel_override
    parser = argparse.ArgumentParser(description="OneDrive → SharePoint Migration Tool")
    parser.add_argument(
        "--verify-only",
        metavar="MANIFEST",
        help="Re-verify all batches from a session manifest JSON via delta walk (no copying)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        metavar="N",
        default=None,
        help="Override parallel worker count (default: auto, up to 6)",
    )
    parser.add_argument(
        "--resync",
        metavar="MANIFEST",
        help="Re-sync source OneDrive against dest SharePoint from a session manifest; copies only changed files",
    )
    args = parser.parse_args()

    if args.parallel is not None:
        _parallel_override = min(max(args.parallel, 1), 6)

    print("\n=== OneDrive → SharePoint Migration Tool ===\n")

    # Flag-based dispatch (scripting / CI use)
    if args.resync:
        path = Path(args.resync)
        if not path.exists():
            print(f"Error: manifest not found: {args.resync}")
            sys.exit(1)
        with open(path, encoding="utf-8") as fh:
            manifest = json.load(fh)
        token = auth.get_access_token()
        graph.register_token_refresher(auth.get_access_token)
        _run_resync(manifest, token)
        return

    if args.verify_only:
        path = Path(args.verify_only)
        if not path.exists():
            print(f"Error: manifest not found: {args.verify_only}")
            sys.exit(1)
        with open(path, encoding="utf-8") as fh:
            manifest = json.load(fh)
        token = auth.get_access_token()
        graph.register_token_refresher(auth.get_access_token)
        _verify_session(manifest, token)
        return

    # Read cached UPN before any network calls (reads token_cache.json only)
    default_upn = auth.get_signed_in_upn() or ""

    # Scan sessions from disk — no network needed
    incomplete = report.find_incomplete_sessions()
    all_sessions = report.find_all_sessions()

    # Always show startup menu (Deep Verify manual is always available)
    action, selected_manifest = prompts.prompt_startup_action(incomplete, all_sessions)

    print("[Auth] Sign in")
    token = auth.get_access_token()
    graph.register_token_refresher(auth.get_access_token)
    # Refresh default UPN in case auth updated the cache
    default_upn = auth.get_signed_in_upn() or default_upn

    if action == "resume":
        _run_resumed_session(selected_manifest, token)
    elif action == "verify":
        _verify_session(selected_manifest, token)
    elif action == "verify_manual":
        _verify_adhoc(token, default_upn=default_upn)
    else:
        _run_new_session(token, default_upn=default_upn)


def _pick_source(
    token: str, default_upn: str = "", step_prefix: str = ""
) -> tuple[str, str, str, str]:
    """
    Interactive source selection. Returns (source_drive_id, source_folder_id,
    source_folder_name, upn).
    """
    print(f"{step_prefix}Pick source (OneDrive)")
    if default_upn:
        upn = default_upn
        print(f"  Using signed-in account: {upn}")
    else:
        upn = prompts.prompt_source_upn()

    print("  Fetching drive...", end="", flush=True)
    drive = graph.get_user_drive(upn, token)
    source_drive_id = drive["id"]
    print(" done")

    print("  Fetching top-level folders...", end="", flush=True)
    top_level = graph.list_children(source_drive_id, "root", token)
    own_folders = _parse_root_items(top_level)
    print(f" done ({len(own_folders)} folders)")

    while True:
        source_folder_item = prompts.prompt_source_folder(own_folders)

        if source_folder_item.get("_use_root"):
            source_folder_id = "root"
            source_folder_name = drive.get("name", "root")
            break
        elif "_search_query" in source_folder_item:
            query = source_folder_item["_search_query"]
            print(f"  Searching for '{query}'...", end="", flush=True)
            results = graph.search_drive_folders(source_drive_id, query, token)
            print(f" {len(results)} folder(s) found")
            if not results:
                print("  No folders found — returning to folder list.")
                continue
            source_folder_item = prompts.prompt_search_result(results)
            source_folder_id = source_folder_item["id"]
            source_folder_name = source_folder_item["name"]
            break
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
            break
        else:
            source_folder_id = source_folder_item["id"]
            source_folder_name = source_folder_item["name"]
            break

    if source_folder_item.get("_drive_id"):
        source_drive_id = source_folder_item["_drive_id"]

    return source_drive_id, source_folder_id, source_folder_name, upn


def _pick_dest(
    token: str, step_prefix: str = ""
) -> tuple[str, str, str, str]:
    """
    Interactive destination selection. Returns (dest_drive_id, dest_root_id,
    site_input, dest_library_name).
    """
    print(f"\n{step_prefix}Pick destination (SharePoint)")
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

    return dest_drive_id, dest_root_id, site_input, dest_library["name"]


def _run_new_session(token: str, default_upn: str = "") -> None:
    """Full interactive flow: pick source, dest, scan, run."""

    source_drive_id, source_folder_id, source_folder_name, upn = _pick_source(
        token, default_upn, step_prefix="[2] "
    )
    dest_drive_id, dest_root_id, site_input, dest_library_name = _pick_dest(
        token, step_prefix="[3] "
    )

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
        dest_library=dest_library_name,
        source_drive_id=source_drive_id,
        source_folder_id=source_folder_id,
        dest_drive_id=dest_drive_id,
        dest_root_id=dest_root_id,
        batch_names=[b["name"] for b in batches],
    )
    sdir = report.session_dir(session_id, source_folder_name)
    batch_mod.set_session_dir(sdir)
    manifest_path = report.save_manifest(manifest, source_folder_name, session_id)

    _run_batches(batches, source_drive_id, dest_drive_id, dest_root_id,
                 source_folder_name, manifest, session_id, token,
                 dest_library_name=dest_library_name,
                 total_batches=len(batches),
                 parallel_override=_parallel_override)

    report.mark_manifest_completed(manifest)
    report.save_manifest(manifest, source_folder_name, session_id)

    total = sum(b.get("file_count") or 0 for b in batches)
    print(f"\n=== Migration complete — {total} files processed ===")
    print(f"Manifest: {manifest_path}\n")


def _run_resumed_session(manifest: dict, token: str) -> None:
    """Resume an incomplete session — skip all prompts and completed batches."""
    session_id = manifest.get("session_id") or "legacy"
    source_folder_name = manifest.get("source_folder", "unknown")
    source_drive_id = manifest.get("source_drive_id", "")
    source_folder_id = manifest.get("source_folder_id", "")
    dest_drive_id = manifest.get("dest_drive_id", "")
    dest_root_id = manifest.get("dest_root_id") or "root"

    if not source_folder_id or not source_drive_id or not dest_drive_id:
        print("  Error: session manifest is missing required IDs — cannot resume copy.")
        print("  Use verify mode instead.")
        return

    completed_names = {b["batch_name"] for b in manifest.get("batches", [])}

    print(f"\nResuming session {session_id}")
    print(f"  {source_folder_name} → {manifest.get('dest_library', '?')}")
    print(f"  {len(completed_names)}/{len(manifest.get('batch_names', []))} batches already done\n")

    # Re-scan to get current batch dicts (need item_id etc.)
    print("  Re-scanning batches...", end="", flush=True)
    batches = batch_mod.scan_batches(source_drive_id, source_folder_id, token)
    print(f" {len(batches)} found")

    # Point session dir at the existing session folder
    sdir = report.session_dir(session_id, source_folder_name)
    batch_mod.set_session_dir(sdir)

    # Filter to only incomplete batches
    remaining = [b for b in batches if b["name"] not in completed_names]

    if not remaining:
        print("  All batches already completed!")
        report.mark_manifest_completed(manifest)
        report.save_manifest(manifest, source_folder_name, session_id)
        return

    print(f"  {len(remaining)} batch(es) remaining\n")

    _run_batches(remaining, source_drive_id, dest_drive_id, dest_root_id,
                 source_folder_name, manifest, session_id, token,
                 dest_library_name=manifest.get("dest_library", ""),
                 total_batches=len(manifest["batch_names"]) if manifest.get("batch_names") else len(batches),
                 is_resuming=True,
                 parallel_override=_parallel_override)

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
    dest_library_name: str = "",
    total_batches: int = 0,
    is_resuming: bool = False,
    parallel_override: int | None = None,
) -> None:
    """Execute the copy->verify->report loop for a list of batches."""
    global _tui
    total = total_batches or len(batches)

    root_batches = [b for b in batches if b["is_root_files"]]
    folder_batches = [b for b in batches if not b["is_root_files"]]

    # Auto-select worker count (1 per folder batch, capped at 6); --parallel overrides
    if parallel_override is not None:
        n_workers = parallel_override
    else:
        n_workers = min(max(len(folder_batches), 1), 6)
    n_workers = max(n_workers, 1)

    manifest_lock = threading.Lock()

    from tui import BatchTUI
    initial_completed = total - len(batches)  # already-done batches on a resume
    tui = BatchTUI(
        source=source_folder_name,
        dest=dest_library_name or "SharePoint",
        total_batches=total,
        n_workers=n_workers,
        initial_completed=max(initial_completed, 0),
    )
    _tui = tui
    batch_mod.set_tui(tui)

    # copy_results holds (batch, files) for Phase 2 verification
    copy_results: list[tuple[dict, list[dict]]] = []
    copy_lock = threading.Lock()

    with tui:
        # ------------------------------------------------------------------
        # Phase 1: Copy all batches (parallel)
        # ------------------------------------------------------------------
        def _copy_worker(b: dict) -> None:
            files = batch_mod.run_batch(
                batch=b,
                source_drive_id=source_drive_id,
                dest_drive_id=dest_drive_id,
                dest_root_id=dest_root_id,
                token=token,
            )
            with copy_lock:
                copy_results.append((b, files))
            if tui:
                if files:
                    tui.update(b["name"], "copied", file_count=len(files))
                else:
                    tui.complete(b["name"], ok_count=0, issue_count=0)

        for b in root_batches:
            _copy_worker(b)

        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = {executor.submit(_copy_worker, b): b for b in folder_batches}
            for future in as_completed(futures):
                exc = future.exception()
                if exc:
                    b = futures[future]
                    tui._plain(f"ERROR copying batch {b['number']:02d} ({b['name']}): {exc}")

        # ------------------------------------------------------------------
        # Phase 2: Verify all batches (sequential, after all copies done)
        # SP has had time to process files during Phase 1.
        # ------------------------------------------------------------------
        for b, files in copy_results:
            if not files:
                continue  # empty batch — already completed above

            if tui:
                tui.update(b["name"], "verifying", file_count=len(files))

            if b["is_root_files"]:
                _verify_root_files(files, dest_drive_id, dest_root_id, token)
            else:
                dest_batch_folder = _find_dest_folder(
                    dest_drive_id, dest_root_id, b["name"], token
                )
                if dest_batch_folder:
                    files = verify.fetch_and_compare(
                        files, dest_drive_id, dest_batch_folder["id"], token,
                        quiet=True,
                    )
                else:
                    for f in files:
                        f["verify_status"] = "MISSING"
                        f["verify_notes"] = "Dest folder not found after copy"

            report.write_batch_csv(
                source_folder_name, b["number"], b["name"], files, session_id
            )

            with manifest_lock:
                report.add_batch_to_manifest(
                    manifest, b["name"], b["number"], files,
                    source_item_id=b.get("item_id", ""),
                )
                report.save_manifest(manifest, source_folder_name, session_id)

            if tui:
                ok_count = sum(1 for f in files if f.get("verify_status") in _OK_STATUSES)
                tui.complete(b["name"], ok_count=ok_count, issue_count=len(files) - ok_count)

    batch_mod.set_tui(None)
    _tui = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_duration(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m"
    return f"{m}m{s:02d}s"



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


def _manifest_to_source_dicts(files: list[dict]) -> list[dict]:
    """Convert manifest batch file entries to the driveItem-like dicts verify expects."""
    return [
        {
            "id": f.get("source_id"),
            "_path": f.get("source_path", ""),
            "name": f.get("source_path", "").rsplit("/", 1)[-1],
            "size": f.get("size", 0),
            "file": {"hashes": {"quickXorHash": f.get("quickXorHash", "")}} if f.get("quickXorHash") else {},
        }
        for f in files
    ]


_OK_STATUSES = {"OK", "OK_SP_OVERHEAD", "OK_IMAGE_META"}


def _verify_session(manifest: dict, token: str) -> None:
    """
    Verify all batches from a session manifest via delta walk (no copying).
    Enumerates ALL destination files in one delta pass, then matches every source
    file from the manifest. Works for both --verify-only and the interactive menu.
    """
    dest_drive_id = manifest.get("dest_drive_id", "")
    dest_root_id = manifest.get("dest_root_id") or "root"
    source_folder_name = manifest.get("source_folder", "unknown")
    session_id = manifest.get("session_id") or "legacy"

    if not dest_drive_id:
        print("  Error: session manifest is missing dest_drive_id — cannot verify.")
        return

    print(f"\n=== Verify Mode ===")
    print(f"Session: {session_id}")
    print(f"Source: {source_folder_name} → {manifest.get('dest_library', '?')}\n")

    run_dir = report.deep_verify_run_dir(session_id, source_folder_name)
    run_dir.mkdir(parents=True, exist_ok=True)

    # Resolve "root" alias — Graph never returns it as a parentReference.id
    delta_root_id = dest_root_id
    if delta_root_id == "root":
        delta_root_id = graph.get_item(dest_drive_id, "root", token, select="id")["id"]

    print("  Enumerating destination via delta...", end="", flush=True)
    items, _delta_link = graph.get_folder_delta(
        dest_drive_id, delta_root_id, token,
        select="id,name,size,file,parentReference,deleted",
    )
    dest_lookup = verify.build_dest_path_lookup(items, delta_root_id)
    print(f" {len(dest_lookup)} files indexed")

    total_issues = 0
    total_files = 0
    all_source_dicts: list[dict] = []

    for batch_entry in manifest.get("batches", []):
        batch_name = batch_entry.get("batch_name", "")
        batch_num = batch_entry.get("batch_number", 0)
        source_files = batch_entry.get("files", [])

        if not source_files:
            continue

        print(f"\n> Batch {batch_num:02d} — {batch_name} ({len(source_files)} files)")
        source_dicts = _manifest_to_source_dicts(source_files)
        source_dicts = verify.compare_from_lookup(
            source_dicts, dest_lookup, dest_drive_id, token
        )
        all_source_dicts.extend(source_dicts)

        csv_path = report.write_batch_csv(
            source_folder_name, batch_num, batch_name, source_dicts, run_dir=run_dir
        )
        print(f"  CSV written: {csv_path}")

        batch_issues = sum(1 for f in source_dicts if f.get("verify_status") not in _OK_STATUSES)
        total_issues += batch_issues
        total_files += len(source_dicts)

    dest_only = verify.find_dest_only(all_source_dicts, dest_lookup)
    if dest_only:
        dest_only_csv = report.write_dest_only_csv(dest_only, run_dir)
        print(f"\n  ⚠  {len(dest_only)} file(s) at destination not in source manifest")
        print(f"  DEST_ONLY CSV: {dest_only_csv}")
    else:
        print(f"\n  ✓ No destination-only files")

    print(f"\n=== Verification complete — {total_files} files, {total_issues} issues ===")
    print(f"Results: {run_dir}\n")


def _verify_adhoc(token: str, default_upn: str = "") -> None:
    """
    Manual verify: pick source and dest interactively, no session required.
    Enumerates source files recursively, enumerates dest via delta, then compares.
    Groups results by top-level subfolder (one CSV per group).
    """
    print("\n=== Verify (manual) ===\n")

    source_drive_id, source_folder_id, source_folder_name, _upn = _pick_source(
        token, default_upn, step_prefix="[1] "
    )
    dest_drive_id, dest_root_id, _site, _lib = _pick_dest(token, step_prefix="[2] ")

    # Enumerate source
    print(f"\n[3] Enumerating source files...")
    source_files = graph.enumerate_recursive(source_drive_id, source_folder_id, "", token)
    if not source_files:
        print("  No files found in source folder. Exiting.")
        return
    print(f"  {len(source_files)} files found")

    # Enumerate dest via delta
    delta_root_id = dest_root_id
    if delta_root_id == "root":
        delta_root_id = graph.get_item(dest_drive_id, "root", token, select="id")["id"]

    print("  Enumerating destination via delta...", end="", flush=True)
    items, _delta_link = graph.get_folder_delta(
        dest_drive_id, delta_root_id, token,
        select="id,name,size,file,parentReference,deleted",
    )
    dest_lookup = verify.build_dest_path_lookup(items, delta_root_id)
    print(f" {len(dest_lookup)} files indexed")

    # Group source files by top-level subfolder for per-group CSV output
    from collections import defaultdict
    groups: dict[str, list] = defaultdict(list)
    for f in source_files:
        top = f["_path"].split("/")[0] if "/" in f["_path"] else "(root files)"
        groups[top].append(f)

    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    run_dir = report.LOGS_DIR / f"{run_id}_verify_{report._safe(source_folder_name)}"
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[4] Verifying {len(source_files)} files in {len(groups)} group(s)...")
    total_issues = 0
    total_files = 0

    for batch_num, group_name in enumerate(sorted(groups.keys()), 1):
        files = groups[group_name]
        print(f"\n> {group_name} ({len(files)} files)")
        verified = verify.compare_from_lookup(files, dest_lookup, dest_drive_id, token)
        csv_path = report.write_batch_csv(
            source_folder_name, batch_num, group_name, verified,
            run_dir=run_dir,
        )
        print(f"  CSV written: {csv_path}")
        total_issues += sum(1 for f in verified if f.get("verify_status") not in _OK_STATUSES)
        total_files += len(verified)

    dest_only = verify.find_dest_only(source_files, dest_lookup)
    if dest_only:
        dest_only_csv = report.write_dest_only_csv(dest_only, run_dir)
        print(f"\n  ⚠  {len(dest_only)} file(s) at destination not in source")
        print(f"  DEST_ONLY CSV: {dest_only_csv}")
    else:
        print(f"\n  ✓ No destination-only files")

    print(f"\n=== Verification complete — {total_files} files, {total_issues} issues ===")
    print(f"Results: {run_dir}\n")


def _verify_root_files(
    files: list[dict], dest_drive_id: str, dest_root_id: str, token: str
) -> None:
    """Verify individually copied root files using their dest_resource_id.

    Falls back to a name-based lookup in dest root when dest_resource_id is
    absent (Graph copy job response doesn't always include resourceId).
    The name lookup is fetched lazily and reused across all files.
    """
    dest_root_by_name: dict[str, dict] | None = None

    for f in files:
        if f.get("copy_status") == "COPY_FAILED":
            f["verify_status"] = "COPY_FAILED"
            continue

        dest_id = f.get("dest_resource_id")

        if not dest_id:
            # Fetch dest root children once and match by filename
            if dest_root_by_name is None:
                children = graph.list_children(
                    dest_drive_id, dest_root_id, token, select="id,name,size,file"
                )
                dest_root_by_name = {c["name"]: c for c in children if "file" in c}
            name = f.get("name") or f.get("_path", "").rsplit("/", 1)[-1]
            dest_item = dest_root_by_name.get(name)
            if not dest_item:
                f["verify_status"] = "MISSING"
                f["verify_notes"] = "Not found at destination (no resourceId; name lookup failed)"
                continue
            dest_id = dest_item["id"]
        else:
            dest_item = graph.get_item(dest_drive_id, dest_id, token, select="id,size,file")

        status, notes = verify.compare_file(f, dest_item)
        f["verify_status"] = status
        f["verify_notes"] = notes
        f["dest_id"] = dest_id


def _run_resync(manifest: dict, token: str) -> None:
    """
    Re-sync source OneDrive against dest SharePoint from a completed session manifest.

    Per batch:
      - Re-enumerate source files (fresh full scan)
      - Diff against the manifest's stored file list (by path, size, lastModifiedDateTime)
      - If any files added or modified: re-copy the entire batch folder with conflict_behavior="replace"
      - Log added/modified files to per-batch CSV; log removed files to a single removals CSV
      - Persist source_item_id per batch for subsequent resyncs

    Removals (files deleted from OneDrive) are only logged — nothing is deleted from SharePoint.
    """
    source_drive_id = manifest.get("source_drive_id", "")
    source_folder_id = manifest.get("source_folder_id", "")
    source_folder_name = manifest.get("source_folder", "unknown")
    dest_drive_id = manifest.get("dest_drive_id", "")
    dest_root_id = manifest.get("dest_root_id") or "root"
    session_id = manifest.get("session_id") or "legacy"

    print(f"\n=== Resync ===")
    print(f"Session: {session_id}")
    print(f"Source: {source_folder_name} → {manifest.get('dest_library', '?')}\n")

    if not source_drive_id or not dest_drive_id:
        print("  Error: manifest missing source or dest drive IDs — cannot resync.")
        return

    run_dir = report.resync_run_dir(session_id, source_folder_name)
    run_dir.mkdir(parents=True, exist_ok=True)

    # Shallow re-scan to get current item_ids (stable IDs, fast one-level scan)
    print("  Re-scanning source batches...", end="", flush=True)
    current_batches = batch_mod.scan_batches(source_drive_id, source_folder_id, token)
    current_batch_map = {b["name"]: b for b in current_batches}
    print(f" {len(current_batches)} found")

    all_removed: list[dict] = []
    total_changed_batches = 0

    for batch_entry in manifest.get("batches", []):
        batch_name = batch_entry.get("batch_name", "")
        batch_number = batch_entry.get("batch_number", 0)
        stored_files = batch_entry.get("files", [])

        # Root files batch uses per-file copy — skip for now (folder copy not applicable)
        if not stored_files or not batch_entry.get("source_item_id") and batch_name == "Root files":
            continue

        source_item_id = batch_entry.get("source_item_id")
        if not source_item_id:
            current_batch = current_batch_map.get(batch_name)
            if not current_batch or current_batch.get("is_root_files"):
                print(f"\n  [{batch_number:02d}] {batch_name}: not found in source — skipping")
                continue
            source_item_id = current_batch["item_id"]

        print(f"\n> [{batch_number:02d}] {batch_name}: enumerating...", end="", flush=True)
        fresh_files = graph.enumerate_recursive(
            source_drive_id, source_item_id, batch_name, token
        )
        print(f" {len(fresh_files)} files")

        fresh_by_path = {f["_path"]: f for f in fresh_files}
        manifest_by_path = {
            f["source_path"]: f
            for f in stored_files
            if f.get("copy_status") != "COPY_FAILED"
        }

        # Detect additions and modifications
        added = [f for f in fresh_files if f["_path"] not in manifest_by_path]
        modified = []
        for f in fresh_files:
            manifest_f = manifest_by_path.get(f["_path"])
            if manifest_f is None:
                continue  # new file, handled in added
            fresh_mtime = (
                (f.get("fileSystemInfo") or {}).get("lastModifiedDateTime")
                or f.get("lastModifiedDateTime", "")
            )
            stored_mtime = manifest_f.get("lastModifiedDateTime", "")
            fresh_size = f.get("size", 0)
            stored_size = manifest_f.get("size", 0)
            if fresh_size != stored_size:
                f["_change_note"] = f"size: {stored_size} → {fresh_size}"
                modified.append(f)
            elif fresh_mtime and stored_mtime and fresh_mtime > stored_mtime:
                f["_change_note"] = f"mtime: {stored_mtime} → {fresh_mtime}"
                modified.append(f)

        # Detect removals (in manifest but gone from source)
        removed = [
            f for f in stored_files
            if f.get("copy_status") != "COPY_FAILED"
            and f.get("source_path", "") not in fresh_by_path
        ]
        all_removed.extend(removed)

        if not added and not modified:
            print(f"  No changes")
            # Still persist source_item_id for future resyncs
            batch_entry["source_item_id"] = source_item_id
            continue

        print(f"  {len(added)} added, {len(modified)} modified, {len(removed)} removed from source")
        total_changed_batches += 1

        csv_path = report.write_resync_changes_csv(
            source_folder_name, batch_number, batch_name, added, modified, run_dir
        )
        print(f"  Changes CSV: {csv_path}")

        # Re-copy the whole batch folder (server-side copy replaces in-place)
        dest_batch = _find_dest_folder(dest_drive_id, dest_root_id, batch_name, token)
        conflict = "replace" if dest_batch else None
        print(f"  Re-copying {len(added) + len(modified)} changed file(s)...", end="", flush=True)
        try:
            location = graph.copy_item(
                source_drive_id, source_item_id, dest_drive_id, dest_root_id, token,
                conflict_behavior=conflict,
            )
            result = graph.poll_copy_job(location)
            if result.get("status") == "completed":
                print(" done.")
            else:
                error = result.get("error", {})
                msg = f"{error.get('code', result.get('status', 'unknown'))}: {error.get('message', '')}"
                print(f" FAILED — {msg}")
                continue
        except Exception as e:
            print(f" ERROR: {e}")
            continue

        batch_entry["source_item_id"] = source_item_id

    if all_removed:
        removals_csv = report.write_resync_removals_csv(all_removed, run_dir)
        print(f"\n  {len(all_removed)} file(s) removed from source — logged to: {removals_csv}")

    report.save_manifest(manifest, source_folder_name, session_id)
    print(f"\n=== Resync complete — {total_changed_batches} batch(es) re-copied ===")
    print(f"Results: {run_dir}\n")


if __name__ == "__main__":
    main()
