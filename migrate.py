import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

import auth
import batch as batch_mod
import graph
import prompts
import report
import verify

load_dotenv()


def main() -> None:
    print("\n=== OneDrive → SharePoint Migration Tool ===\n")

    # ------------------------------------------------------------------
    # [1] Auth
    # ------------------------------------------------------------------
    print("[1] Sign in")
    token = auth.get_access_token()

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
    top_level = graph.list_children(source_drive_id, "root", token, select="id,name,folder")
    folders = [item for item in top_level if "folder" in item]
    print(" done")

    source_folder_item = prompts.prompt_source_folder(folders)

    # Resolve custom path if entered manually
    if "_custom_path" in source_folder_item:
        print("  Resolving path...", end="", flush=True)
        source_folder_item = graph.get_item_by_path(
            source_drive_id, "/" + source_folder_item["_custom_path"], token
        )
        print(" done")

    source_folder_id = source_folder_item["id"]
    source_folder_name = source_folder_item["name"]

    # ------------------------------------------------------------------
    # [3] Destination (SharePoint)
    # ------------------------------------------------------------------
    print("\n[3] Pick destination (SharePoint)")
    site_input = prompts.prompt_dest_site()

    # Parse hostname and server-relative path from user input
    # e.g. "airtho.sharepoint.com/sites/Airtho" → hostname + /sites/Airtho
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
    )
    manifest_path = report.save_manifest(manifest, source_folder_name, session_id)

    for b in batches:
        # --- Copy phase ---
        files = batch_mod.run_batch(
            batch=b,
            source_drive_id=source_drive_id,
            dest_drive_id=dest_drive_id,
            dest_root_id=dest_root_id,
            token=token,
        )

        if not files:
            continue

        # --- Verify phase ---
        if b["is_root_files"]:
            # Root files were copied individually; verify by dest_resource_id
            _verify_root_files(files, dest_drive_id, token)
        else:
            # Folder batch: find the newly created dest batch folder by name
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

        # --- Write CSV ---
        csv_path = report.write_batch_csv(
            source_folder_name, b["number"], b["name"], files
        )
        print(f"  CSV written: {csv_path}")

        # --- Update manifest ---
        report.add_batch_to_manifest(manifest, b["name"], b["number"], files)
        report.save_manifest(manifest, source_folder_name, session_id)

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    total = sum(b.get("file_count") or 0 for b in batches)
    print(f"\n=== Migration complete — {total} files processed ===")
    print(f"Manifest: {manifest_path}\n")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
