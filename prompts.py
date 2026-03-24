from pathlib import Path
from typing import Optional

import inquirer


# ---------------------------------------------------------------------------
# Startup action menu
# ---------------------------------------------------------------------------

def prompt_startup_action(
    incomplete_sessions: list[tuple[Path, dict]],
    all_sessions: list[tuple[Path, dict]],
) -> tuple[str, dict | None]:
    """
    Show a startup menu. Returns (action, manifest) where action is one of:
    'new', 'resume', 'verify', 'verify_manual'. manifest is None for
    'new' and 'verify_manual'.
    """
    choices = ["[New migration]", "[Verify (manual — pick source & dest)]"]
    session_map: dict[str, tuple[str, dict]] = {}

    for _path, manifest in incomplete_sessions:
        completed = len(manifest.get("batches", []))
        total = len(manifest.get("batch_names", []))
        label = (
            f"[Resume]  {manifest.get('source_folder', '?')} → "
            f"{manifest.get('dest_library', '?')} "
            f"({completed}/{total} batches done)  {manifest.get('session_id', '')}"
        )
        choices.append(label)
        session_map[label] = ("resume", manifest)

    for _path, manifest in all_sessions:
        if manifest.get("status") != "completed":
            continue
        total = len(manifest.get("batch_names") or manifest.get("batches", []))
        label = (
            f"[Verify]  {manifest.get('source_folder', '?')} → "
            f"{manifest.get('dest_library', '?')} "
            f"({total} batches)  {manifest.get('session_id', '')}"
        )
        choices.append(label)
        session_map[label] = ("verify", manifest)

    answers = inquirer.prompt([
        inquirer.List("action", message="What would you like to do?", choices=choices)
    ])
    selected = answers["action"]

    if selected == "[New migration]":
        return "new", None
    if selected == "[Verify (manual — pick source & dest)]":
        return "verify_manual", None

    action, manifest = session_map[selected]
    return action, manifest


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------

def prompt_source_upn(default: str = "") -> str:
    answers = inquirer.prompt([
        inquirer.Text(
            "upn",
            message="Enter OneDrive user UPN",
            default=default,
        )
    ])
    return (answers["upn"] or default).strip()


def prompt_source_folder(folders: list[dict]) -> dict:
    """
    Let the user pick from top-level OneDrive folders, shared folders, or enter a custom path.
    Returns the selected driveItem dict, or a synthetic dict with '_custom_path' / '_use_root'.
    """
    def display(f: dict) -> str:
        return f"{f['name']}  (shared)" if f.get("_shared") else f["name"]

    choices = (
        ["[Use drive root]"]
        + [display(f) for f in folders]
        + ["[Search for folder by name]", "[Enter custom path]"]
    )
    answers = inquirer.prompt([
        inquirer.List("folder", message="Select source folder", choices=choices)
    ])
    selected = answers["folder"]

    if selected == "[Use drive root]":
        return {"name": "root", "_use_root": True}

    if selected == "[Search for folder by name]":
        query = inquirer.prompt([
            inquirer.Text("query", message="Enter folder name to search for")
        ])["query"].strip()
        return {"name": query, "_search_query": query}


    if selected == "[Enter custom path]":
        path = inquirer.prompt([
            inquirer.Text("path", message="Enter path relative to drive root (e.g. Documents/Vendors)")
        ])["path"].strip().lstrip("/")
        return {"name": path.split("/")[-1], "_custom_path": path}

    return next(f for f in folders if display(f) == selected)


def prompt_search_result(folders: list[dict]) -> dict:
    """Pick from folders returned by a name search."""
    def display(f: dict) -> str:
        label = f["name"]
        if f.get("_shared"):
            label += "  (shared)"
        return label

    choices = [display(f) for f in folders]
    answers = inquirer.prompt([
        inquirer.List("folder", message="Select folder from search results", choices=choices)
    ])
    return next(f for f in folders if display(f) == answers["folder"])


# ---------------------------------------------------------------------------
# Destination
# ---------------------------------------------------------------------------

def prompt_dest_site() -> str:
    answers = inquirer.prompt([
        inquirer.Text(
            "site",
            message="Enter SharePoint site",
            default="airtho.sharepoint.com/sites/Airtho",
        )
    ])
    return (answers["site"] or "airtho.sharepoint.com/sites/Airtho").strip()


def prompt_dest_library(drives: list[dict]) -> dict:
    """Pick from available document libraries on the SharePoint site."""
    choices = [d["name"] for d in drives] + ["[Enter custom library name]"]
    answers = inquirer.prompt([
        inquirer.List("library", message="Select destination library", choices=choices)
    ])
    selected = answers["library"]

    if selected == "[Enter custom library name]":
        name = inquirer.prompt([
            inquirer.Text("name", message="Enter library name")
        ])["name"].strip()
        return {"name": name, "_custom": True}

    return next(d for d in drives if d["name"] == selected)


def prompt_dest_folder(folders: list[dict]) -> Optional[dict]:
    """
    Pick a subfolder within the destination library, or use the library root.
    Returns the selected driveItem or None for root.
    """
    choices = ["(root)"] + [f["name"] for f in folders]
    answers = inquirer.prompt([
        inquirer.List(
            "folder",
            message="Select destination folder (or root)",
            choices=choices,
        )
    ])
    selected = answers["folder"]

    if selected == "(root)":
        return None

    return next(f for f in folders if f["name"] == selected)


# ---------------------------------------------------------------------------
# Batch confirmation
# ---------------------------------------------------------------------------

def confirm_batches(batches: list[dict]) -> bool:
    print(f"\nFound {len(batches)} batch(es):")
    for b in batches:
        count_str = f"~{b['child_count']} items" if not b["is_root_files"] else f"{b['file_count']} files"
        print(f"  Batch {b['number']:02d} — {b['name']:<35} ({count_str})")
    print()
    answers = inquirer.prompt([
        inquirer.Confirm("proceed", message="Proceed with migration?", default=True)
    ])
    return answers["proceed"]
