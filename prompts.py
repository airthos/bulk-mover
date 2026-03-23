from typing import Optional

import inquirer


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------

def prompt_source_upn() -> str:
    answers = inquirer.prompt([
        inquirer.Text("upn", message="Enter OneDrive user UPN (e.g. brendan@airtho.com)")
    ])
    return answers["upn"].strip()


def prompt_source_folder(folders: list[dict]) -> dict:
    """
    Let the user pick from top-level OneDrive folders or enter a custom path.
    Returns the selected driveItem dict, or a synthetic dict with '_custom_path'.
    """
    choices = [f["name"] for f in folders] + ["[Enter custom path]"]
    answers = inquirer.prompt([
        inquirer.List("folder", message="Select source folder", choices=choices)
    ])
    selected = answers["folder"]

    if selected == "[Enter custom path]":
        path = inquirer.prompt([
            inquirer.Text("path", message="Enter path relative to drive root (e.g. Documents/Vendors)")
        ])["path"].strip().lstrip("/")
        return {"name": path.split("/")[-1], "_custom_path": path}

    return next(f for f in folders if f["name"] == selected)


# ---------------------------------------------------------------------------
# Destination
# ---------------------------------------------------------------------------

def prompt_dest_site() -> str:
    answers = inquirer.prompt([
        inquirer.Text(
            "site",
            message="Enter SharePoint site (e.g. airtho.sharepoint.com/sites/Airtho)",
        )
    ])
    return answers["site"].strip()


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
