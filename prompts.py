from pathlib import Path

import inquirer

import graph


def prompt_resume_run(runs: list[tuple[Path, dict]]) -> tuple[Path | None, dict | None]:
    choices = ["[Start new run]"]
    run_map: dict[str, tuple[Path, dict]] = {}
    for run_dir, config in runs:
        label = (
            f"[Resume] {config.get('source_name', '?')} to "
            f"{config.get('dest_library', '?')} ({run_dir.name})"
        )
        choices.append(label)
        run_map[label] = (run_dir, config)

    answers = inquirer.prompt([
        inquirer.List("run", message="Found unfinished runs", choices=choices)
    ])
    selected = answers["run"]
    if selected == "[Start new run]":
        return None, None
    return run_map[selected]


def prompt_source_url() -> str:
    answers = inquirer.prompt([
        inquirer.Text("url", message="Paste OneDrive source file or folder URL")
    ])
    return answers["url"].strip()


def prompt_source(token: str) -> dict:
    choices = ["[Paste OneDrive URL]"]
    source_map: dict[str, dict] = {}
    try:
        for item in graph.list_source_roots(token):
            choices.append(item["label"])
            source_map[item["label"]] = item
    except Exception as exc:
        print(f"Could not list OneDrive folders. Paste a URL instead. ({exc})")

    answers = inquirer.prompt([
        inquirer.List("source", message="Select source", choices=choices)
    ])
    selected = answers["source"]
    if selected == "[Paste OneDrive URL]":
        url = prompt_source_url()
        item = graph.get_drive_item_from_url(url, token)
        remote = item.get("remoteItem")
        actual = remote or item
        drive_id = (actual.get("parentReference") or {}).get("driveId")
        return {
            "source_url": url,
            "source_name": item.get("name") or actual.get("name", "source"),
            "source_drive_id": drive_id,
            "source_item_id": actual["id"],
            "source_is_folder": "folder" in actual,
        }

    item = source_map[selected]
    return {
        "source_url": "",
        "source_name": item["name"],
        "source_drive_id": item["drive_id"],
        "source_item_id": item["item_id"],
        "source_is_folder": item["is_folder"],
    }


def prompt_sharepoint_site_url() -> str:
    answers = inquirer.prompt([
        inquirer.Text("url", message="Paste SharePoint site URL")
    ])
    return answers["url"].strip()


def prompt_destination(token: str) -> dict:
    choices = ["[Paste SharePoint site URL]"]
    site_map: dict[str, dict] = {}
    try:
        sites = graph.list_followed_sites(token)
        for site in sites:
            label = site.get("displayName") or site.get("name") or site.get("webUrl") or site["id"]
            choices.append(label)
            site_map[label] = site
    except Exception as exc:
        print(f"Could not list followed sites. Paste a site URL instead. ({exc})")

    answers = inquirer.prompt([
        inquirer.List("site", message="Select destination site", choices=choices)
    ])
    selected = answers["site"]
    if selected == "[Paste SharePoint site URL]":
        site_url = prompt_sharepoint_site_url()
        site = graph.get_site_from_url(site_url, token)
    else:
        site = site_map[selected]
        site_url = site.get("webUrl", "")

    drives = graph.list_site_drives(site["id"], token)
    dest_library = prompt_dest_library(drives)
    dest_root_id = prompt_dest_folder(dest_library["id"], token)
    return {
        "dest_site_url": site_url,
        "dest_site_id": site["id"],
        "dest_library": dest_library["name"],
        "dest_drive_id": dest_library["id"],
        "dest_root_id": dest_root_id,
    }


def prompt_dest_library(drives: list[dict]) -> dict:
    choices = [drive["name"] for drive in drives]
    answers = inquirer.prompt([
        inquirer.List("library", message="Select destination library", choices=choices)
    ])
    return next(drive for drive in drives if drive["name"] == answers["library"])


def prompt_dest_folder(dest_drive_id: str, token: str) -> str:
    current_id = "root"
    path_parts: list[str] = []

    while True:
        children = graph.list_children(
            dest_drive_id, current_id, token, select="id,name,folder"
        )
        folders = [item for item in children if "folder" in item]
        location = "/" + "/".join(path_parts) if path_parts else "library root"

        choices = ["[Use this folder]", "[Create folder here]"]
        if path_parts:
            choices.append("[Go up]")
        choices.extend(folder["name"] for folder in folders)

        answers = inquirer.prompt([
            inquirer.List(
                "folder",
                message=f"Destination: {location}",
                choices=choices,
            )
        ])
        selected = answers["folder"]

        if selected == "[Use this folder]":
            return current_id
        if selected == "[Create folder here]":
            name = inquirer.prompt([
                inquirer.Text("name", message="New folder name")
            ])["name"].strip()
            if name:
                current_id = graph.get_or_create_folder(dest_drive_id, current_id, name, token)
                path_parts.append(name)
            continue
        if selected == "[Go up]":
            path_parts.pop()
            current_id = "root"
            for part in path_parts:
                current_id = graph.get_or_create_folder(dest_drive_id, current_id, part, token)
            continue

        folder = next(item for item in folders if item["name"] == selected)
        current_id = folder["id"]
        path_parts.append(folder["name"])


def confirm_run(config: dict) -> bool:
    print("\nCopy plan")
    print(f"  Source:      {config['source_name']}")
    print(f"  Destination: {config['dest_library']}")
    print("  Existing destination files are replaced when source is newer or size differs.")
    print("  Destination-only files are moved to TRASH at the destination root.")
    print("  Source files are never modified.\n")
    answers = inquirer.prompt([
        inquirer.Confirm("proceed", message="Run copy/update now?", default=True)
    ])
    return bool(answers["proceed"])
