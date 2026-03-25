import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

LOGS_DIR = Path("migration-logs")


def _ensure_logs_dir() -> None:
    LOGS_DIR.mkdir(exist_ok=True)


def _date_prefix() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _safe(name: str) -> str:
    return name.replace("/", "-").replace(" ", "-").replace("'", "")


def _session_id_safe(session_id: str) -> str:
    """Convert e.g. '2026-03-24T12:00:00Z' → '2026-03-24T120000Z' for use in path names."""
    return session_id.replace(":", "")


def session_dir(session_id: str | None, source_folder: str) -> Path:
    """Return the per-session log directory path (not yet created)."""
    if not session_id:
        return LOGS_DIR / _safe(source_folder)
    dirname = f"{_session_id_safe(session_id)}_{_safe(source_folder)}"
    return LOGS_DIR / dirname


# ---------------------------------------------------------------------------
# Source file helpers
# ---------------------------------------------------------------------------

def _source_hash(item: dict) -> str:
    return (item.get("file") or {}).get("hashes", {}).get("quickXorHash", "")


def _user_email(identity: Optional[dict]) -> str:
    if not identity:
        return ""
    user = identity.get("user", {})
    return user.get("email") or user.get("displayName", "")


def _modified_dt(item: dict) -> str:
    return (
        (item.get("fileSystemInfo") or {}).get("lastModifiedDateTime")
        or item.get("lastModifiedDateTime", "")
    )


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "batch",
    "source_path",
    "dest_path",
    "size_source",
    "hash_source",
    "created_by",
    "created_datetime",
    "last_modified_by",
    "last_modified_source",
    "copy_status",
    "verify_status",
    "notes",
]


def resync_run_dir(session_id: str | None, source_folder: str) -> Path:
    """Return a timestamped sub-run directory for a resync pass."""
    sdir = session_dir(session_id, source_folder)
    return sdir / f"resync-{_date_prefix()}"


def deep_verify_run_dir(session_id: str | None, source_folder: str) -> Path:
    """Return a timestamped sub-run directory for a deep verify pass."""
    sdir = session_dir(session_id, source_folder)
    return sdir / f"deep-verify-{_date_prefix()}"


def write_batch_csv(
    source_folder: str,
    batch_number: int,
    batch_name: str,
    files: list[dict],
    session_id: str = "",
    run_dir: "Path | None" = None,
) -> Path:
    if run_dir is not None:
        run_dir.mkdir(parents=True, exist_ok=True)
        filename = run_dir / f"batch-{batch_number:02d}_{_safe(batch_name)}.csv"
    elif session_id:
        sdir = session_dir(session_id, source_folder)
        sdir.mkdir(parents=True, exist_ok=True)
        filename = sdir / f"batch-{batch_number:02d}_{_safe(batch_name)}.csv"
    else:
        _ensure_logs_dir()
        filename = (
            LOGS_DIR
            / f"{_date_prefix()}_{_safe(source_folder)}_batch-{batch_number:02d}_{_safe(batch_name)}.csv"
        )

    ok = sum(1 for f in files if f.get("verify_status") == "OK")
    failed = sum(1 for f in files if f.get("copy_status") == "COPY_FAILED")
    mismatches = sum(
        1 for f in files
        if f.get("verify_status") in ("SIZE_MISMATCH", "HASH_MISMATCH", "MISSING", "HASH_PENDING")
    )

    with open(filename, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()

        for item in files:
            writer.writerow({
                "batch": batch_name,
                "source_path": item.get("_path", ""),
                "dest_path": item.get("_path", ""),
                "size_source": item.get("size", ""),
                "hash_source": _source_hash(item),
                "created_by": _user_email(item.get("createdBy")),
                "created_datetime": item.get("createdDateTime", ""),
                "last_modified_by": _user_email(item.get("lastModifiedBy")),
                "last_modified_source": _modified_dt(item),
                "copy_status": item.get("copy_status", "COMPLETED"),
                "verify_status": item.get("verify_status", ""),
                "notes": item.get("verify_notes") or item.get("copy_notes", ""),
            })

        # Summary row
        writer.writerow({
            "batch": "SUMMARY",
            "source_path": (
                f"total={len(files)}, ok={ok}, failed={failed}, mismatches={mismatches}"
            ),
            "dest_path": "",
            "size_source": "",
            "hash_source": "",
            "created_by": "",
            "created_datetime": "",
            "last_modified_by": "",
            "last_modified_source": "",
            "copy_status": "",
            "verify_status": "",
            "notes": "",
        })

    return filename


_DEST_ONLY_FIELDS = ["dest_path", "size", "verify_status", "notes"]


def write_dest_only_csv(dest_only_items: list[dict], run_dir: Path) -> Path:
    """
    Write a CSV of destination-only files (DEST_ONLY status) — files present at
    the destination but not in the source manifest. Written once per verify run.
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    filename = run_dir / "dest-only.csv"
    with open(filename, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_DEST_ONLY_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for item in dest_only_items:
            writer.writerow({
                "dest_path": item.get("_dest_path", ""),
                "size": item.get("size", ""),
                "verify_status": "DEST_ONLY",
                "notes": "File exists at destination but not in source manifest",
            })
        writer.writerow({
            "dest_path": f"SUMMARY: total={len(dest_only_items)}",
            "size": "",
            "verify_status": "",
            "notes": "",
        })
    return filename


# ---------------------------------------------------------------------------
# Resync reports
# ---------------------------------------------------------------------------

_RESYNC_CHANGES_FIELDS = [
    "batch", "source_path", "change_type", "size", "last_modified_source", "notes"
]
_RESYNC_REMOVALS_FIELDS = [
    "source_path", "size", "last_modified_source", "notes"
]


def write_resync_changes_csv(
    source_folder: str,
    batch_number: int,
    batch_name: str,
    added: list[dict],
    modified: list[dict],
    run_dir: Path,
) -> Path:
    """Write per-batch CSV of added and modified files detected during a resync."""
    run_dir.mkdir(parents=True, exist_ok=True)
    filename = run_dir / f"batch-{batch_number:02d}_{_safe(batch_name)}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_RESYNC_CHANGES_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for f in added:
            writer.writerow({
                "batch": batch_name,
                "source_path": f.get("_path", ""),
                "change_type": "ADDED",
                "size": f.get("size", ""),
                "last_modified_source": _modified_dt(f),
                "notes": "",
            })
        for f in modified:
            writer.writerow({
                "batch": batch_name,
                "source_path": f.get("_path", ""),
                "change_type": "MODIFIED",
                "size": f.get("size", ""),
                "last_modified_source": _modified_dt(f),
                "notes": f.get("_change_note", ""),
            })
        writer.writerow({
            "batch": "SUMMARY",
            "source_path": f"added={len(added)}, modified={len(modified)}",
            "change_type": "",
            "size": "",
            "last_modified_source": "",
            "notes": "",
        })
    return filename


def write_resync_removals_csv(removed: list[dict], run_dir: Path) -> Path:
    """
    Write a single removals CSV listing source files that no longer exist in OneDrive.
    These are logged only — nothing is deleted from the destination.
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    filename = run_dir / "removals.csv"
    with open(filename, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_RESYNC_REMOVALS_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for f in removed:
            writer.writerow({
                "source_path": f.get("source_path", ""),
                "size": f.get("size", ""),
                "last_modified_source": f.get("lastModifiedDateTime", ""),
                "notes": "Removed from source OneDrive — NOT deleted from destination",
            })
        writer.writerow({
            "source_path": f"SUMMARY: total={len(removed)}",
            "size": "",
            "last_modified_source": "",
            "notes": "",
        })
    return filename


# ---------------------------------------------------------------------------
# Session manifest
# ---------------------------------------------------------------------------

def init_manifest(
    session_id: str,
    source_upn: str,
    source_folder: str,
    dest_site: str,
    dest_library: str,
    *,
    source_drive_id: str = "",
    source_folder_id: str = "",
    dest_drive_id: str = "",
    dest_root_id: str = "",
    batch_names: list[str] | None = None,
) -> dict:
    return {
        "session_id": session_id,
        "status": "in_progress",
        "source_upn": source_upn,
        "source_folder": source_folder,
        "source_drive_id": source_drive_id,
        "source_folder_id": source_folder_id,
        "dest_site": dest_site,
        "dest_library": dest_library,
        "dest_drive_id": dest_drive_id,
        "dest_root_id": dest_root_id,
        "batch_names": batch_names or [],
        "batches": [],
    }


def add_batch_to_manifest(
    manifest: dict,
    batch_name: str,
    batch_number: int,
    files: list[dict],
    source_item_id: str = "",
) -> None:
    batch_entry = {
        "batch_name": batch_name,
        "batch_number": batch_number,
        "source_item_id": source_item_id,
        "files": [],
    }
    for item in files:
        batch_entry["files"].append({
            "source_id": item.get("id"),
            "source_path": item.get("_path"),
            "size": item.get("size"),
            "quickXorHash": _source_hash(item),
            "createdBy": _user_email(item.get("createdBy")),
            "createdDateTime": item.get("createdDateTime"),
            "lastModifiedBy": _user_email(item.get("lastModifiedBy")),
            "lastModifiedDateTime": _modified_dt(item),
            "dest_id": item.get("dest_id"),
            "copy_status": item.get("copy_status", "COMPLETED"),
            "verify_status": item.get("verify_status", ""),
            "verify_notes": item.get("verify_notes", ""),
        })
    manifest["batches"].append(batch_entry)


def mark_manifest_completed(manifest: dict) -> None:
    manifest["status"] = "completed"


def save_manifest(manifest: dict, source_folder: str, session_id: str) -> Path:
    sdir = session_dir(session_id, source_folder)
    sdir.mkdir(parents=True, exist_ok=True)
    filename = sdir / "session.manifest.json"
    with open(filename, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, default=str)
    return filename


def _load_all_manifests() -> list[tuple[Path, dict]]:
    """Load all session manifests, newest first."""
    _ensure_logs_dir()
    results = []
    candidates = sorted(
        list(LOGS_DIR.glob("*/session.manifest.json"))
        + list(LOGS_DIR.glob("*_session.manifest.json")),
        reverse=True,
    )
    for p in candidates:
        try:
            with open(p, encoding="utf-8") as fh:
                manifest = json.load(fh)
            results.append((p, manifest))
        except (json.JSONDecodeError, OSError):
            continue
    return results


def find_incomplete_sessions() -> list[tuple[Path, dict]]:
    """Find manifest files with status 'in_progress'. Returns [(path, manifest), ...]."""
    return [(p, m) for p, m in _load_all_manifests() if m.get("status") == "in_progress"]


def find_all_sessions() -> list[tuple[Path, dict]]:
    """Find all session manifests regardless of status. Returns [(path, manifest), ...]."""
    return _load_all_manifests()

