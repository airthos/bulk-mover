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


def write_batch_csv(
    source_folder: str,
    batch_number: int,
    batch_name: str,
    files: list[dict],
) -> Path:
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
) -> None:
    batch_entry = {
        "batch_name": batch_name,
        "batch_number": batch_number,
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
    _ensure_logs_dir()
    date = session_id[:10]
    filename = LOGS_DIR / f"{date}_{_safe(source_folder)}_session.manifest.json"
    with open(filename, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, default=str)
    return filename


def find_incomplete_sessions() -> list[tuple[Path, dict]]:
    """Find manifest files with status 'in_progress'. Returns [(path, manifest), ...]."""
    _ensure_logs_dir()
    results = []
    for p in sorted(LOGS_DIR.glob("*_session.manifest.json"), reverse=True):
        try:
            with open(p, encoding="utf-8") as fh:
                manifest = json.load(fh)
            if manifest.get("status") == "in_progress":
                results.append((p, manifest))
        except (json.JSONDecodeError, OSError):
            continue
    return results

