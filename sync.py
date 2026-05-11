import csv
import json
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import graph
import report


RUN_CONFIG = "run.json"
LEDGER = "ledger.jsonl"
PLAN = "plan.csv"
SUMMARY = "report.csv"
MAX_ACTIVE_JOBS = 20
COPY_POLL_INTERVAL = 8
STATUS_INTERVAL = 15
TERMINAL_STATUSES = {"COMPLETED", "FAILED", "SKIPPED"}
DONE_STATUSES = {"COMPLETED", "SKIPPED"}


def find_incomplete_runs() -> list[tuple[Path, dict]]:
    report.LOGS_DIR.mkdir(exist_ok=True)
    runs: list[tuple[Path, dict]] = []
    for path in sorted(report.LOGS_DIR.glob("*/run.json"), reverse=True):
        try:
            config = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if config.get("status") in {"running", "failed"}:
            runs.append((path.parent, config))
    return runs


def run(config: dict, token: str, run_dir: Path | None = None) -> None:
    if run_dir is None:
        run_dir = _create_run_dir(config)
        config = {**config, "status": "running", "started_at": _now()}
    else:
        config = {**config, "status": "running"}
    _write_config(run_dir, config)

    ledger_path = run_dir / LEDGER

    print(f"\nRun log: {run_dir}")
    _resume_submitted_jobs(ledger_path)

    print("\nScanning source...")
    source_progress = ScanProgress("source")
    source_files, source_folders = scan_source_tree(config, token, source_progress)
    source_progress.finish()
    print(f"  {len(source_files)} files, {len(source_folders)} folders")

    print("Scanning destination...")
    dest_progress = ScanProgress("destination")
    dest_files, dest_folders, dest_folder_ids = scan_dest_tree(
        config["dest_drive_id"], config["dest_root_id"], token, dest_progress
    )
    dest_folder_ids = add_folder_id_aliases(source_folders, dest_folder_ids)
    dest_progress.finish()
    print(f"  {len(dest_files)} files, {len(dest_folders)} folders")

    plan = build_plan(source_files, source_folders, dest_files, dest_folders)
    _write_plan(run_dir, plan)
    _print_plan_summary(plan)

    if not plan:
        print("\nNothing to do.")
        config["status"] = "completed"
        config["completed_at"] = _now()
        _write_config(run_dir, config)
        return

    trash_folder_plan = [
        row for row in plan
        if row["action"] == "MKDIR" and row.get("reason") == "trash folder missing"
    ]
    dest_folder_ids = create_missing_folders(
        trash_folder_plan,
        dest_folder_ids,
        config["dest_drive_id"],
        config["dest_root_id"],
        token,
    )

    skipped = [row for row in plan if row["action"] == "SKIP"]
    for row in skipped:
        _append_ledger(ledger_path, _ledger_row(row, "SKIPPED"))

    trash_rows = [row for row in plan if row["action"] == "TRASH"]
    trash_rows = _filter_already_terminal(trash_rows, _read_ledger(ledger_path))
    move_destination_only_files(trash_rows, dest_folder_ids, config, token, ledger_path)

    source_folder_plan = [
        row for row in plan
        if row["action"] == "MKDIR" and row.get("reason") == "destination folder missing"
    ]
    dest_folder_ids = create_missing_folders(
        source_folder_plan,
        dest_folder_ids,
        config["dest_drive_id"],
        config["dest_root_id"],
        token,
    )

    copy_rows = [row for row in plan if row["action"] in {"COPY", "REPLACE"}]
    copy_rows = _filter_already_terminal(copy_rows, _read_ledger(ledger_path))
    submitted = submit_copy_jobs(copy_rows, dest_folder_ids, config, token, ledger_path)
    monitor_jobs(submitted, ledger_path)

    rows = _latest_terminal_rows(_read_ledger(ledger_path))
    report_path = _write_report(run_dir, rows)
    failed = sum(1 for row in rows if row.get("status") == "FAILED")
    config["status"] = "failed" if failed else "completed"
    config["completed_at"] = _now()
    _write_config(run_dir, config)

    actions = Counter(row.get("action", "") for row in rows if row.get("status") in TERMINAL_STATUSES)
    totals = Counter(row.get("status", "") for row in rows)
    print("\nDone.")
    print(f"  Copied:   {actions.get('COPY', 0)}")
    print(f"  Replaced: {actions.get('REPLACE', 0)}")
    print(f"  Skipped:  {actions.get('SKIP', 0)}")
    print(f"  Trashed:  {actions.get('TRASH', 0)}")
    print(f"  Failed:   {totals.get('FAILED', 0)}")
    print(f"  Report:   {report_path}\n")


class ScanProgress:
    def __init__(self, label: str, interval: float = 2.0):
        self.label = label
        self.interval = interval
        self.files = 0
        self.folders = 0
        self.pages = 0
        self.items = 0
        self.started = time.monotonic()
        self.last_print = 0.0

    def add_file(self) -> None:
        self.files += 1
        self.maybe_print()

    def add_folder(self) -> None:
        self.folders += 1
        self.maybe_print()

    def maybe_print(self) -> None:
        now = time.monotonic()
        if now - self.last_print < self.interval:
            return
        elapsed = max(now - self.started, 0.01)
        rate = self.files / (elapsed / 60)
        print(
            f"\r  {self.label}: {self.files} files, {self.folders} folders, "
            f"{rate:.0f} files/min",
            end="",
            flush=True,
        )
        self.last_print = now

    def delta_page(self, pages: int, items: int) -> None:
        self.pages = pages
        self.items = items
        now = time.monotonic()
        if now - self.last_print < self.interval:
            return
        print(
            f"\r  {self.label}: {items} items from {pages} delta page(s)",
            end="",
            flush=True,
        )
        self.last_print = now

    def finish(self) -> None:
        self.last_print = 0.0
        self.maybe_print()
        print()


def scan_source_tree(
    config: dict,
    token: str,
    progress: ScanProgress | None = None,
) -> tuple[dict[str, dict], set[str]]:
    if not config["source_is_folder"]:
        item = graph.get_item(
            config["source_drive_id"],
            config["source_item_id"],
            token,
            select=graph.SOURCE_SELECT,
        )
        item["_path"] = item["name"]
        item["_parent_path"] = ""
        if progress:
            progress.add_file()
        return {item["_path"]: item}, set()

    root = graph.get_item(config["source_drive_id"], config["source_item_id"], token, select="id")
    items, _delta_link = graph.get_folder_delta(
        config["source_drive_id"],
        config["source_item_id"],
        token,
        select=graph.SOURCE_SELECT,
        progress_callback=progress.delta_page if progress else None,
    )
    files, folders, _folder_ids = build_tree_maps(items, root["id"], include_folder_ids=False)
    if progress:
        progress.files = len(files)
        progress.folders = len(folders)
    return files, folders


def scan_dest_tree(
    drive_id: str,
    root_id: str,
    token: str,
    progress: ScanProgress | None = None,
) -> tuple[dict[str, dict], set[str], dict[str, str]]:
    root = graph.get_item(drive_id, root_id, token, select="id")
    items, _delta_link = graph.get_folder_delta(
        drive_id,
        root_id,
        token,
        select="id,name,size,file,folder,fileSystemInfo,lastModifiedDateTime,parentReference,deleted",
        progress_callback=progress.delta_page if progress else None,
    )
    files, folders, folder_ids = build_tree_maps(items, root["id"], include_folder_ids=True)
    folder_ids[""] = root_id
    if progress:
        progress.files = len(files)
        progress.folders = len(folders)
    return files, folders, folder_ids


def build_tree_maps(
    items: list[dict],
    root_id: str,
    include_folder_ids: bool = True,
) -> tuple[dict[str, dict], set[str], dict[str, str]]:
    by_id: dict[str, dict] = {}
    for item in items:
        item_id = item.get("id")
        if not item_id:
            continue
        if item.get("deleted"):
            by_id.pop(item_id, None)
        else:
            by_id[item_id] = item

    path_cache: dict[str, str | None] = {root_id: ""}

    def rel_path(item_id: str) -> str | None:
        if item_id in path_cache:
            return path_cache[item_id]
        item = by_id.get(item_id)
        if not item:
            path_cache[item_id] = None
            return None
        parent_id = (item.get("parentReference") or {}).get("id")
        if not parent_id or parent_id == root_id:
            path = item.get("name", "")
        else:
            parent_path = rel_path(parent_id)
            if parent_path is None:
                path_cache[item_id] = None
                return None
            path = _join(parent_path, item.get("name", ""))
        path_cache[item_id] = path
        return path

    files: dict[str, dict] = {}
    folders: set[str] = set()
    folder_ids: dict[str, str] = {}

    for item_id, item in by_id.items():
        if item_id == root_id:
            continue
        path = rel_path(item_id)
        if not path:
            continue
        if "folder" in item:
            folders.add(path)
            if include_folder_ids:
                folder_ids[path] = item_id
        elif "file" in item:
            item["_path"] = path
            parent_path, _name = _split_parent(path)
            item["_parent_path"] = parent_path
            files[path] = item

    return files, folders, folder_ids


def build_plan(
    source_files: dict[str, dict],
    source_folders: set[str],
    dest_files: dict[str, dict],
    dest_folders: set[str],
) -> list[dict]:
    rows: list[dict] = []
    dest_folder_norm = {_norm_path(path) for path in dest_folders}
    dest_file_by_norm = {_norm_path(path): item for path, item in dest_files.items()}
    source_file_norm = {_norm_path(path) for path in source_files}

    for folder in sorted(source_folders, key=lambda p: (p.count("/"), p.lower())):
        if _norm_path(folder) not in dest_folder_norm:
            rows.append({
                "path": folder,
                "action": "MKDIR",
                "reason": "destination folder missing",
                "is_folder": True,
            })

    trash_folders: set[str] = set()
    for path, dest in sorted(dest_files.items(), key=lambda item: item[0].lower()):
        norm = _norm_path(path)
        if norm == "trash" or norm.startswith("trash/"):
            continue
        if norm in source_file_norm:
            continue
        parent_path, _name = _split_parent(path)
        trash_parent = _join("TRASH", parent_path) if parent_path else "TRASH"
        trash_folders.add("TRASH")
        if parent_path:
            parts = parent_path.split("/")
            for idx in range(len(parts)):
                trash_folders.add(_join("TRASH", "/".join(parts[: idx + 1])))
        rows.append({
            "path": path,
            "action": "TRASH",
            "reason": "destination-only file",
            "is_folder": False,
            "dest_id": dest.get("id", ""),
            "dest_size": dest.get("size", ""),
            "dest_mtime": _mtime(dest),
            "parent_path": parent_path,
            "trash_parent_path": trash_parent,
            "name": dest.get("name", ""),
        })

    for folder in sorted(trash_folders, key=lambda p: (p.count("/"), p.lower())):
        if _norm_path(folder) not in dest_folder_norm:
            rows.append({
                "path": folder,
                "action": "MKDIR",
                "reason": "trash folder missing",
                "is_folder": True,
            })

    for path, source in sorted(source_files.items(), key=lambda item: item[0].lower()):
        dest = dest_file_by_norm.get(_norm_path(path))
        action, reason = _decide_action(source, dest)
        rows.append({
            "path": path,
            "action": action,
            "reason": reason,
            "is_folder": False,
            "source_id": source.get("id", ""),
            "source_size": source.get("size", ""),
            "source_mtime": _mtime(source),
            "dest_id": (dest or {}).get("id", ""),
            "dest_size": (dest or {}).get("size", ""),
            "dest_mtime": _mtime(dest or {}),
            "parent_path": source.get("_parent_path", ""),
            "name": source.get("name", ""),
        })
    return rows


def add_folder_id_aliases(source_folders: set[str], folder_ids: dict[str, str]) -> dict[str, str]:
    result = dict(folder_ids)
    by_norm = {_norm_path(path): folder_id for path, folder_id in folder_ids.items()}
    for folder in source_folders:
        if folder in result:
            continue
        folder_id = by_norm.get(_norm_path(folder))
        if folder_id:
            result[folder] = folder_id
    return result


def create_missing_folders(
    plan: list[dict],
    folder_ids: dict[str, str],
    dest_drive_id: str,
    dest_root_id: str,
    token: str,
) -> dict[str, str]:
    folder_ids = dict(folder_ids)
    folder_ids.setdefault("", dest_root_id)
    folders = [row["path"] for row in plan if row["action"] == "MKDIR"]
    if not folders:
        return folder_ids

    print("\nCreating folders...")
    ordered = sorted(folders, key=lambda p: (p.count("/"), p.lower()))
    total = len(ordered)
    completed = 0
    failed = 0
    for depth in sorted({folder.count("/") for folder in ordered}):
        level = [folder for folder in ordered if folder.count("/") == depth]
        with ThreadPoolExecutor(max_workers=min(MAX_ACTIVE_JOBS, len(level))) as pool:
            futures = {
                pool.submit(
                    _create_folder_item,
                    folder,
                    folder_ids,
                    dest_drive_id,
                    token,
                ): folder
                for folder in level
            }
            for future in as_completed(futures):
                folder = futures[future]
                try:
                    folder_id = future.result()
                    folder_ids[folder] = folder_id
                except Exception as exc:
                    failed += 1
                    print(f"\n  folder failed: {folder} ({exc})")
                completed += 1
                _print_operation_progress("folders", completed, total, folder, failed=failed)
    print()
    return folder_ids


def _create_folder_item(
    folder: str,
    folder_ids: dict[str, str],
    dest_drive_id: str,
    token: str,
) -> str:
    parent_path, name = _split_parent(folder)
    parent_id = folder_ids[parent_path]
    return graph.get_or_create_folder(dest_drive_id, parent_id, name, token)


def submit_copy_jobs(
    rows: list[dict],
    dest_folder_ids: dict[str, str],
    config: dict,
    token: str,
    ledger_path: Path,
) -> list[dict]:
    pending = deque(rows)
    active: list[dict] = []
    submitted: list[dict] = []
    total = len(rows)
    if not total:
        return submitted

    print("\nSubmitting copy jobs...")
    last_progress = 0.0
    while pending or active:
        while pending and len(active) < MAX_ACTIVE_JOBS:
            row = pending.popleft()
            parent_id = dest_folder_ids.get(row.get("parent_path", ""))
            if not parent_id:
                error = f"destination parent missing: {row.get('parent_path', '')}"
                _append_ledger(ledger_path, _ledger_row(row, "FAILED", error=error))
                continue
            try:
                monitor_url = graph.copy_item(
                    source_drive_id=config["source_drive_id"],
                    item_id=row["source_id"],
                    dest_drive_id=config["dest_drive_id"],
                    dest_folder_id=parent_id,
                    token=token,
                    conflict_behavior="replace" if row["action"] == "REPLACE" else None,
                )
                job = {**row, "monitor_url": monitor_url, "submitted_at": _now()}
                active.append(job)
                submitted.append(job)
                _append_ledger(ledger_path, _ledger_row(row, "SUBMITTED", monitor_url=monitor_url))
            except Exception as exc:
                _append_ledger(ledger_path, _ledger_row(row, "FAILED", error=str(exc)))

        completed = []
        for job in active:
            status = _copy_status(job["monitor_url"])
            if status.get("status") in {"completed", "failed"}:
                completed.append((job, status))

        for job, status in completed:
            active.remove(job)
            if status.get("status") == "completed":
                _append_ledger(
                    ledger_path,
                    _ledger_row(job, "COMPLETED", dest_id=status.get("resourceId", "")),
                )
            else:
                error = status.get("error") or {}
                message = f"{error.get('code', 'unknown')}: {error.get('message', '')}"
                _append_ledger(ledger_path, _ledger_row(job, "FAILED", error=message))

        done = len(submitted) - len(active)
        now = time.monotonic()
        if now - last_progress >= 2 or done == total:
            current = active[-1]["path"] if active else ""
            _print_operation_progress("copy jobs", done, total, current, active=len(active))
            last_progress = now
        if pending or active:
            time.sleep(COPY_POLL_INTERVAL)

    print()
    return submitted


def move_destination_only_files(
    rows: list[dict],
    dest_folder_ids: dict[str, str],
    config: dict,
    token: str,
    ledger_path: Path,
) -> None:
    if not rows:
        return
    print("\nMoving destination-only files to TRASH...")
    total = len(rows)
    completed = 0
    with ThreadPoolExecutor(max_workers=min(MAX_ACTIVE_JOBS, total)) as pool:
        futures = {
            pool.submit(_move_trash_item, row, dest_folder_ids, config, token): row
            for row in rows
        }
        for future in as_completed(futures):
            row = futures[future]
            completed += 1
            status, error = future.result()
            _append_ledger(ledger_path, _ledger_row(row, status, error=error))
            _print_operation_progress("trash moves", completed, total, row["path"])
    print()


def _move_trash_item(
    row: dict,
    dest_folder_ids: dict[str, str],
    config: dict,
    token: str,
) -> tuple[str, str]:
    try:
        trash_parent_id = dest_folder_ids[row["trash_parent_path"]]
        trash_name = _trash_name(row)
        graph.move_item(
            config["dest_drive_id"],
            row["dest_id"],
            trash_parent_id,
            token,
            new_name=trash_name,
        )
        return "COMPLETED", ""
    except Exception as exc:
        return "FAILED", str(exc)


def monitor_jobs(jobs: list[dict], ledger_path: Path) -> None:
    open_jobs = _submitted_without_terminal(_read_ledger(ledger_path))
    if not open_jobs:
        return

    print("\nMonitoring copy jobs...")
    _monitor_open_jobs(open_jobs, ledger_path)


def _resume_submitted_jobs(ledger_path: Path) -> None:
    open_jobs = _submitted_without_terminal(_read_ledger(ledger_path))
    if not open_jobs:
        return
    print(f"\nResuming {len(open_jobs)} submitted copy job(s)...")
    _monitor_open_jobs(open_jobs, ledger_path)


def _monitor_open_jobs(jobs: list[dict], ledger_path: Path) -> None:
    pending = list(jobs)
    total = len(pending)
    started = time.monotonic()
    last_status = 0.0

    while pending:
        for job in list(pending):
            status = _copy_status(job["monitor_url"])
            if status.get("status") not in {"completed", "failed"}:
                continue
            pending.remove(job)
            if status.get("status") == "completed":
                _append_ledger(
                    ledger_path,
                    _ledger_row(job, "COMPLETED", dest_id=status.get("resourceId", "")),
                )
            else:
                error = status.get("error") or {}
                message = f"{error.get('code', 'unknown')}: {error.get('message', '')}"
                _append_ledger(ledger_path, _ledger_row(job, "FAILED", error=message))

        now = time.monotonic()
        if now - last_status >= STATUS_INTERVAL or not pending:
            completed = total - len(pending)
            rate = completed / max((now - started) / 60, 0.01)
            eta = (len(pending) / rate) if rate else 0
            print(
                f"  Completed {completed}/{total}, active {len(pending)}, "
                f"rate {rate:.0f}/min, ETA {_fmt_minutes(eta)}"
            )
            last_status = now
        if pending:
            time.sleep(COPY_POLL_INTERVAL)


def _copy_status(monitor_url: str) -> dict:
    try:
        return graph.get_copy_job_status(monitor_url)
    except Exception as exc:
        return {"status": "pending", "error": str(exc)}


def _decide_action(source: dict, dest: dict | None) -> tuple[str, str]:
    if dest is None:
        return "COPY", "destination missing"

    source_size = source.get("size", 0)
    dest_size = dest.get("size", 0)
    if source_size != dest_size:
        return "REPLACE", f"size changed {dest_size} to {source_size}"

    source_mtime = _mtime(source)
    dest_mtime = _mtime(dest)
    if source_mtime and dest_mtime and _parse_time(source_mtime) > _parse_time(dest_mtime):
        return "REPLACE", "source is newer"

    return "SKIP", "destination current"


def _ledger_row(
    row: dict,
    status: str,
    monitor_url: str = "",
    dest_id: str = "",
    error: str = "",
) -> dict:
    return {
        "time": _now(),
        "path": row.get("path", ""),
        "action": row.get("action", ""),
        "status": status,
        "reason": error or row.get("reason", ""),
        "source_id": row.get("source_id", ""),
        "source_size": row.get("source_size", ""),
        "source_mtime": row.get("source_mtime", ""),
        "dest_id": dest_id or row.get("dest_id", ""),
        "dest_size": row.get("dest_size", ""),
        "dest_mtime": row.get("dest_mtime", ""),
        "monitor_url": monitor_url or row.get("monitor_url", ""),
        "parent_path": row.get("parent_path", ""),
        "trash_parent_path": row.get("trash_parent_path", ""),
        "name": row.get("name", ""),
    }


def _append_ledger(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _read_ledger(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _latest_terminal_rows(rows: list[dict]) -> list[dict]:
    latest: dict[str, dict] = {}
    for row in rows:
        if row.get("status") not in TERMINAL_STATUSES:
            continue
        latest[row.get("path", "")] = row
    return list(latest.values())


def _filter_already_terminal(plan_rows: list[dict], ledger_rows: list[dict]) -> list[dict]:
    done_by_path = {
        row.get("path", ""): row
        for row in ledger_rows
        if row.get("status") in DONE_STATUSES
    }
    return [
        row for row in plan_rows
        if row.get("path", "") not in done_by_path
    ]


def _submitted_without_terminal(rows: list[dict]) -> list[dict]:
    latest: dict[str, dict] = {}
    for row in rows:
        latest[row.get("path", "")] = row
    return [
        row for row in latest.values()
        if row.get("status") == "SUBMITTED" and row.get("monitor_url")
    ]


def _write_plan(run_dir: Path, rows: list[dict]) -> Path:
    path = run_dir / PLAN
    fields = [
        "path",
        "action",
        "reason",
        "source_size",
        "source_mtime",
        "dest_size",
        "dest_mtime",
        "source_id",
        "dest_id",
    ]
    run_dir.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def _write_report(run_dir: Path, rows: list[dict]) -> Path:
    report_path = run_dir / SUMMARY
    fields = [
        "time",
        "path",
        "action",
        "status",
        "reason",
        "source_size",
        "source_mtime",
        "dest_size",
        "dest_mtime",
        "source_id",
        "dest_id",
    ]
    with open(report_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return report_path


def _print_plan_summary(rows: list[dict]) -> None:
    counts = Counter(row["action"] for row in rows)
    print("Plan ready:")
    print(f"  Folders: {counts.get('MKDIR', 0)}")
    print(f"  Copy:    {counts.get('COPY', 0)}")
    print(f"  Replace: {counts.get('REPLACE', 0)}")
    print(f"  Skip:    {counts.get('SKIP', 0)}")
    print(f"  Trash:   {counts.get('TRASH', 0)}")


def _print_operation_progress(
    label: str,
    done: int,
    total: int,
    current: str = "",
    active: int | None = None,
    failed: int = 0,
) -> None:
    pct = (done / total * 100) if total else 100
    active_text = f", active {active}" if active is not None else ""
    failed_text = f", failed {failed}" if failed else ""
    current_text = f", current: {_shorten(current)}" if current else ""
    print(
        f"\r  {label}: {done}/{total} ({pct:.0f}%){active_text}{failed_text}{current_text}",
        end="",
        flush=True,
    )


def _shorten(value: str, limit: int = 72) -> str:
    if len(value) <= limit:
        return value
    return "..." + value[-(limit - 3):]


def _create_run_dir(config: dict) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    return report.LOGS_DIR / f"{stamp}_{report._safe(config.get('source_name', 'source'))}"


def _write_config(run_dir: Path, config: dict) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / RUN_CONFIG, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2)


def _join(parent: str, name: str) -> str:
    return f"{parent}/{name}" if parent else name


def _norm_path(path: str) -> str:
    return path.casefold()


def _split_parent(path: str) -> tuple[str, str]:
    if "/" not in path:
        return "", path
    parent, name = path.rsplit("/", 1)
    return parent, name


def _trash_name(row: dict) -> str:
    name = row.get("name") or row.get("path", "").rsplit("/", 1)[-1] or "item"
    dest_id = row.get("dest_id", "")
    if "." in name:
        stem, ext = name.rsplit(".", 1)
        return f"{stem}__trashed_{dest_id}.{ext}"
    return f"{name}__trashed_{dest_id}"


def _mtime(item: dict) -> str:
    return (
        (item.get("fileSystemInfo") or {}).get("lastModifiedDateTime")
        or item.get("lastModifiedDateTime")
        or ""
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_time(value: str) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _fmt_minutes(minutes: float) -> str:
    if minutes <= 0:
        return "unknown"
    total = int(minutes * 60)
    mins, secs = divmod(total, 60)
    hours, mins = divmod(mins, 60)
    if hours:
        return f"{hours}h{mins:02d}m"
    return f"{mins}m{secs:02d}s"
