import json
from pathlib import Path

import sync


def test_decide_copy_when_missing():
    source = {"size": 10}
    assert sync._decide_action(source, None) == ("COPY", "destination missing")


def test_decide_replace_when_size_differs():
    source = {"size": 10}
    dest = {"size": 5}
    action, reason = sync._decide_action(source, dest)
    assert action == "REPLACE"
    assert "size changed" in reason


def test_decide_replace_when_source_newer():
    source = {"size": 10, "lastModifiedDateTime": "2026-05-10T12:00:00Z"}
    dest = {"size": 10, "lastModifiedDateTime": "2026-05-09T12:00:00Z"}
    assert sync._decide_action(source, dest) == ("REPLACE", "source is newer")


def test_decide_skip_when_current():
    source = {"size": 10, "lastModifiedDateTime": "2026-05-09T12:00:00Z"}
    dest = {"size": 10, "lastModifiedDateTime": "2026-05-10T12:00:00Z"}
    assert sync._decide_action(source, dest) == ("SKIP", "destination current")


def test_latest_terminal_rows_keeps_last_status_per_path():
    rows = [
        {"path": "a.txt", "status": "SUBMITTED", "action": "COPY"},
        {"path": "a.txt", "status": "FAILED", "action": "COPY"},
        {"path": "a.txt", "status": "COMPLETED", "action": "COPY"},
        {"path": "b.txt", "status": "SKIPPED", "action": "SKIP"},
    ]
    latest = sync._latest_terminal_rows(rows)
    by_path = {row["path"]: row for row in latest}
    assert by_path["a.txt"]["status"] == "COMPLETED"
    assert by_path["b.txt"]["status"] == "SKIPPED"


def test_build_plan_adds_trash_for_destination_only_files():
    source_files = {
        "keep.txt": {
            "id": "src-1",
            "name": "keep.txt",
            "size": 1,
            "_parent_path": "",
            "lastModifiedDateTime": "2026-05-10T12:00:00Z",
        }
    }
    dest_files = {
        "keep.txt": {
            "id": "dest-1",
            "name": "keep.txt",
            "size": 1,
            "lastModifiedDateTime": "2026-05-10T12:00:00Z",
        },
        "old/f.txt": {
            "id": "dest-2",
            "name": "f.txt",
            "size": 2,
            "lastModifiedDateTime": "2026-05-10T12:00:00Z",
        },
        "TRASH/already.txt": {
            "id": "dest-3",
            "name": "already.txt",
            "size": 3,
        },
    }

    plan = sync.build_plan(source_files, set(), dest_files, {"old"})
    actions = {(row["path"], row["action"]) for row in plan}

    assert ("old/f.txt", "TRASH") in actions
    assert ("TRASH", "MKDIR") in actions
    assert ("TRASH/old", "MKDIR") in actions
    assert ("TRASH/already.txt", "TRASH") not in actions


def test_build_plan_matches_destination_paths_case_insensitively():
    source_files = {
        "Job/Vendors/file.pdf": {
            "id": "src-1",
            "name": "file.pdf",
            "size": 1,
            "_parent_path": "Job/Vendors",
            "lastModifiedDateTime": "2026-05-10T12:00:00Z",
        }
    }
    dest_files = {
        "Job/vendors/file.pdf": {
            "id": "dest-1",
            "name": "file.pdf",
            "size": 1,
            "lastModifiedDateTime": "2026-05-10T12:00:00Z",
        }
    }

    plan = sync.build_plan(source_files, {"Job", "Job/Vendors"}, dest_files, {"Job", "Job/vendors"})
    actions = {(row["path"], row["action"]) for row in plan}

    assert ("Job/Vendors", "MKDIR") not in actions
    assert ("Job/vendors/file.pdf", "TRASH") not in actions
    assert ("Job/Vendors/file.pdf", "SKIP") in actions


def test_add_folder_id_aliases_maps_source_casing_to_destination_ids():
    result = sync.add_folder_id_aliases(
        {"Job/Vendors"},
        {"": "root", "Job": "job-id", "Job/vendors": "vendors-id"},
    )

    assert result["Job/Vendors"] == "vendors-id"


def test_submitted_without_terminal_returns_only_open_jobs():
    rows = [
        {"path": "a.txt", "status": "SUBMITTED", "monitor_url": "url-a"},
        {"path": "a.txt", "status": "COMPLETED", "monitor_url": "url-a"},
        {"path": "b.txt", "status": "SUBMITTED", "monitor_url": "url-b"},
        {"path": "c.txt", "status": "SUBMITTED"},
    ]

    open_jobs = sync._submitted_without_terminal(rows)

    assert len(open_jobs) == 1
    assert open_jobs[0]["path"] == "b.txt"


def test_filter_already_terminal_skips_done_but_retries_failed_rows():
    plan = [
        {"path": "old.txt", "action": "TRASH"},
        {"path": "copy.txt", "action": "COPY"},
        {"path": "todo.txt", "action": "TRASH"},
    ]
    ledger = [
        {"path": "old.txt", "action": "TRASH", "status": "COMPLETED"},
        {"path": "copy.txt", "action": "COPY", "status": "FAILED"},
        {"path": "started.txt", "action": "TRASH", "status": "SUBMITTED"},
    ]

    remaining = sync._filter_already_terminal(plan, ledger)

    assert remaining == [
        {"path": "copy.txt", "action": "COPY"},
        {"path": "todo.txt", "action": "TRASH"},
    ]


def test_move_trash_item_renames_to_avoid_conflicts(monkeypatch):
    calls = []

    def fake_move_item(drive_id, item_id, new_parent_id, token, new_name=None):
        calls.append((drive_id, item_id, new_parent_id, token, new_name))

    monkeypatch.setattr(sync.graph, "move_item", fake_move_item)

    status, error = sync._move_trash_item(
        {
            "dest_id": "abc123",
            "trash_parent_path": "TRASH/A",
            "name": "report.pdf",
        },
        {"TRASH/A": "trash-folder"},
        {"dest_drive_id": "drive-1"},
        "token",
    )

    assert status == "COMPLETED"
    assert error == ""
    assert calls == [(
        "drive-1",
        "abc123",
        "trash-folder",
        "token",
        "report__trashed_abc123.pdf",
    )]


def test_trash_name_handles_files_without_extension():
    assert sync._trash_name({"name": "README", "dest_id": "id1"}) == "README__trashed_id1"


def test_create_missing_folders_creates_by_depth(monkeypatch, tmp_path):
    calls = []

    def fake_get_or_create_folder(drive_id, parent_id, name, token):
        calls.append((parent_id, name))
        return f"id-{name}"

    monkeypatch.setattr(sync.graph, "get_or_create_folder", fake_get_or_create_folder)

    folder_ids = sync.create_missing_folders(
        [
            {"path": "A/B", "action": "MKDIR"},
            {"path": "A", "action": "MKDIR"},
            {"path": "C", "action": "MKDIR"},
        ],
        {"": "root"},
        "drive",
        "root",
        "token",
    )

    assert folder_ids["A"] == "id-A"
    assert folder_ids["C"] == "id-C"
    assert folder_ids["A/B"] == "id-B"
    assert calls.index(("root", "A")) < calls.index(("id-A", "B"))


def test_create_missing_folders_continues_after_failure(monkeypatch, capsys):
    def fake_get_or_create_folder(drive_id, parent_id, name, token):
        if name == "Bad":
            raise RuntimeError("conflict")
        return f"id-{name}"

    monkeypatch.setattr(sync.graph, "get_or_create_folder", fake_get_or_create_folder)

    folder_ids = sync.create_missing_folders(
        [
            {"path": "Good", "action": "MKDIR"},
            {"path": "Bad", "action": "MKDIR"},
        ],
        {"": "root"},
        "drive",
        "root",
        "token",
    )

    assert folder_ids["Good"] == "id-Good"
    assert "Bad" not in folder_ids
    assert "folder failed: Bad" in capsys.readouterr().out


def test_write_and_read_ledger(tmp_path: Path):
    path = tmp_path / "ledger.jsonl"
    sync._append_ledger(path, {"path": "a.txt", "status": "COMPLETED"})
    sync._append_ledger(path, {"path": "b.txt", "status": "SKIPPED"})

    assert [row["path"] for row in sync._read_ledger(path)] == ["a.txt", "b.txt"]
    assert json.loads(path.read_text().splitlines()[0])["status"] == "COMPLETED"


def test_scan_progress_counts_files_and_folders(capsys):
    progress = sync.ScanProgress("source", interval=0)
    progress.add_file()
    progress.add_folder()
    progress.add_file()
    progress.finish()

    out = capsys.readouterr().out
    assert "source: 2 files, 1 folders" in out


def test_operation_progress_is_compact(capsys):
    sync._print_operation_progress("folders", 2, 4, "A/B/C")
    out = capsys.readouterr().out
    assert "folders: 2/4 (50%)" in out
    assert "current: A/B/C" in out


def test_build_tree_maps_reconstructs_paths_from_delta_items():
    items = [
        {"id": "root", "name": "Root", "folder": {}},
        {"id": "folder-a", "name": "A", "folder": {}, "parentReference": {"id": "root"}},
        {"id": "folder-b", "name": "B", "folder": {}, "parentReference": {"id": "folder-a"}},
        {
            "id": "file-1",
            "name": "doc.txt",
            "file": {},
            "size": 12,
            "parentReference": {"id": "folder-b"},
        },
    ]

    files, folders, folder_ids = sync.build_tree_maps(items, "root")

    assert folders == {"A", "A/B"}
    assert folder_ids["A/B"] == "folder-b"
    assert list(files) == ["A/B/doc.txt"]
    assert files["A/B/doc.txt"]["_parent_path"] == "A/B"


def test_build_tree_maps_last_delta_item_wins_and_deleted_removed():
    items = [
        {"id": "root", "name": "Root", "folder": {}},
        {"id": "file-1", "name": "old.txt", "file": {}, "parentReference": {"id": "root"}},
        {"id": "file-1", "name": "new.txt", "file": {}, "parentReference": {"id": "root"}},
        {"id": "file-2", "name": "gone.txt", "file": {}, "parentReference": {"id": "root"}},
        {"id": "file-2", "deleted": {}},
    ]

    files, folders, folder_ids = sync.build_tree_maps(items, "root")

    assert list(files) == ["new.txt"]
    assert folders == set()
    assert folder_ids == {}
