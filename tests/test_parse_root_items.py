"""
Unit tests for _parse_root_items — the logic that turns the raw Graph API
children response into the normalised folder list shown to the user.

Run with:  python -m pytest tests/ -v
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from migrate import _parse_root_items


# ---------------------------------------------------------------------------
# Regular (non-shortcut) items
# ---------------------------------------------------------------------------

def test_regular_folder_included():
    items = [{"id": "f1", "name": "Documents", "folder": {"childCount": 3}}]
    result = _parse_root_items(items)
    assert len(result) == 1
    assert result[0]["name"] == "Documents"


def test_regular_file_excluded():
    items = [{"id": "f1", "name": "notes.txt", "file": {"mimeType": "text/plain"}}]
    assert _parse_root_items(items) == []


def test_mixed_regular_items():
    items = [
        {"id": "f1", "name": "Folder", "folder": {}},
        {"id": "f2", "name": "File.txt", "file": {}},
    ]
    result = _parse_root_items(items)
    assert len(result) == 1
    assert result[0]["name"] == "Folder"


# ---------------------------------------------------------------------------
# Shortcut items — folder facet inside remoteItem (happy path)
# ---------------------------------------------------------------------------

def test_shortcut_folder_facet_in_remote():
    """Standard shortcut: folder facet lives inside remoteItem."""
    items = [{
        "id": "local-id",
        "name": "Shared Folder",
        "remoteItem": {
            "id": "remote-id",
            "folder": {"childCount": 5},
            "parentReference": {"driveId": "remote-drive-id"},
        },
    }]
    result = _parse_root_items(items)
    assert len(result) == 1
    r = result[0]
    assert r["name"] == "Shared Folder"
    assert r["id"] == "remote-id"
    assert r["_drive_id"] == "remote-drive-id"
    assert r["_shared"] is True


def test_shortcut_folder_facet_on_item():
    """Some Graph responses put folder on the top-level item, not in remoteItem."""
    items = [{
        "id": "local-id",
        "name": "Shared Folder",
        "folder": {"childCount": 2},
        "remoteItem": {
            "id": "remote-id",
            "parentReference": {"driveId": "remote-drive-id"},
        },
    }]
    result = _parse_root_items(items)
    assert len(result) == 1
    assert result[0]["id"] == "remote-id"
    assert result[0]["folder"] == {"childCount": 2}


def test_shortcut_no_folder_facet_anywhere_included():
    """
    Some Graph responses return shortcut items with no folder facet at all —
    just remoteItem with an id. As long as there's no file facet, include it.
    This is the case that was silently dropping shortcuts before.
    """
    items = [{
        "id": "local-id",
        "name": "Shared Folder",
        "remoteItem": {
            "id": "remote-id",
            "parentReference": {"driveId": "remote-drive-id"},
        },
    }]
    result = _parse_root_items(items)
    assert len(result) == 1
    assert result[0]["id"] == "remote-id"


# ---------------------------------------------------------------------------
# Shortcut items — file shortcuts should be excluded
# ---------------------------------------------------------------------------

def test_shortcut_file_in_remote_excluded():
    items = [{
        "id": "local-id",
        "name": "Shared File.pdf",
        "remoteItem": {
            "id": "remote-id",
            "file": {"mimeType": "application/pdf"},
            "parentReference": {"driveId": "remote-drive-id"},
        },
    }]
    assert _parse_root_items(items) == []


def test_shortcut_file_on_item_excluded():
    """file facet on the top-level item should also be excluded."""
    items = [{
        "id": "local-id",
        "name": "Shared File.pdf",
        "file": {"mimeType": "application/pdf"},
        "remoteItem": {
            "id": "remote-id",
            "parentReference": {"driveId": "remote-drive-id"},
        },
    }]
    assert _parse_root_items(items) == []


# ---------------------------------------------------------------------------
# driveId extraction edge cases
# ---------------------------------------------------------------------------

def test_shortcut_missing_parent_reference():
    """remoteItem without parentReference — _drive_id should be None."""
    items = [{
        "id": "local-id",
        "name": "Shared Folder",
        "remoteItem": {
            "id": "remote-id",
            "folder": {},
        },
    }]
    result = _parse_root_items(items)
    assert len(result) == 1
    assert result[0]["_drive_id"] is None


def test_shortcut_missing_drive_id_in_parent_reference():
    """parentReference present but no driveId — _drive_id should be None."""
    items = [{
        "id": "local-id",
        "name": "Shared Folder",
        "remoteItem": {
            "id": "remote-id",
            "folder": {},
            "parentReference": {"path": "/drives/something/root:"},
        },
    }]
    result = _parse_root_items(items)
    assert result[0]["_drive_id"] is None


# ---------------------------------------------------------------------------
# Mixed list
# ---------------------------------------------------------------------------

def test_mixed_list():
    items = [
        {"id": "f1", "name": "Local Folder", "folder": {"childCount": 1}},
        {"id": "f2", "name": "File.txt", "file": {}},
        {
            "id": "s1",
            "name": "Shared (folder in remote)",
            "remoteItem": {
                "id": "r1",
                "folder": {},
                "parentReference": {"driveId": "d1"},
            },
        },
        {
            "id": "s2",
            "name": "Shared (no folder facet)",
            "remoteItem": {
                "id": "r2",
                "parentReference": {"driveId": "d2"},
            },
        },
        {
            "id": "s3",
            "name": "Shared File",
            "remoteItem": {
                "id": "r3",
                "file": {},
                "parentReference": {"driveId": "d3"},
            },
        },
    ]
    result = _parse_root_items(items)
    names = [r["name"] for r in result]
    assert "Local Folder" in names
    assert "Shared (folder in remote)" in names
    assert "Shared (no folder facet)" in names  # the previously-broken case
    assert "File.txt" not in names
    assert "Shared File" not in names
    assert len(result) == 3
