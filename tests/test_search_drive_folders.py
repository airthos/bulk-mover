"""
Unit tests for graph.search_drive_folders — verifies mount-point shortcut
parsing from search results.

Run with: python -m pytest tests/ -v
"""
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import graph


def _make_request(items):
    """Patch graph._request to return a fake response with the given items."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"value": items}
    return patch("graph._request", return_value=mock_resp)


# ---------------------------------------------------------------------------
# Mount-point shortcut (the case that /children misses)
# ---------------------------------------------------------------------------

def test_mount_point_shortcut_found():
    """A remoteItem folder with no top-level folder facet is included."""
    items = [{
        "id": "local-id",
        "name": "standard forms",
        "parentReference": {"driveId": "my-drive", "id": "root-id"},
        "remoteItem": {
            "id": "remote-id",
            "folder": {"childCount": 10},
            "parentReference": {"driveId": "sp-drive-id"},
        },
    }]
    with _make_request(items):
        result = graph.search_drive_folders("my-drive", "standard forms", "token")
    assert len(result) == 1
    r = result[0]
    assert r["name"] == "standard forms"
    assert r["id"] == "remote-id"
    assert r["_drive_id"] == "sp-drive-id"
    assert r["_shared"] is True


def test_mount_point_shortcut_no_folder_facet_included():
    """remoteItem without folder facet still included (no file facet = assume folder)."""
    items = [{
        "id": "local-id",
        "name": "standard forms",
        "remoteItem": {
            "id": "remote-id",
            "parentReference": {"driveId": "sp-drive-id"},
        },
    }]
    with _make_request(items):
        result = graph.search_drive_folders("my-drive", "standard forms", "token")
    assert len(result) == 1
    assert result[0]["id"] == "remote-id"


def test_file_shortcut_excluded():
    """remoteItem with file facet is excluded."""
    items = [{
        "id": "local-id",
        "name": "report.pdf",
        "remoteItem": {
            "id": "remote-id",
            "file": {"mimeType": "application/pdf"},
            "parentReference": {"driveId": "sp-drive-id"},
        },
    }]
    with _make_request(items):
        result = graph.search_drive_folders("my-drive", "report", "token")
    assert result == []


def test_regular_folder_in_results_included():
    """A regular folder (no remoteItem) appearing in search results is included."""
    items = [{"id": "f1", "name": "Standard Forms Local", "folder": {"childCount": 2}}]
    with _make_request(items):
        result = graph.search_drive_folders("my-drive", "standard", "token")
    assert len(result) == 1
    assert result[0]["id"] == "f1"


def test_mixed_results():
    items = [
        {
            "id": "s1", "name": "standard forms",
            "remoteItem": {"id": "r1", "folder": {}, "parentReference": {"driveId": "d1"}},
        },
        {"id": "f1", "name": "Standard Local Folder", "folder": {}},
        {
            "id": "s2", "name": "standard report.pdf",
            "remoteItem": {"id": "r2", "file": {}, "parentReference": {"driveId": "d2"}},
        },
    ]
    with _make_request(items):
        result = graph.search_drive_folders("my-drive", "standard", "token")
    names = [r["name"] for r in result]
    assert "standard forms" in names
    assert "Standard Local Folder" in names
    assert "standard report.pdf" not in names
    assert len(result) == 2
