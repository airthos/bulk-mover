import graph


def test_list_source_roots_normalizes_shortcuts(monkeypatch):
    monkeypatch.setattr(graph, "get_me_drive", lambda token: {"id": "me-drive", "name": "Mine"})
    monkeypatch.setattr(
        graph,
        "list_children",
        lambda drive_id, item_id, token, select=None: [
            {"id": "folder-1", "name": "Local", "folder": {}},
            {
                "id": "shortcut-shell",
                "name": "Shared Project",
                "remoteItem": {
                    "id": "remote-folder",
                    "folder": {},
                    "parentReference": {"driveId": "remote-drive"},
                },
            },
            {
                "id": "file-shortcut",
                "name": "Shared File",
                "remoteItem": {
                    "id": "remote-file",
                    "file": {},
                    "parentReference": {"driveId": "remote-drive"},
                },
            },
        ],
    )

    roots = graph.list_source_roots("token")
    by_name = {root["name"]: root for root in roots}

    assert by_name["Mine"]["drive_id"] == "me-drive"
    assert by_name["Local"]["item_id"] == "folder-1"
    assert by_name["Shared Project"]["drive_id"] == "remote-drive"
    assert by_name["Shared Project"]["item_id"] == "remote-folder"
    assert "Shared File" not in by_name
