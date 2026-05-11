import requests

import graph


def test_get_or_create_folder_returns_existing_after_create_conflict(monkeypatch):
    calls = {"list": 0}

    def fake_list_children(drive_id, parent_id, token, select=None):
        calls["list"] += 1
        if calls["list"] == 1:
            return []
        return [{"id": "existing-id", "name": "A", "folder": {}}]

    def fake_request(method, url, token, json=None, params=None):
        response = requests.Response()
        response.status_code = 409
        raise requests.HTTPError("409 Client Error: Conflict", response=response)

    monkeypatch.setattr(graph, "list_children", fake_list_children)
    monkeypatch.setattr(graph, "_request", fake_request)

    folder_id = graph.get_or_create_folder("drive", "parent", "A", "token")

    assert folder_id == "existing-id"
    assert calls["list"] == 2


def test_get_or_create_folder_matches_existing_case_insensitively(monkeypatch):
    def fake_list_children(drive_id, parent_id, token, select=None):
        return [{"id": "existing-id", "name": "vendors", "folder": {}}]

    def fail_request(*args, **kwargs):
        raise AssertionError("create should not be called")

    monkeypatch.setattr(graph, "list_children", fake_list_children)
    monkeypatch.setattr(graph, "_request", fail_request)

    folder_id = graph.get_or_create_folder("drive", "parent", "Vendors", "token")

    assert folder_id == "existing-id"


def test_get_or_create_folder_reraises_non_conflict_create_errors(monkeypatch):
    def fake_list_children(drive_id, parent_id, token, select=None):
        return []

    def fake_request(method, url, token, json=None, params=None):
        response = requests.Response()
        response.status_code = 500
        raise requests.HTTPError("500 Server Error", response=response)

    monkeypatch.setattr(graph, "list_children", fake_list_children)
    monkeypatch.setattr(graph, "_request", fake_request)

    try:
        graph.get_or_create_folder("drive", "parent", "A", "token")
    except requests.HTTPError as exc:
        assert exc.response.status_code == 500
    else:
        raise AssertionError("expected HTTPError")
