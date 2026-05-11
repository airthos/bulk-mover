import graph


def test_get_folder_delta_uses_root_endpoint_for_root(monkeypatch):
    seen = []

    class Response:
        def json(self):
            return {"value": [], "@odata.deltaLink": "delta"}

    def fake_request(method, url, token, params=None):
        seen.append((method, url, params))
        return Response()

    monkeypatch.setattr(graph, "_request", fake_request)

    items, delta = graph.get_folder_delta("drive-1", "root", "token", select="id")

    assert items == []
    assert delta == "delta"
    assert seen == [("GET", f"{graph.GRAPH_BASE}/drives/drive-1/root/delta", {"$select": "id"})]


def test_get_folder_delta_uses_items_endpoint_for_item_id(monkeypatch):
    seen = []

    class Response:
        def json(self):
            return {"value": [], "@odata.deltaLink": "delta"}

    def fake_request(method, url, token, params=None):
        seen.append((method, url, params))
        return Response()

    monkeypatch.setattr(graph, "_request", fake_request)

    graph.get_folder_delta("drive-1", "item-1", "token")

    assert seen == [("GET", f"{graph.GRAPH_BASE}/drives/drive-1/items/item-1/delta", None)]
