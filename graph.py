import time
from typing import Generator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Fields captured from source files for the manifest and verification
SOURCE_SELECT = (
    "id,name,size,file,folder,fileSystemInfo,createdBy,lastModifiedBy,"
    "createdDateTime,lastModifiedDateTime,sharepointIds,parentReference"
)


# ---------------------------------------------------------------------------
# Shared session with transport-level retry + connection pooling
# ---------------------------------------------------------------------------

_retry = Retry(
    total=5,
    backoff_factor=1.0,
    status_forcelist=[500, 502, 503, 504],
    backoff_jitter=0.3,
    allowed_methods=["GET", "POST", "PUT", "PATCH"],
)
_adapter = HTTPAdapter(
    max_retries=_retry,
    pool_connections=1,   # single host: graph.microsoft.com
    pool_maxsize=4,
)

_session = requests.Session()
_session.mount("https://", _adapter)
_session.headers.update({
    "User-Agent": "NONISV|Airtho|BulkMover/1.0",
})

# Separate session for anonymous poll URLs (no auth, still needs retry)
_poll_session = requests.Session()
_poll_session.mount("https://", _adapter)
_poll_session.headers.update({
    "User-Agent": "NONISV|Airtho|BulkMover/1.0",
})


# ---------------------------------------------------------------------------
# Core request helpers
# ---------------------------------------------------------------------------

def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _request(method: str, url: str, token: str, retries: int = 5, **kwargs) -> requests.Response:
    """Single Graph API request with app-level throttle/retry on top of transport retry."""
    headers = _headers(token)
    headers.update(kwargs.pop("extra_headers", {}))
    for attempt in range(retries):
        resp = _session.request(method, url, headers=headers, timeout=30, **kwargs)
        if resp.status_code in (429, 503):
            wait = int(resp.headers.get("Retry-After", 2 ** attempt * 5))
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Max retries exceeded for {url}")


def _paginate(url: str, token: str, params: Optional[dict] = None) -> Generator[dict, None, None]:
    """Yield all items from a paged Graph API list endpoint."""
    while url:
        resp = _request("GET", url, token, params=params)
        data = resp.json()
        yield from data.get("value", [])
        url = data.get("@odata.nextLink")
        params = None  # nextLink already contains query params


# ---------------------------------------------------------------------------
# Drive / site resolution
# ---------------------------------------------------------------------------

def get_user_drive(upn: str, token: str) -> dict:
    return _request("GET", f"{GRAPH_BASE}/users/{upn}/drive", token).json()


def get_site(hostname: str, site_path: str, token: str) -> dict:
    """Resolve a SharePoint site by hostname and server-relative path."""
    url = f"{GRAPH_BASE}/sites/{hostname}:{site_path}"
    return _request("GET", url, token).json()


def list_site_drives(site_id: str, token: str) -> list[dict]:
    return list(_paginate(f"{GRAPH_BASE}/sites/{site_id}/drives", token))


# ---------------------------------------------------------------------------
# Item operations
# ---------------------------------------------------------------------------

def list_children(
    drive_id: str,
    item_id: str,
    token: str,
    select: Optional[str] = None,
) -> list[dict]:
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/children"
    params = {"$select": select} if select else None
    return list(_paginate(url, token, params=params))


def get_item(
    drive_id: str,
    item_id: str,
    token: str,
    select: Optional[str] = None,
) -> dict:
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
    params = {"$select": select} if select else None
    return _request("GET", url, token, params=params).json()


def get_item_by_path(drive_id: str, path: str, token: str) -> dict:
    """Get a driveItem by path relative to drive root. path must start with /."""
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:{path}"
    return _request("GET", url, token).json()


def enumerate_recursive(
    drive_id: str,
    folder_id: str,
    base_path: str,
    token: str,
) -> list[dict]:
    """
    Recursively enumerate all files under folder_id.
    Returns a flat list of file driveItems, each with a '_path' key
    set to its path relative to the selected root folder.
    """
    result = []
    children = list_children(drive_id, folder_id, token, select=SOURCE_SELECT)
    for item in children:
        item_path = f"{base_path}/{item['name']}" if base_path else item["name"]
        if "folder" in item:
            result.extend(enumerate_recursive(drive_id, item["id"], item_path, token))
        else:
            item["_path"] = item_path
            result.append(item)
    return result


# ---------------------------------------------------------------------------
# Copy
# ---------------------------------------------------------------------------

def copy_item(
    source_drive_id: str,
    item_id: str,
    dest_drive_id: str,
    dest_folder_id: str,
    token: str,
    conflict_behavior: Optional[str] = None,
) -> str:
    """Trigger a server-side copy. Returns the Location URL to poll.

    conflict_behavior: "replace" to overwrite existing, "rename" to add suffix,
    or None for default (fail on conflict).
    Only pass "replace" when the caller has already confirmed source is newer.
    """
    url = f"{GRAPH_BASE}/drives/{source_drive_id}/items/{item_id}/copy"
    body: dict = {
        "parentReference": {
            "driveId": dest_drive_id,
            "id": dest_folder_id,
        }
    }
    if conflict_behavior:
        body["@microsoft.graph.conflictBehavior"] = conflict_behavior
    resp = _request("POST", url, token, json=body)
    location = resp.headers.get("Location")
    if not location:
        raise RuntimeError("Copy accepted but no Location header returned")
    return location


def poll_copy_job(
    location: str,
    timeout_seconds: Optional[int] = None,
    progress_callback=None,
) -> dict:
    """
    Poll a copy job monitor URL until completed, failed, or timed out.
    The Location URL is anonymous — no auth header required.

    Uses exponential backoff: starts at 5s, grows by 1.5x, caps at 60s.
    timeout_seconds=None means poll indefinitely.

    Returns the final status response body. On timeout, includes the
    Location URL so callers can persist it for later resume.
    """
    interval = 5.0
    start = time.time()

    while True:
        if timeout_seconds is not None and (time.time() - start) >= timeout_seconds:
            return {"status": "timeout", "_location": location}

        try:
            resp = _poll_session.get(location, timeout=30)
        except requests.RequestException:
            # Transport error — back off and retry
            time.sleep(min(interval, 60))
            interval = min(interval * 1.5, 60)
            continue

        if resp.status_code in (200, 202):
            data = resp.json()
            status = data.get("status", "")

            pct = data.get("percentageComplete") or data.get("percentComplete", 0)
            if progress_callback and pct:
                progress_callback(pct, time.time() - start)

            if status in ("completed", "failed"):
                return data

        time.sleep(interval)
        interval = min(interval * 1.5, 60)


# ---------------------------------------------------------------------------
# JSON batching (verification phase)
# ---------------------------------------------------------------------------

def batch_get_items(requests_list: list[dict], token: str) -> list[dict]:
    """
    Execute JSON batch GETs (up to 20 per call).
    requests_list: [{ "id": str, "method": "GET", "url": str }, ...]

    Handles per-item 429s by retrying after the longest retry-after seen.
    Returns responses in the same order as the input list.
    """
    url = "https://graph.microsoft.com/v1.0/$batch"
    results: dict[str, dict] = {}

    chunks = [requests_list[i : i + 20] for i in range(0, len(requests_list), 20)]

    for chunk in chunks:
        pending = {req["id"]: req for req in chunk}

        while pending:
            resp = _request("POST", url, token, json={"requests": list(pending.values())})
            responses = resp.json().get("responses", [])

            retry_after = 0
            still_pending: dict[str, dict] = {}

            for r in responses:
                if r["status"] == 429:
                    ra = int(r.get("headers", {}).get("retry-after", 5))
                    retry_after = max(retry_after, ra)
                    still_pending[r["id"]] = pending[r["id"]]
                else:
                    results[r["id"]] = r

            if still_pending:
                time.sleep(retry_after)
                pending = still_pending
            else:
                break

    return [results[req["id"]] for req in requests_list if req["id"] in results]


# ---------------------------------------------------------------------------
# SharePoint metadata
# ---------------------------------------------------------------------------

def search_drive_folders(drive_id: str, query: str, token: str) -> list[dict]:
    """
    Search a drive for folders matching query. Returns normalised folder dicts.
    Includes mount-point shortcuts (remoteItem) which are invisible to /children.

    Only fetches the first page — search results can span thousands of pages and
    mount-point shortcuts appear in results when the query matches their name.
    """
    url = f"{GRAPH_BASE}/drives/{drive_id}/root/search(q='{query}')"
    resp = _request("GET", url, token, params={"$top": 50})
    items = resp.json().get("value", [])
    folders = []
    for item in items:
        if "remoteItem" in item:
            remote = item["remoteItem"]
            if "file" in remote or "file" in item:
                continue
            folders.append({
                "id": remote["id"],
                "name": item["name"],
                "folder": remote.get("folder") or item.get("folder", {}),
                "_drive_id": remote.get("parentReference", {}).get("driveId"),
                "_shared": True,
            })
        elif "folder" in item:
            folders.append(item)
    return folders



# ---------------------------------------------------------------------------
# Delta queries
# ---------------------------------------------------------------------------

def get_drive_delta(
    drive_id: str,
    token: str,
    delta_link: str | None = None,
    select: str | None = None,
) -> tuple[list[dict], str]:
    """
    Fetch changed/new items since the last delta token.

    If delta_link is None, does a full enumeration (initial sync).
    Returns (items, new_delta_link).
    """
    if delta_link:
        url = delta_link
        params = None
    else:
        url = f"{GRAPH_BASE}/drives/{drive_id}/root/delta"
        params = {}
        if select:
            params["$select"] = select

    items: list[dict] = []
    new_delta_link = ""

    while url:
        resp = _request("GET", url, token, params=params)
        data = resp.json()
        items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
        params = None  # nextLink has params baked in
        if "@odata.deltaLink" in data:
            new_delta_link = data["@odata.deltaLink"]

    return items, new_delta_link


def patch_list_item_fields(
    site_id: str,
    list_id: str,
    list_item_id: str,
    fields: dict,
    token: str,
) -> dict:
    url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items/{list_item_id}/fields"
    return _request("PATCH", url, token, json=fields).json()
