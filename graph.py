import base64
import time
from urllib.parse import urlparse, unquote
from typing import Callable, Generator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Fields captured from source files for copy decisions and reports.
SOURCE_SELECT = (
    "id,name,size,file,folder,fileSystemInfo,createdBy,lastModifiedBy,"
    "createdDateTime,lastModifiedDateTime,sharepointIds,parentReference,remoteItem"
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
    pool_connections=1,
    pool_maxsize=4,
)

_session = requests.Session()
_session.mount("https://", _adapter)
_session.headers.update({
    "User-Agent": "BulkMover/1.0",
})

# Separate session for anonymous poll URLs (no auth, still needs retry)
_poll_session = requests.Session()
_poll_session.mount("https://", _adapter)
_poll_session.headers.update({
    "User-Agent": "BulkMover/1.0",
})

# Optional token refresher set once at startup.
_token_refresher: Optional[Callable[[], str]] = None


def register_token_refresher(fn: Callable[[], str]) -> None:
    """Register a callable that returns a fresh access token on demand.

    Called once from migrate.py after initial auth so that _request can
    transparently recover from 401 (token expiry) mid-session.
    """
    global _token_refresher
    _token_refresher = fn


# ---------------------------------------------------------------------------
# Core request helpers
# ---------------------------------------------------------------------------

def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _request(method: str, url: str, token: str, retries: int = 5, **kwargs) -> requests.Response:
    """Single Graph API request with app-level throttle/retry on top of transport retry.

    On a 401 (token expiry), calls the registered token refresher once and
    retries immediately before falling through to raise_for_status.
    """
    extra = kwargs.pop("extra_headers", {})
    _tok = token
    for attempt in range(retries):
        hdrs = {**_headers(_tok), **extra}
        resp = _session.request(method, url, headers=hdrs, timeout=30, **kwargs)
        if resp.status_code == 401 and _token_refresher is not None and attempt == 0:
            _tok = _token_refresher()
            continue
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
        params = None


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


def get_me_drive(token: str) -> dict:
    return _request("GET", f"{GRAPH_BASE}/me/drive", token).json()


def list_followed_sites(token: str) -> list[dict]:
    return list(_paginate(f"{GRAPH_BASE}/me/followedSites", token))


def get_site_from_url(site_url: str, token: str) -> dict:
    parsed = urlparse(site_url.strip())
    if not parsed.netloc:
        raise ValueError("SharePoint site URL must include a hostname.")

    path = unquote(parsed.path.rstrip("/"))
    marker = "/sites/"
    if marker in path:
        site_path = path[: path.find(marker) + len(marker)]
        site_name = path[path.find(marker) + len(marker):].split("/", 1)[0]
        path = f"{site_path}{site_name}"
    elif path:
        parts = [part for part in path.split("/") if part]
        path = "/" + "/".join(parts[:2]) if parts else ""

    return get_site(parsed.netloc, path, token)


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


def get_drive_item_from_url(item_url: str, token: str) -> dict:
    encoded = base64.urlsafe_b64encode(item_url.strip().encode("utf-8")).decode("ascii")
    share_id = "u!" + encoded.rstrip("=")
    url = f"{GRAPH_BASE}/shares/{share_id}/driveItem"
    return _request("GET", url, token).json()


def list_source_roots(token: str) -> list[dict]:
    drive = get_me_drive(token)
    children = list_children(drive["id"], "root", token, select=SOURCE_SELECT)
    roots = [{
        "label": "[My OneDrive root]",
        "name": drive.get("name", "My OneDrive"),
        "drive_id": drive["id"],
        "item_id": "root",
        "is_folder": True,
    }]
    for item in children:
        if "folder" in item:
            roots.append({
                "label": item["name"],
                "name": item["name"],
                "drive_id": drive["id"],
                "item_id": item["id"],
                "is_folder": True,
            })
            continue
        remote = item.get("remoteItem")
        if not remote or "file" in remote:
            continue
        remote_drive_id = (remote.get("parentReference") or {}).get("driveId")
        if not remote_drive_id:
            continue
        roots.append({
            "label": f"{item['name']} (shortcut/shared)",
            "name": item["name"],
            "drive_id": remote_drive_id,
            "item_id": remote["id"],
            "is_folder": True,
        })
    return roots


def get_or_create_folder(
    drive_id: str,
    parent_id: str,
    name: str,
    token: str,
) -> str:
    """
    Get or create a folder by name directly under parent_id.
    Returns the folder item ID.
    """
    children = list_children(drive_id, parent_id, token, select="id,name,folder")
    existing = next((c for c in children if _same_name(c.get("name", ""), name) and "folder" in c), None)
    if existing:
        return existing["id"]
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{parent_id}/children"
    body = {"name": name, "folder": {}, "@microsoft.graph.conflictBehavior": "fail"}
    try:
        resp = _request("POST", url, token, json=body)
        return resp.json()["id"]
    except requests.HTTPError as exc:
        if exc.response is None or exc.response.status_code != 409:
            raise
        children = list_children(drive_id, parent_id, token, select="id,name,folder")
        existing = next((c for c in children if _same_name(c.get("name", ""), name) and "folder" in c), None)
        if existing:
            return existing["id"]
        raise


def _same_name(left: str, right: str) -> bool:
    return left.casefold() == right.casefold()


def move_item(
    drive_id: str,
    item_id: str,
    new_parent_id: str,
    token: str,
    new_name: str | None = None,
) -> dict:
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
    body = {"parentReference": {"id": new_parent_id}}
    if new_name:
        body["name"] = new_name
    return _request("PATCH", url, token, json=body).json()


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
    params = None
    if conflict_behavior:
        params = {"@microsoft.graph.conflictBehavior": conflict_behavior}
    resp = _request("POST", url, token, json=body, params=params)
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
    The Location URL is anonymous. No auth header required.

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


def get_copy_job_status(location: str) -> dict:
    resp = _poll_session.get(location, timeout=30)
    resp.raise_for_status()
    return resp.json()

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
        params = None
        if "@odata.deltaLink" in data:
            new_delta_link = data["@odata.deltaLink"]

    return items, new_delta_link


def get_folder_delta(
    drive_id: str,
    folder_id: str,
    token: str,
    select: str | None = None,
    progress_callback=None,
) -> tuple[list[dict], str]:
    if folder_id == "root":
        url = f"{GRAPH_BASE}/drives/{drive_id}/root/delta"
    else:
        url = f"{GRAPH_BASE}/drives/{drive_id}/items/{folder_id}/delta"
    params = {"$select": select} if select else None
    items: list[dict] = []
    delta_link = ""
    pages = 0

    while url:
        resp = _request("GET", url, token, params=params)
        data = resp.json()
        page_items = data.get("value", [])
        items.extend(page_items)
        pages += 1
        if progress_callback:
            progress_callback(pages, len(items))
        url = data.get("@odata.nextLink")
        params = None
        if "@odata.deltaLink" in data:
            delta_link = data["@odata.deltaLink"]

    return items, delta_link
