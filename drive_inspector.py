#!/usr/bin/env python3
"""
drive_inspector.py — standalone Graph API drive stats tool

Usage:
    python drive_inspector.py
    python drive_inspector.py --site "airtho.sharepoint.com/sites/Airtho"
    python drive_inspector.py --drive-id <id>
    python drive_inspector.py --no-tenant          # skip tenant-wide section

Auth: reads AZURE_CLIENT_ID / AZURE_TENANT_ID from .env (same as migrate.py).
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from auth import get_access_token
from graph import GRAPH_BASE, _request, get_drive_delta, get_site, list_site_drives

GRAPH = GRAPH_BASE

TENANT_EXTS = [".pdf", ".docx", ".xlsx", ".dwg", ".msg", ".png", ".jpg"]


# ── Graph helpers ─────────────────────────────────────────────────────────────

def _get(token: str, url: str, **params) -> dict:
    return _request("GET", url, token, params=params or None).json()


# ── Site / drive selection ─────────────────────────────────────────────────────

def resolve_site_and_drive(
    token: str, site_arg: str | None, drive_id_arg: str | None
) -> tuple[str, str, list[dict] | None]:
    """
    Returns (drive_id, drive_name, site_drives_or_None).
    site_drives is the full list of drives on the resolved site, used for
    tenant-wide stats. None when only a bare drive-id was given.
    """
    if drive_id_arg and not site_arg:
        data = _get(token, f"{GRAPH}/drives/{drive_id_arg}")
        return drive_id_arg, data.get("name", drive_id_arg), None

    if not site_arg:
        site_arg = input(
            "Enter SharePoint site [airtho.sharepoint.com/sites/Airtho]: "
        ).strip().lstrip("https://").lstrip("http://") or "airtho.sharepoint.com/sites/Airtho"

    # Split hostname and /path — same convention as bulk-mover
    if "/" in site_arg:
        hostname, site_path = site_arg.split("/", 1)
        site_path = "/" + site_path
    else:
        hostname = site_arg
        site_path = ""

    site = get_site(hostname, site_path, token)
    site_id = site["id"]

    drives = list_site_drives(site_id, token)
    if not drives:
        raise RuntimeError("No drives found on this site.")

    # If a specific drive-id was given alongside --site, use it directly
    if drive_id_arg:
        selected = next((d for d in drives if d["id"] == drive_id_arg), None)
        name = selected["name"] if selected else drive_id_arg
        return drive_id_arg, name, drives

    print("\nAvailable drives:")
    for i, d in enumerate(drives):
        print(f"  [{i}] {d['name']}")
    choice = input(f"Select drive [0-{len(drives)-1}]: ").strip()
    selected = drives[int(choice)]
    return selected["id"], selected["name"], drives


# ── Formatting ────────────────────────────────────────────────────────────────

def fmt_bytes(n: int | None) -> str:
    if n is None:
        return "N/A"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print("=" * 60)


# ── Per-drive queries ─────────────────────────────────────────────────────────

def drive_quota(token: str, drive_id: str) -> None:
    section("DRIVE QUOTA")
    data = _get(token, f"{GRAPH}/drives/{drive_id}")
    quota = data.get("quota", {})
    total = quota.get("total")
    used = quota.get("used")
    remaining = quota.get("remaining")
    pct = f"  ({used / total * 100:.1f}%)" if total and used else ""
    print(f"  Name      : {data.get('name')}")
    print(f"  Total     : {fmt_bytes(total)}")
    print(f"  Used      : {fmt_bytes(used)}{pct}")
    print(f"  Remaining : {fmt_bytes(remaining)}")


def file_sample(token: str, drive_id: str, top: int = 10) -> None:
    section(f"FILE SAMPLE — root children (up to {top})")
    data = _get(
        token,
        f"{GRAPH}/drives/{drive_id}/root/children",
        **{"$select": "name,size,file,folder,lastModifiedDateTime", "$top": top},
    )
    items = data.get("value", [])
    if not items:
        print("  (empty)")
        return
    for item in items:
        kind = "FILE  " if "file" in item else "FOLDER"
        size = fmt_bytes(item.get("size"))
        ts = item.get("lastModifiedDateTime", "")[:10]
        print(f"  {kind}  {item['name']:<50}  {size:>10}  {ts}")


def modified_last_n_days(token: str, drive_id: str, days: int = 30) -> None:
    section(f"ITEMS MODIFIED IN LAST {days} DAYS")
    since = datetime.now(timezone.utc) - timedelta(days=days)
    # $filter not supported on search; $count not supported on drive search.
    # Delta + client-side filter is the only reliable approach.
    items, _ = get_drive_delta(drive_id, token, select="name,lastModifiedDateTime,deleted")
    count = sum(
        1 for item in items
        if "deleted" not in item
        and item.get("lastModifiedDateTime", "") >= since.strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    print(f"  Count: {count}  (out of {len(items)} total indexed items)")


def ext_count(token: str, ext: str) -> None:
    """Tenant-wide file type count via POST /search/query (KQL filetype:).
    Note: contentSources is not supported for driveItem — result is tenant-wide.
    """
    section(f"{ext.upper()} FILE COUNT  (tenant-wide)")
    filetype = ext.lstrip(".")
    body = {
        "requests": [{
            "entityTypes": ["driveItem"],
            "query": {"queryString": f"filetype:{filetype}"},
            "from": 0,
            "size": 1,
        }]
    }
    data = _request("POST", f"{GRAPH}/search/query", token, json=body).json()
    containers = data.get("value", [{}])[0].get("hitsContainers", [])
    total = containers[0].get("total", "unknown") if containers else "unknown"
    print(f"  Count: {total}")


# ── Tenant-wide stats ─────────────────────────────────────────────────────────

def _scan_drive(drive: dict, token: str) -> dict:
    """Delta-scan one drive; return item count and per-extension counts."""
    items, _ = get_drive_delta(drive["id"], token, select="name,deleted")
    active = [i for i in items if "deleted" not in i]
    ext_counts = {
        ext: sum(1 for i in active if i.get("name", "").lower().endswith(ext))
        for ext in TENANT_EXTS
    }
    return {
        "name": drive["name"],
        "item_count": len(active),
        "ext_counts": ext_counts,
    }


def tenant_wide_stats(token: str, drives: list[dict]) -> None:
    section(f"TENANT-WIDE STATS  ({len(drives)} drives on this site)")

    # Storage — SharePoint drives share a site collection quota pool,
    # so all drives report the same numbers. Show once, not summed.
    quota_data = _get(token, f"{GRAPH}/drives/{drives[0]['id']}")
    quota = quota_data.get("quota", {})
    used = quota.get("used", 0)
    total = quota.get("total")
    pct = f"  ({used / total * 100:.2f}%)" if total and used else ""
    print(f"\n  Site collection storage (shared pool across all drives):")
    print(f"    Used  : {fmt_bytes(used)}{pct}")
    print(f"    Total : {fmt_bytes(total)}")

    # Scan all drives in parallel (delta + client-side count)
    print(f"\n  Scanning {len(drives)} drives via delta (may take a moment)...")
    results: list[dict] = [None] * len(drives)  # preserve order
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(_scan_drive, d, token): i for i, d in enumerate(drives)}
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()

    # Header row
    ext_headers = "  ".join(f"{e[1:]:>5}" for e in TENANT_EXTS)
    print(f"\n  {'Drive':<22} {'Items':>7}  {ext_headers}")
    print(f"  {'-'*22} {'-'*7}  " + "  ".join(f"{'-----':>5}" for _ in TENANT_EXTS))

    totals_ext = {e: 0 for e in TENANT_EXTS}
    total_items = 0
    for r in results:
        ext_row = "  ".join(f"{r['ext_counts'][e]:>5}" for e in TENANT_EXTS)
        print(f"  {r['name']:<22} {r['item_count']:>7}  {ext_row}")
        total_items += r["item_count"]
        for e in TENANT_EXTS:
            totals_ext[e] += r["ext_counts"][e]

    print(f"  {'─'*22} {'─'*7}  " + "  ".join(f"{'─'*5}" for _ in TENANT_EXTS))
    ext_totals_row = "  ".join(f"{totals_ext[e]:>5}" for e in TENANT_EXTS)
    print(f"  {'TOTAL':<22} {total_items:>7}  {ext_totals_row}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Graph API drive inspector")
    parser.add_argument("--site", help="SharePoint site, e.g. airtho.sharepoint.com/sites/Airtho")
    parser.add_argument("--drive-id", help="Drive ID (skips site/drive selection)")
    parser.add_argument("--days", type=int, default=30, help="Days back for recent-modified query")
    parser.add_argument("--sample", type=int, default=10, help="Number of root items to show")
    parser.add_argument("--no-tenant", action="store_true", help="Skip tenant-wide stats section")
    args = parser.parse_args()

    token = get_access_token()
    drive_id, drive_name, site_drives = resolve_site_and_drive(token, args.site, args.drive_id)

    print(f"\nInspecting drive: {drive_name}  ({drive_id})")

    drive_quota(token, drive_id)
    file_sample(token, drive_id, top=args.sample)
    modified_last_n_days(token, drive_id, days=args.days)
    for ext in (".pdf", ".docx", ".xlsx", ".dwg"):
        ext_count(token, ext)

    if site_drives and not args.no_tenant:
        tenant_wide_stats(token, site_drives)

    print()


if __name__ == "__main__":
    main()
