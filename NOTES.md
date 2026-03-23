# Project Notes — OneDrive → SharePoint Migration Tool

Tracks research findings, architectural decisions, and current state. Update this as the project evolves.

---

## Current State

**Status:** Pre-implementation. Spec document exists (`onedrive-to-sharepoint-migration-tool.md`), no code written yet.

**Next step:** Begin implementation starting with `auth.py` (MSAL device code flow + token cache), then `graph.py`, then wire everything through `migrate.py`.

---

## Scope Assumption

**Destination is always an empty drive/library.** Conflict resolution is not required. This simplifies the architecture significantly — no pre-flight dest listing, no date comparison, no `conflictBehavior` parameter needed.

---

## Copy Strategy: Per-Folder (not per-file)

### Decision
Copy at the **batch folder level**, not per-file. Each batch = one `POST /copy` call on the top-level subfolder. Microsoft handles parallelization internally.

### Why
- A folder with 84 files takes ~1-2 min via folder copy vs. 10-30+ min via sequential per-file copies
- Rate limiting is not a concern for sequential per-file, but speed is — folder copy is dramatically faster
- No conflict resolution needed (empty dest), so the only thing per-file copy bought us is now gone
- Verification still enumerates all files post-copy regardless — same observability either way

### What changes vs per-file
- One copy job per batch instead of one per file
- Source manifest captured before copy by recursively enumerating the source batch folder
- Post-copy verification enumerates dest batch folder and compares against manifest
- CSV rows are still per-file (captured during verification, not during copy)

### What stays the same
- Full source manifest captured before every batch
- Per-file hash + size verification after each batch
- Per-file rows in the CSV
- Session manifest JSON updated incrementally

---

## Full Batch Flow

```
for each batch (top-level subfolder of selected root):

  1. ENUMERATE source batch folder recursively
     → capture per-file: id, path, size, quickXorHash, createdBy, lastModifiedBy,
       createdDateTime, lastModifiedDateTime, sharepointIds
     → save to session manifest as source truth

  2. COPY the batch folder
     POST /drives/{sourceDriveId}/items/{batchFolderId}/copy
     Body: { "parentReference": { "driveId": "{destDriveId}", "id": "{destParentId}" } }
     → 202 Accepted, Location: https://.../_api/v2.0/monitor/{jobId}

  3. POLL the Location URL (no auth required) every 3s
     → { "status": "inProgress", "percentageComplete": 45.0 }
     → { "status": "completed", "resourceId": "...", "resourceLocation": "..." }
     → { "status": "failed", "error": {...} }
     Timeout: 10 min → log entire batch as COPY_FAILED

  4. ENUMERATE dest batch folder recursively (same paginated children calls)
     → collect per-file: id, name, size, quickXorHash, sharepointIds

  5. VERIFY per file against source manifest
     → compare size and quickXorHash
     → handle null hash: retry up to 3x with 10s delay → log HASH_PENDING if still null
     → use JSON batching (20 GETs per batch call) for efficiency

  6. WRITE CSV for this batch (one row per file)
     WRITE/UPDATE session manifest JSON

  7. Move to next batch
```

---

## Graph API Research Findings

### Site Resolution
User enters `airtho.sharepoint.com/sites/Airtho` → resolve to IDs:
```
GET /sites/airtho.sharepoint.com:/sites/Airtho   → returns site.id
GET /sites/{siteId}/drives                        → returns all document libraries (name + driveId)
```

### OneDrive Enumeration
```
GET /users/{upn}/drive                              → get source driveId
GET /drives/{driveId}/items/root/children           → top-level folders for batch scan
GET /drives/{driveId}/items/{folderId}/children     → recurse into batch for manifest
```

---

## Pagination — CRITICAL

All list responses are paged. **Must follow `@odata.nextLink` until absent.**

```python
def get_all_children(drive_id, item_id, headers):
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children"
    items = []
    while url:
        resp = requests.get(url, headers=headers)
        data = resp.json()
        items.extend(data["value"])
        url = data.get("@odata.nextLink")
    return items
```

Default page size: 200 items.

---

## Rate Limiting / Throttling

Graph returns `429` (and sometimes `503`) when throttled.

- Read `Retry-After` header (seconds). Always honour it.
- If no `Retry-After`, exponential backoff starting at 5s.
- Pause **all** requests when throttled — sending more extends the window.
- With per-folder copy the number of copy POST calls is tiny (one per batch). Throttling risk is minimal.

```python
def graph_request(method, url, headers, json=None, retries=5):
    for attempt in range(retries):
        resp = requests.request(method, url, headers=headers, json=json)
        if resp.status_code in (429, 503):
            wait = int(resp.headers.get("Retry-After", 2 ** attempt * 5))
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    raise Exception(f"Max retries exceeded: {url}")
```

---

## Copy + Poll Pattern

```
POST /drives/{sourceDriveId}/items/{batchFolderItemId}/copy
Content-Type: application/json
Body: { "parentReference": { "driveId": "...", "id": "..." } }

→ 202 Accepted
  Location: https://.../_api/v2.0/monitor/{jobId}
```

Poll Location URL (no auth header):
```
→ { "status": "inProgress", "percentageComplete": 27.8 }
→ { "status": "completed", "resourceId": "...", "resourceLocation": "..." }
→ { "status": "failed", "error": { "code": "...", "message": "..." } }
```

Poll every 3s. Timeout 10 min → log batch as `COPY_FAILED`.

---

## JSON Batching (Verification Phase)

Use for fetching dest file metadata during verification — 20 GETs per call.

```
POST https://graph.microsoft.com/v1.0/$batch
{
  "requests": [
    { "id": "1", "method": "GET", "url": "/drives/{destDriveId}/items/{id1}?$select=id,name,size,file,sharepointIds" },
    { "id": "2", "method": "GET", "url": "/drives/{destDriveId}/items/{id2}?$select=id,name,size,file,sharepointIds" }
  ]
}
```

Each response item can independently 429. Check `response.status` per item; retry 429s individually. Batch outer response is always 200.

---

## Session Files + Observability

### Files written to `./migration-logs/`
```
{date}_{source-folder}_{batch-nn}_{batch-name}.csv     ← per-batch result
{date}_{source-folder}_session.manifest.json           ← full session
```

### Manifest structure (written before copy, updated after verify)
```json
{
  "session_id": "2026-03-23T10:00:00Z",
  "source_upn": "brendan@airtho.com",
  "source_drive_id": "...",
  "dest_site": "airtho.sharepoint.com/sites/Airtho",
  "dest_drive_id": "...",
  "batches": [
    {
      "batch_name": "A's",
      "batch_number": 2,
      "batch_folder_id": "...",
      "copy_status": "COMPLETED",
      "files": [
        {
          "source_id": "...",
          "source_path": "Vendors/A's/foo.pdf",
          "dest_path": "Vendors/A's/foo.pdf",
          "size": 204800,
          "quickXorHash": "abc123==",
          "createdBy": "brendan@airtho.com",
          "createdDateTime": "2022-04-01T09:00:00Z",
          "lastModifiedBy": "brendan@airtho.com",
          "lastModifiedDateTime": "2023-11-15T14:30:00Z",
          "dest_id": "...",
          "verify_status": "OK"
        }
      ]
    }
  ]
}
```

Manifest is written **before copy starts** with source truth, then updated with `copy_status` and `verify_status` after each phase. If the run crashes, the manifest shows exactly where it stopped.

### QuickXorHash null handling
- Hash computed async on Microsoft's servers; can be null immediately after copy
- Retry up to 3x with 10s delay
- Still null → log `HASH_PENDING`, not `HASH_MISMATCH`

### Verify statuses
- `OK` — size and hash match
- `SIZE_MISMATCH` — sizes differ
- `HASH_MISMATCH` — sizes match, hashes differ
- `HASH_PENDING` — hash null after retries
- `MISSING` — file not found at dest
- `COPY_FAILED` — copy job failed or timed out

---

## Metadata Preservation

### What Graph API copy resets
All metadata on the destination driveItem is new: `createdBy`, `lastModifiedBy`, `createdDateTime`, `lastModifiedDateTime` all reflect the migration runner and copy time. This is expected and unavoidable with the copy API.

### What can be restored (best-effort, optional)
After copy, the dest file's SharePoint `listItem/fields` can be PATCHed:
```
PATCH /sites/{siteId}/lists/{listId}/items/{listItemId}/fields
{ "Created": "2022-04-01T09:00:00Z", "Modified": "2023-11-15T14:30:00Z" }
```
`Author` and `Editor` (Created By / Modified By) are harder — require site admin, user must exist in SP, need SP user ID not email. **Deferred to v2.**

### v1 approach
- Capture full source metadata in manifest (always)
- Attempt `Created`/`Modified` date restoration post-verify
- Log `METADATA_OK`, `METADATA_PARTIAL`, `METADATA_FAILED` per file
- Inform user at startup that Created By / Modified By will show migration runner

---

## Key Design Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Copy granularity | Per-folder | Empty dest assumption; speed; simplicity |
| Conflict resolution | None | Destination always empty |
| Hash for verification | `quickXorHash` only | Only hash guaranteed on both ODB and SharePoint |
| Batch definition | One level deep — each immediate child folder = one batch | Root-level files = "Root files" batch |
| Authentication | MSAL device code + local token cache | Delegated, no secret, cache avoids re-auth |
| Verification fetching | JSON batch (20 GETs/call) | Reduces API calls by 20x in verify phase |
| Session file | JSON manifest written before copy, updated after verify | Full audit trail; crash-recoverable state |
| Metadata restoration | Dates only, best-effort PATCH to listItem | Author/Editor deferred to v2 |

---

## Important Edge Cases

1. **`@odata.nextLink` pagination** — all list calls, no exceptions
2. **429/503 throttling** — respect `Retry-After`; pause all requests
3. **`quickXorHash` null post-copy** — retry 3x with 10s delay; log `HASH_PENDING`
4. **Batch 429s** — check each response item status; outer batch is always 200
5. **30,000 item limit** — Graph copy API cap per operation; warn if batch is large
6. **"Root files" batch** — files sitting directly in the selected root folder; copy individually (not a folder)

---

## Planned File Structure

```
/
├── migrate.py          # entry point, orchestrates full flow
├── auth.py             # MSAL device code flow + token cache
├── graph.py            # all Graph API calls + throttle retry wrapper
├── prompts.py          # inquirer terminal prompts
├── batch.py            # batch scanning and per-batch orchestration
├── verify.py           # post-copy manifest comparison + optional metadata patch
├── report.py           # CSV writer + manifest JSON writer
├── .env                # AZURE_CLIENT_ID, AZURE_TENANT_ID (not committed)
├── token_cache.json    # MSAL token cache (not committed)
├── requirements.txt    # msal, requests, inquirer, tqdm, python-dotenv
└── migration-logs/
    ├── {date}_{source}_{batch-nn}_{name}.csv
    └── {date}_{source}_session.manifest.json
```

---

## API Endpoints Quick Reference

| Purpose | Endpoint |
|---------|----------|
| Get source user's drive | `GET /users/{upn}/drive` |
| List top-level folders (batch scan) | `GET /drives/{driveId}/items/root/children?$select=id,name,size,folder` |
| Enumerate batch folder (manifest) | `GET /drives/{driveId}/items/{folderId}/children?$select=id,name,size,file,fileSystemInfo,createdBy,lastModifiedBy,sharepointIds` |
| Get SP site by URL | `GET /sites/{hostname}:{/server-relative-path}` |
| List SP document libraries | `GET /sites/{siteId}/drives` |
| Copy batch folder | `POST /drives/{sourceDriveId}/items/{folderId}/copy` |
| Poll copy job | `GET {Location header URL}` (no auth) |
| Batch fetch dest items (verify) | `POST https://graph.microsoft.com/v1.0/$batch` |
| Patch SP metadata (dates) | `PATCH /sites/{siteId}/lists/{listId}/items/{listItemId}/fields` |

---

## Requirements

```
msal
requests
inquirer
tqdm
python-dotenv
```

Azure app: single-tenant, public client, delegated:
- `Files.ReadWrite.All`
- `Sites.ReadWrite.All`
- `User.Read`

Redirect URI: `https://login.microsoftonline.com/common/oauth2/nativeclient`
