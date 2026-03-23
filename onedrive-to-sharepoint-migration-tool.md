# OneDrive to SharePoint Migration Tool — Implementation Brief

## Purpose

A Python CLI tool that copies files from a personal OneDrive account to a SharePoint document library using the Microsoft Graph API. Designed for the Airtho M365 tenant but reusable across any tenant. Runs locally on Windows from an admin account.

---

## Auth

- **Flow:** Device code (delegated)
- On first run, the tool prints a URL and code to the terminal. The user opens the browser, signs in, and the CLI continues automatically.
- Token cached locally (MSAL token cache in a local JSON file) so re-auth is not needed on every run.
- **Required Graph API permissions (delegated):**
  - `Files.ReadWrite.All`
  - `Sites.ReadWrite.All`
  - `User.Read`

The user registers an Azure app (single-tenant, public client, mobile/desktop redirect URI `https://login.microsoftonline.com/common/oauth2/nativeclient`) and provides **Client ID** and **Tenant ID** via a `.env` file.

---

## Stack

- **Runtime:** Python 3.10+
- **Key packages:**
  - `msal` — Microsoft's official auth library, device code flow + token cache
  - `requests` — all Graph API HTTP calls
  - `inquirer` — interactive terminal selection menus
  - `tqdm` — per-batch progress bars
  - `python-dotenv` — `.env` config loading
  - `csv` — stdlib, no extra dependency needed for report output

---

## User Flow (Terminal)

```
$ python migrate.py

[1] Sign in
> To sign in, visit https://microsoft.com/devicelogin and enter code ABCD-1234
> Waiting for authentication...
> Signed in as brendan@airtho.com

[2] Pick source (OneDrive)
> Enter OneDrive user UPN: brandon@airtho.com
> Fetching top-level folders... done
> Select source folder:
  ❯ Documents/AIrtho/Vendors
    Documents/AIrtho/jobs
    Documents/AIrtho/standard forms
    Documents/AIrtho/Vehicles
    [enter custom path]

[3] Pick destination (SharePoint)
> Enter SharePoint site (e.g. airtho.sharepoint.com/sites/Airtho): airtho.sharepoint.com/sites/Airtho
> Fetching document libraries... done
> Select destination library:
  ❯ Vendors
    Jobs
    Engineering & Standards
    Marketing
    [enter custom library name]
> Select destination folder (Enter for library root):
  ❯ (root)
    A's
    B's
    ...

[4] Scan batches
> Scanning one level deep inside Vendors/...
> Found 27 batches:
    Batch 01 — #           (12 files, 4.2 MB)
    Batch 02 — A's         (84 files, 31 MB)
    Batch 03 — B's         (61 files, 22 MB)
    ...
    Batch 27 — Root files  (3 files, 0.4 MB)
> Proceed? (y/n):

[5] Run
> Running Batch 01 — # ...
  100%|████████████████| 12/12 files
> Verifying...
> ✔ All 12 files verified (hash + size match)
> CSV written: ./migration-logs/2026-03-11_Vendors_batch-01_#.csv

> Running Batch 02 — A's ...
  ...
```

---

## Batching Logic

- User selects a root folder (e.g. `Vendors/`).
- Tool enumerates **one level deep** inside that folder.
- Each immediate child folder = one batch.
- Any files sitting at the root of the selected folder = one final "Root files" batch.
- Batches run sequentially.

---

## Copy Mechanism

- Use the Graph API server-side copy endpoint: `POST /drives/{driveId}/items/{itemId}/copy`
- No local download or upload of file bytes — copy happens entirely on Microsoft's servers.
- After issuing the copy request, poll the async job URL returned in the `Location` response header.
- Poll every 3 seconds, timeout after 10 minutes per batch item. Log a `COPY_FAILED` if timeout is reached.
- Recreate the full subfolder tree under the destination for each batch.

---

## Conflict Resolution

- Before copying, check whether the item already exists at the destination path.
- Compare `lastModifiedDateTime` between source and destination.
- **Take the newer file.** If source is newer, overwrite. If destination is newer, skip and log as `SKIPPED_DEST_NEWER`.
- Log all conflicts in the CSV regardless of outcome.

---

## Verification

After each batch completes, re-fetch metadata for every copied item from the destination and compare against the source manifest captured before the copy.

**Fields compared per file:**
- `size` (bytes)
- `file.hashes.quickXorHash`

**Match statuses written to CSV:**
- `OK` — size and hash match
- `SIZE_MISMATCH`
- `HASH_MISMATCH`
- `MISSING` — file not found at destination after copy
- `SKIPPED_DEST_NEWER` — conflict, destination was newer, not overwritten
- `COPY_FAILED` — Graph API returned an error or polling timed out

---

## CSV Output

One CSV per batch, written to `./migration-logs/` with filename:
`{date}_{source-folder}_{batch-number}_{batch-name}.csv`

**Columns:**
```
batch, source_path, dest_path, size_source, size_dest, hash_source, hash_dest, last_modified_source, last_modified_dest, status, notes
```

A summary row is appended at the end of each CSV:
```
SUMMARY, total={n}, ok={n}, skipped={n}, failed={n}, mismatches={n}
```

---

## Config

`.env` file at project root:

```
AZURE_CLIENT_ID=your-app-client-id
AZURE_TENANT_ID=your-tenant-id
```

No client secret required (public client / device code flow).

---

## Project Structure

```
/
├── migrate.py          # entry point, orchestrates the full flow
├── auth.py             # MSAL device code flow + token cache
├── graph.py            # all Graph API calls (enumerate, copy, poll, verify)
├── prompts.py          # all inquirer terminal prompts
├── batch.py            # batching logic and copy orchestration per batch
├── verify.py           # post-copy manifest comparison
├── report.py           # CSV writer
├── .env                # client ID + tenant ID (not committed)
├── token_cache.json    # MSAL token cache (not committed)
├── migration-logs/     # output CSVs
└── requirements.txt    # msal, requests, inquirer, tqdm, python-dotenv
```

---

## Azure App Setup

Before running the tool, register an app in Azure Portal:

1. Azure Portal > App registrations > New registration
2. Single tenant, name it something like `airtho-migration`
3. Under Authentication > Add platform > Mobile and desktop > add redirect URI: `https://login.microsoftonline.com/common/oauth2/nativeclient`
4. Enable "Allow public client flows"
5. API permissions > Add > Microsoft Graph > Delegated: `Files.ReadWrite.All`, `Sites.ReadWrite.All`, `User.Read`
6. Grant admin consent
7. Copy Client ID and Tenant ID into `.env`

---

## Out of Scope (v1)

- GUI or web interface
- Parallel batch execution
- Mid-batch resume (re-running is safe due to conflict logic)
- Deleting source files — copy only, deletion is a manual step after verification
