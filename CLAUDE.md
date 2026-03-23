# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A Python CLI tool that copies files from a personal OneDrive to a SharePoint document library using the Microsoft Graph API. Designed for the Airtho M365 tenant. Runs locally on Windows from an admin account.

## Running the Tool

```bash
python migrate.py
```

Install dependencies:
```bash
pip install -r requirements.txt
# packages: msal, requests, inquirer, tqdm, python-dotenv
```

Requires a `.env` file at project root:
```
AZURE_CLIENT_ID=your-app-client-id
AZURE_TENANT_ID=your-tenant-id
```

## Architecture

The tool is orchestrated by `migrate.py` and splits concerns across these modules:

- **`auth.py`** — MSAL device code flow; caches tokens to `token_cache.json` so re-auth isn't needed every run
- **`graph.py`** — all Graph API calls: enumerate folders/files, trigger server-side copy, poll async job, fetch verification metadata
- **`prompts.py`** — all `inquirer` terminal selection menus (source UPN, source folder, SP site, destination library/folder)
- **`batch.py`** — enumerates one level deep inside the selected root folder; each immediate child = one batch; files at the root = "Root files" batch; runs batches sequentially
- **`verify.py`** — post-copy manifest comparison (size + quickXorHash per file)
- **`report.py`** — writes one CSV per batch to `./migration-logs/`

## Key Behaviors

**Copy mechanism:** Server-side copy via `POST /drives/{driveId}/items/{itemId}/copy` — no bytes move through the local machine. Poll the `Location` header URL every 3s, timeout 10 min per item.

**Conflict resolution:** Compare `lastModifiedDateTime` source vs destination before copying. Newer file wins. Destination-newer files are skipped and logged as `SKIPPED_DEST_NEWER`.

**Verification statuses:** `OK`, `SIZE_MISMATCH`, `HASH_MISMATCH`, `MISSING`, `SKIPPED_DEST_NEWER`, `COPY_FAILED`

**CSV output:** `./migration-logs/{date}_{source-folder}_{batch-number}_{batch-name}.csv`

## Auth Setup (Azure)

The app registration needs delegated permissions: `Files.ReadWrite.All`, `Sites.ReadWrite.All`, `User.Read`. Public client / device code flow — no client secret. Redirect URI: `https://login.microsoftonline.com/common/oauth2/nativeclient`.

## Out of Scope (v1)

No parallel batches, no GUI, no mid-batch resume, no source deletion (copy-only).
