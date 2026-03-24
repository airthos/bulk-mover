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
- **`batch.py`** — enumerates one level deep inside the selected root folder; each immediate child = one batch; files at the root = "Root files" batch; includes verify-then-skip logic to avoid re-copying already-present files
- **`verify.py`** — post-copy manifest comparison (size + quickXorHash per file); uses JSON batching for HASH_PENDING retries; supports delta queries for incremental re-verification
- **`report.py`** — writes one CSV per batch to `./migration-logs/`; manages session manifests with runtime params for resume support

## Key Behaviors

**Copy mechanism:** Server-side copy via `POST /drives/{driveId}/items/{itemId}/copy` — no bytes move through the local machine. Polls with exponential backoff (5s→60s cap), no hard timeout.

**Verify-then-skip:** Before re-copying a batch, checks if dest folder already has all files with matching size/hash. Skips copy if complete — prevents duplicate work on re-runs.

**Conflict resolution:** Compare `lastModifiedDateTime` source vs destination before copying. Newer file wins. Destination-newer files are skipped and logged as `SKIPPED_DEST_NEWER`.

**Verification statuses:** `OK`, `OK_SP_OVERHEAD`, `OK_IMAGE_META`, `SIZE_MISMATCH`, `HASH_MISMATCH`, `HASH_PENDING`, `MISSING`, `SKIPPED_DEST_NEWER`, `COPY_FAILED`

**CSV output:** `./migration-logs/{date}_{source-folder}_{batch-number}_{batch-name}.csv`

**Session persistence:** Manifests track `status: in_progress/completed` and store all runtime params. On startup, detects incomplete sessions and offers to resume — skips prompts, re-scans batches, skips completed ones.

**Parallel execution:** `--parallel N` (max 4) runs folder copies concurrently via ThreadPoolExecutor. Root files always run sequentially first.

**Verification:** All verify paths (menu `[Verify]`, `[Verify (manual)]`, `--verify-only`) use a single delta walk of the dest root — one API pass enumerates all files, then every source manifest file is matched by relative path. No per-folder enumeration. `--verify-only MANIFEST` re-verifies a session manifest from the CLI.

## Auth Setup (Azure)

The app registration needs delegated permissions: `Files.ReadWrite.All`, `Sites.ReadWrite.All`, `User.Read`. Public client / device code flow — no client secret. Redirect URI: `https://login.microsoftonline.com/common/oauth2/nativeclient`.

## CLI Flags

```bash
python migrate.py                          # normal interactive run
python migrate.py --parallel 4             # 4 concurrent folder copies
python migrate.py --verify-only path.json  # re-verify from manifest
```

## Out of Scope

No GUI, no mid-batch resume (only inter-batch), no source deletion (copy-only).
