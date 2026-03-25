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
# packages: msal, requests, inquirer, tqdm, python-dotenv, rich
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
- **`tui.py`** — `rich.Live` dashboard for migration progress; one worker row per active batch; non-TTY fallback to plain timestamped output
- **`verify.py`** — post-copy manifest comparison (size + quickXorHash per file); uses JSON batching for HASH_PENDING retries; supports delta queries for incremental re-verification
- **`report.py`** — writes one CSV per batch to `./migration-logs/`; manages session manifests with runtime params for resume support

## Migration Flow

1. **[2] Shallow scan** (`batch.scan_batches`): One API call lists immediate children of the source root — each subfolder becomes a batch, files at root become "Root files" batch. Fast; just collects `id`, `name`, `childCount`.
2. **[3] User confirms**: Preview table of batches → proceed or abort.
3. **[4] Per-batch deep scan + copy** (parallel, `batch.run_batch`):
   - **Enumerate source** (`graph.enumerate_recursive`): Recursively walks the entire source subfolder to build a full file manifest (id, path, size, quickXorHash). Visible as "scanning" in the TUI. Required for verify-then-skip and post-copy integrity checking — the folder copy API returns no file manifest.
   - **Verify-then-skip** (`_verify_already_copied`): Compares source manifest against dest. If all files already present with matching size/hash, skips copy entirely.
   - **Copy** (`graph.copy_item`): Server-side folder copy. No bytes through local machine.
   - **Poll** (`graph.poll_copy_job`): Exponential backoff until completed/failed.
4. **[5] Verify + report** (sequential, after all copies): Fetches dest metadata, compares size/hash per file, writes CSV and updates session manifest.

## Key Behaviors

**Copy mechanism:** Server-side copy via `POST /drives/{driveId}/items/{itemId}/copy` — no bytes move through the local machine. Polls with exponential backoff (5s→60s cap), no hard timeout.

**Why scan before copy:** The deep per-batch `enumerate_recursive` runs before `copy_item` because: (a) verify-then-skip needs the source file list to compare against dest, and (b) post-copy verification needs a manifest of expected files. The folder copy API gives no per-file receipt.

**Verify-then-skip:** Before re-copying a batch, checks if dest folder already has all files with matching size/hash. Skips copy if complete — prevents duplicate work on re-runs.

**Conflict resolution:** Compare `lastModifiedDateTime` source vs destination before copying. Newer file wins. Destination-newer files are skipped and logged as `SKIPPED_DEST_NEWER`.

**Verification statuses:** `OK`, `OK_SP_OVERHEAD`, `OK_IMAGE_META`, `SIZE_MISMATCH`, `HASH_MISMATCH`, `HASH_PENDING`, `MISSING`, `SKIPPED_DEST_NEWER`, `COPY_FAILED`

**CSV output:** `./migration-logs/{date}_{source-folder}_{batch-number}_{batch-name}.csv`

**Session persistence:** Manifests track `status: in_progress/completed` and store all runtime params. On startup, detects incomplete sessions and offers to resume — skips prompts, re-scans batches, skips completed ones.

**Parallel execution:** Auto-selects `min(folder_batch_count, 6)` workers. `--parallel N` overrides. Root files always run sequentially first. `rich.Live` TUI shows one row per active worker; plain timestamped output in non-TTY environments.

**Verification:** All verify paths (menu `[Verify]`, `[Verify (manual)]`, `--verify-only`) use a single delta walk of the dest root — one API pass enumerates all files, then every source manifest file is matched by relative path. No per-folder enumeration. `--verify-only MANIFEST` re-verifies a session manifest from the CLI.

## Auth Setup (Azure)

The app registration needs delegated permissions: `Files.ReadWrite.All`, `Sites.ReadWrite.All`, `User.Read`. Public client / device code flow — no client secret. Redirect URI: `https://login.microsoftonline.com/common/oauth2/nativeclient`.

## CLI Flags

```bash
python migrate.py                          # normal interactive run (auto-parallel, live TUI)
python migrate.py --parallel 4             # override to exactly 4 concurrent folder copies
python migrate.py --verify-only path.json  # re-verify from manifest
```

## Out of Scope

No GUI, no mid-batch resume (only inter-batch), no source deletion (copy-only).
