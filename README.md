# Bulk Mover

Copies files from a OneDrive source URL into a SharePoint document library using Microsoft Graph. The tool runs server-side copy jobs, so file bytes do not pass through your machine.

The current behavior is intentionally small:

- Select a OneDrive root folder or shortcut, or paste a OneDrive URL.
- Select a followed SharePoint site, or paste a SharePoint site URL.
- Select a destination document library and folder.
- Scan source and destination with Graph delta and visible progress.
- Write a visible plan for every source and destination-only file.
- Show compact terminal progress for folder creation, trash moves, copy submission, and monitoring.
- Run folder creation, copy jobs, and trash moves with a controlled local concurrency window.
- Copy missing files.
- Replace destination files when the source is newer or the size differs.
- Move destination-only files to a root `TRASH` folder in the destination, renamed with their item ID to avoid conflicts.
- Write plain logs and a final CSV report.
- Never modify source files or folders.

## Setup

```bash
pip install -r requirements.txt
```

Create `.env`:

```text
AZURE_CLIENT_ID=your-app-client-id
AZURE_TENANT_ID=your-tenant-id
```

The Azure app needs delegated Microsoft Graph permissions:

- `Files.ReadWrite.All`
- `Sites.ReadWrite.All`
- `User.Read`

## Run

```bash
python migrate.py
```

There are no CLI flags. The script prompts for everything it needs.

## Resume

Each run writes:

```text
migration-logs/<timestamp>_<source>/
  run.json
  plan.csv
  ledger.jsonl
  report.csv
```

`plan.csv` lists every planned action. `ledger.jsonl` is append-only. Each file gets rows such as `SUBMITTED`, `COMPLETED`, `SKIPPED`, or `FAILED`. If a run stops or fails, the next launch offers to resume it and monitors any submitted copy jobs before rebuilding the plan.

## Architecture

| File | Responsibility |
|------|----------------|
| `migrate.py` | Single interactive entrypoint |
| `auth.py` | MSAL device code auth and token cache |
| `graph.py` | Microsoft Graph requests, URL resolution, copy jobs |
| `prompts.py` | Terminal prompts |
| `sync.py` | Delta scan, plan, folder create, copy submit, monitor, trash, report |
| `report.py` | Shared log directory helpers |

Copy, folder, and trash concurrency is controlled by `MAX_ACTIVE_JOBS` in `sync.py`. Copy monitor polling is controlled by `COPY_POLL_INTERVAL`.

## Copy Rules

For each source file:

| Condition | Action |
|-----------|--------|
| Destination file missing | Copy |
| Destination file size differs | Replace |
| Source modified time is newer | Replace |
| Destination appears current | Skip |
| Destination file missing from source | Move to `TRASH` with conflict-safe rename |

Folders are created as needed. Existing destination folders are reused.
