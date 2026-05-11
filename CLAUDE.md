# CLAUDE.md

Guidance for this repository.

## What This Is

Bulk Mover is a small Python tool that copies files from a pasted OneDrive source URL into a selected SharePoint document library destination. It uses Microsoft Graph server-side copy jobs.

## Run

```bash
python migrate.py
```

No CLI flags. The script prompts for:

1. OneDrive root folder or shortcut, with URL paste fallback.
2. Followed SharePoint site, with site URL paste fallback.
3. Destination document library.
4. Destination folder.

## Architecture

- `migrate.py`: single interactive entrypoint.
- `auth.py`: MSAL device code flow and token cache.
- `graph.py`: Graph request helpers, pasted URL resolution, copy job polling.
- `prompts.py`: all terminal prompts.
- `sync.py`: file-by-file sync engine, ledger, report writing.
- `report.py`: shared log directory helpers.

## Behavior

The tool scans source and destination first with Graph delta and visible progress, writes `plan.csv`, creates destination folders as needed, submits file copy jobs, then monitors the accepted jobs.
Terminal output should stay compact. For repeated operations, update one progress line with counts, percent, and current path rather than printing one line per item.
Folder creation, copy jobs, and trash moves use local concurrency capped by `MAX_ACTIVE_JOBS`. Copy monitor polling uses `COPY_POLL_INTERVAL`.

For each file:

- Missing destination file: copy.
- Size differs: replace.
- Source modified time is newer: replace.
- Destination appears current: skip.
- Destination-only file: move to `TRASH` at the destination root, renamed with its item ID to avoid conflicts.

Source files and folders are never modified.

## Logs

Each run writes:

```text
migration-logs/<timestamp>_<source>/
  run.json
  plan.csv
  ledger.jsonl
  report.csv
```

`ledger.jsonl` is append-only. A copied file gets a `SUBMITTED` row after Graph accepts the copy job and a `COMPLETED` or `FAILED` row after polling finishes. Resume monitors submitted jobs first, then rebuilds the plan.

## Microsoft Graph Notes

Use file-level copy for overwrite behavior. Graph supports `@microsoft.graph.conflictBehavior=replace` for file items. Folder conflicts can still fail, so this code does not depend on folder-level overwrite.
