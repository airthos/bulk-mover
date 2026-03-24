# OneDrive → SharePoint Migration Tool

Copies files from a personal OneDrive to a SharePoint document library using the Microsoft Graph API. All copying happens server-side — no file bytes pass through the local machine.

Built for the Airtho M365 tenant. Assumes the destination library is empty.

---

## Setup

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Create a `.env` file at the project root**
```
AZURE_CLIENT_ID=your-app-client-id
AZURE_TENANT_ID=your-tenant-id
```

**3. Register an Azure app** (one-time)

- Azure Portal → App registrations → New registration
- Single tenant, any name (e.g. `airtho-migration`)
- Authentication → Add platform → Mobile and desktop → add redirect URI:
  `https://login.microsoftonline.com/common/oauth2/nativeclient`
- Enable **Allow public client flows**
- API permissions → Add → Microsoft Graph → Delegated:
  - `Files.ReadWrite.All`
  - `Sites.ReadWrite.All`
  - `User.Read`
- Grant admin consent
- Copy **Application (client) ID** and **Directory (tenant) ID** into `.env`

---

## Usage

```bash
python migrate.py                          # interactive run (sequential)
python migrate.py --parallel 4             # 4 concurrent folder copies
python migrate.py --verify-only manifest.json  # re-verify from a session manifest
```

The tool walks you through:

1. **Sign in** — device code flow; opens a browser tab once, then caches the token
2. **Resume?** — if an incomplete session exists, offers to resume it (skips all prompts)
3. **Pick source** — enter the OneDrive user's UPN, then select a folder
4. **Pick destination** — enter the SharePoint site URL, select a document library and optional subfolder
5. **Review batches** — the tool scans one level deep; each subfolder = one batch
6. **Run** — copies each batch, verifies integrity, writes a CSV report per batch

---

## Output

Results are written to `./migration-logs/`:

| File | Description |
|------|-------------|
| `{date}_{folder}_batch-{nn}_{name}.csv` | Per-file result for each batch |
| `{date}_{folder}_session.manifest.json` | Full session record with source metadata and verify status |

### Verification statuses

| Status | Meaning |
|--------|---------|
| `OK` | Size and hash match |
| `OK_SP_OVERHEAD` | Office doc — dest slightly larger due to co-authoring XML (expected) |
| `OK_IMAGE_META` | Image — hash differs due to metadata rewrite (expected) |
| `SIZE_MISMATCH` | File sizes differ |
| `HASH_MISMATCH` | Sizes match but `quickXorHash` differs |
| `HASH_PENDING` | Hash not yet computed by Microsoft after copy; needs follow-up |
| `MISSING` | File not found at destination after copy |
| `COPY_FAILED` | Copy job failed or timed out |

---

## Architecture

| File | Responsibility |
|------|---------------|
| `migrate.py` | Entry point; orchestrates the full flow |
| `auth.py` | MSAL device code flow + token cache |
| `graph.py` | All Graph API calls; throttle/retry wrapper; JSON batching |
| `prompts.py` | Terminal selection menus (`inquirer`) |
| `batch.py` | Batch scanning and per-batch copy orchestration |
| `verify.py` | Post-copy file comparison (size + `quickXorHash`) |
| `report.py` | CSV and manifest JSON writer |
