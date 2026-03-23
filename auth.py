import os
from pathlib import Path

import msal
from dotenv import load_dotenv

load_dotenv()

AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"
SCOPES = ["Files.ReadWrite.All", "Sites.ReadWrite.All", "User.Read"]
CACHE_FILE = Path("token_cache.json")


def _load_cache() -> msal.SerializableTokenCache:
    cache = msal.SerializableTokenCache()
    if CACHE_FILE.exists():
        cache.deserialize(CACHE_FILE.read_text())
    return cache


def _save_cache(cache: msal.SerializableTokenCache) -> None:
    if cache.has_state_changed:
        CACHE_FILE.write_text(cache.serialize())


def get_access_token() -> str:
    client_id = os.environ["AZURE_CLIENT_ID"]
    tenant_id = os.environ["AZURE_TENANT_ID"]

    cache = _load_cache()
    app = msal.PublicClientApplication(
        client_id=client_id,
        authority=AUTHORITY.format(tenant_id=tenant_id),
        token_cache=cache,
    )

    # Try silent first (cached token)
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            _save_cache(cache)
            return result["access_token"]

    # Device code flow
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"Failed to create device flow: {flow.get('error_description')}")

    print(f"\n{flow['message']}\n")
    result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        raise RuntimeError(f"Authentication failed: {result.get('error_description')}")

    _save_cache(cache)
    username = result.get("id_token_claims", {}).get("preferred_username", "unknown")
    print(f"Signed in as {username}\n")
    return result["access_token"]
