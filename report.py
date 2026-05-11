from pathlib import Path

LOGS_DIR = Path("migration-logs")


def _safe(name: str) -> str:
    return name.replace("/", "-").replace(" ", "-").replace("'", "")
