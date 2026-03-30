import os
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_BASE_DIR = Path(__file__).resolve().parents[2]
_load_env_file(_BASE_DIR / ".env")
_load_env_file(Path(__file__).resolve().parents[1] / ".env")

# ── 通用 ──
OFFICE_API_TOKEN   = os.getenv("OFFICE_API_TOKEN", "")
OFFICE_API_TIMEOUT = float(os.getenv("OFFICE_API_TIMEOUT", "30"))

_BASE   = os.getenv("OFFICE_API_BASE_URL", "https://ahyg.online-office.net")
_APP    = os.getenv("OFFICE_APP_ID",       "51ba0d9e0f8ebfbd3bed1bc8")

def _url(entry_id: str, action: str = "data_count") -> str:
    return f"{_BASE}/openapi/v1/app/{_APP}/entry/{entry_id}/{action}"

# ── 各表接口地址（data_count 用于统计数量）──
ENTRY_CUSTOMER_ID    = os.getenv("ENTRY_CUSTOMER",    "503e3fa088717299600a46f5")
ENTRY_FOLLOWUP_ID    = os.getenv("ENTRY_FOLLOWUP",    "5c5f98606d6aba37c3eead64")
ENTRY_RECEPTION_ID   = os.getenv("ENTRY_RECEPTION",   "f1aa4b82881f55a84894b06d")
ENTRY_CLUE_ID        = os.getenv("ENTRY_CLUE",        "55978023b70d62687ee11897")
ENTRY_OPPORTUNITY_ID = os.getenv("ENTRY_OPPORTUNITY", "5cce95109bd26d02432c141b")

URL_CUSTOMER_COUNT    = _url(ENTRY_CUSTOMER_ID)
URL_FOLLOWUP_COUNT    = _url(ENTRY_FOLLOWUP_ID)
URL_RECEPTION_COUNT   = _url(ENTRY_RECEPTION_ID)
URL_CLUE_COUNT        = _url(ENTRY_CLUE_ID)
URL_OPPORTUNITY_COUNT = _url(ENTRY_OPPORTUNITY_ID)
