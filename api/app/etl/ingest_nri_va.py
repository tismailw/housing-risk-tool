# ingest_nri_va.py
from pathlib import Path
import os, csv
from app.services.db import SessionLocal
from app.models.models import NriCounty

# -------- path helpers --------
def repo_root() -> Path:
    p = Path(__file__).resolve()
    for up in (2, 3, 4):
        try_root = p.parents[up]
        if (try_root / "data").exists():
            return try_root
    return p.parents[3]

def in_docker() -> bool:
    return Path("/.dockerenv").exists()

def get_csv_path() -> Path:
    env_path = os.environ.get("NRI_VA_CLEAN_CSV")
    if env_path:
        p = Path(env_path).resolve()
        if p.exists():
            return p
        return p
    if in_docker():
        return Path("/data/clean/nri_va_clean.csv")
    return (repo_root() / "data" / "clean" / "nri_va_clean.csv").resolve()

CSV_PATH = get_csv_path()

FIELDS = [
    "county_fips","county","state","risk_score","flood_score","heat_score",
    "wildfire_score","tornado_score","winter_score","hurricane_score",
    "sovi_score","resilience_score"
]

STATE_ABBR = {
    "VA": "VA",
    "VIRGINIA": "VA",
    # add more if you ingest other states
}

def _coerce_row(row: dict) -> dict:
    data = {k: row.get(k) for k in FIELDS if k in row}

    # strings
    for k in ("county_fips", "county", "state"):
        data[k] = (data.get(k) or "").strip()

    # ensure county_fips is 5-char, left-padded with 0
    if "county_fips" in data:
        raw = data["county_fips"].replace(".0", "").strip()
        data["county_fips"] = raw.zfill(5)

    # --- normalize state to 2-letter abbr (fix for VARCHAR(2)) ---
    if "state" in data:
        s = data["state"]
        if len(s) != 2:
            s = STATE_ABBR.get(s.upper(), s[:2].upper())
        data["state"] = s

    # floats (optional fields become None if empty)
    for k in FIELDS:
        if k in ("county_fips", "county", "state"):
            continue
        v = row.get(k, "")
        data[k] = float(v) if (v not in ("", None)) else None

    return data

def run():
    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"Clean CSV not found at {CSV_PATH}. "
            "Set NRI_VA_CLEAN_CSV or ensure the file exists "
            "at /data/clean (Docker) or <repo>/data/clean (local)."
        )

    session = SessionLocal()
    ingested = 0
    try:
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data = _coerce_row(row)
                key = data.get("county_fips")
                if not key:
                    continue

                obj = session.get(NriCounty, key)
                if obj:
                    for k, v in data.items():
                        setattr(obj, k, v)
                else:
                    obj = NriCounty(**data)
                    session.add(obj)
                ingested += 1

        session.commit()
        print(f"[ingest] {ingested} rows ingested from {CSV_PATH}")
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    run()
