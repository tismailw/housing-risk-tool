# ingest_acs5_va.py
from pathlib import Path
import os
import re
import pandas as pd
from sqlalchemy import text
from app.services.db import SessionLocal

# ---------------- Path helpers ----------------
def repo_root() -> Path:
    p = Path(__file__).resolve()
    for up in (2, 3, 4):
        root = p.parents[up]
        if (root / "data").exists():
            return root
    return p.parents[3]

def in_docker() -> bool:
    return Path("/.dockerenv").exists()

def default_data_path() -> Path:
    """
    Default location:
      - Docker: /data/raw/cdc_svi_acs5_2024_va_county.xlsx
      - Local:  <repo>/data/raw/cdc_svi_acs5_2024_va_county.xlsx
    """
    if in_docker():
        return Path("/data/raw/cdc_svi_acs5_2024_va_county.xlsx")
    return (repo_root() / "data" / "raw" / "cdc_svi_acs5_2024_va_county.xlsx").resolve()

def get_input_path() -> Path:
    """
    Priority:
      1) ACS5_VA_PATH env var
      2) default_data_path()
    """
    envp = os.environ.get("ACS5_VA_PATH")
    if envp:
        return Path(envp).resolve()
    return default_data_path()

INPUT_PATH = get_input_path()

# Staging table for ACS; override via env if you like
STAGE_TABLE = os.environ.get("ACS5_VA_STAGE", "acs5_va_stage")

# ---------------- IO helpers ----------------
def load_any(path: Path) -> pd.DataFrame:
    s = str(path)
    if s.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(s)        # first sheet by default
    return pd.read_csv(s)

# ---------------- Normalization ----------------
IDENT_RENAMES = {
    "FIPS": "county_fips",
    "GEOID": "county_fips",   # some exports
    "ST": "state",
    "STATE": "state",
    "COUNTY": "county",
    "COUNTYNAME": "county",
    "NAME": "county",         # some ACS extracts use NAME
}

def clean_county_name(name: str) -> str:
    """
    Remove trailing 'County' or 'city' (e.g., 'Accomack County' -> 'Accomack',
    'Alexandria city' -> 'Alexandria'). Also trims optional 'City of ' prefix.
    """
    if name is None:
        return name
    s = str(name).strip()
    s = re.sub(r"\s+(County|county|CITY|City|city)$", "", s).strip()
    s = re.sub(r"^(City of)\s+", "", s, flags=re.IGNORECASE).strip()
    return s

def _pick_series(df: pd.DataFrame, name: str) -> pd.Series:
    """
    Return a single Series even if df[name] is duplicated (DataFrame).
    Keeps the first duplicated column.
    """
    col = df[name]
    if isinstance(col, pd.DataFrame):
        col = col.iloc[:, 0]
    return col

def normalize_and_slice(df: pd.DataFrame):
    """
    - Rename identifiers
    - De-duplicate same-named columns
    - Clean FIPS (5 digits), state (2 letters), county (remove suffix)
    - Identify 'AREA_SQMI' (case-insensitive) and return columns from it to the end
    - Build staged DataFrame with county_fips + acs_*[AREA_SQMI..end]
    """
    # 1) rename IDs; lower-case all others
    cols_norm = {}
    for c in df.columns:
        if c in IDENT_RENAMES:
            cols_norm[c] = IDENT_RENAMES[c]
        else:
            cols_norm[c] = c.lower()
    df = df.rename(columns=cols_norm)

    # 2) ensure required IDs exist (possibly duplicated)
    required = ["county_fips", "state", "county"]
    missing = [r for r in required if r not in df.columns]
    if missing:
        raise KeyError(f"Missing required id columns {missing}. Found: {list(df.columns)}")

    # 2.1) drop duplicate-named columns (keep first)
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    # 3) clean IDs using a single Series each
    cf = _pick_series(df, "county_fips")
    st = _pick_series(df, "state")
    ct = _pick_series(df, "county")

    df["county_fips"] = (
        cf.astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(5)
    )
    df["state"] = st.astype(str).str.strip().str.upper().str[:2]
    df["county"] = ct.astype(str).str.strip().apply(clean_county_name)

    # 4) find area_sqmi and slice from there to the end
    cols = list(df.columns)
    lower_map = {c.lower(): c for c in cols}
    if "area_sqmi" not in lower_map:
        raise KeyError("Could not find column 'AREA_SQMI' (any case) in the input file.")
    start_name = lower_map["area_sqmi"]
    start_idx = cols.index(start_name)
    extra_cols = cols[start_idx:]

    # ensure we keep county_fips for the join
    extras = ["county_fips"] + [c for c in extra_cols if c not in ("county_fips", "state", "county")]

    # 5) numeric coercion for extras (except county_fips)
    for c in extras:
        if c == "county_fips":
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 6) build staged frame and prefix with acs_
    sliced = df[extras].copy()
    rename_map = {c: f"acs_{c}" for c in sliced.columns if c != "county_fips"}
    sliced = sliced.rename(columns=rename_map)

    return df, sliced

# ---------------- Main ----------------
def run():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input not found at {INPUT_PATH}\n"
            "Set ACS5_VA_PATH to override, or place the file at:\n"
            "  - Docker: /data/raw/cdc_svi_acs5_2024_va_county.xlsx\n"
            "  - Local:  <repo>/data/raw/cdc_svi_acs5_2024_va_county.xlsx"
        )

    print(f"[acs5] reading {INPUT_PATH}")
    raw = load_any(INPUT_PATH)
    df_full, df_stage = normalize_and_slice(raw)

    print(f"[acs5] cleaned county names; staging {df_stage.shape[1]-1} ACS columns starting at AREA_SQMI")

    session = SessionLocal()
    try:
        engine = session.get_bind()

        # 1) write the staged ACS columns (county_fips + acs_* columns)
        df_stage.to_sql(STAGE_TABLE, engine, if_exists="replace", index=False)

        with engine.begin() as conn:
            # index stage for fast join
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{STAGE_TABLE}_fips ON "{STAGE_TABLE}"(county_fips);'))

            # 2) add missing columns into nri_county for each acs_* column, pick a reasonable type
            acs_cols = [c for c in df_stage.columns if c != "county_fips"]

            for c in acs_cols:
                dtype = str(df_stage[c].dtype)
                if dtype.startswith("Int"):
                    sqltype = "BIGINT"
                elif dtype.startswith(("float", "Float")):
                    sqltype = "NUMERIC"
                else:
                    sqltype = "NUMERIC"
                conn.execute(text(f'ALTER TABLE nri_county ADD COLUMN IF NOT EXISTS "{c}" {sqltype};'))

            # 3) update nri_county by join on county_fips
            set_clause = ", ".join([f'"{c}" = s."{c}"' for c in acs_cols])
            conn.execute(text(f'''
                UPDATE nri_county n
                SET {set_clause}
                FROM "{STAGE_TABLE}" s
                WHERE s.county_fips = n.county_fips;
            '''))

        print(f"[acs5] merged {len(df_stage)} rows into nri_county (added/updated {len(acs_cols)} acs_* columns)")
    finally:
        session.close()

if __name__ == "__main__":
    run()
