# clean_nri_va.py
from pathlib import Path
import os
import pandas as pd


# ---------- Path helpers ----------
def repo_root() -> Path:
    # file: <repo>/api/app/etl/clean_nri_va.py  → parents[3] = <repo>
    return Path(__file__).resolve().parents[3]


def in_docker() -> bool:
    # Heuristic: present when running inside most containers
    return Path("/.dockerenv").exists()


def get_raw_path() -> Path:
    """
    Priority:
      1) NRI_VA_XLSX env var (absolute or relative)
      2) Docker mount:      /data/raw/NRI_Table_Counties_Virginia.csv
      3) Local repo path:   <repo>/data/raw/NRI_Table_Counties_Virginia.csv
    """
    env_path = os.environ.get("NRI_VA_XLSX")
    if env_path:
        p = Path(env_path).resolve()
        if p.exists():
            return p

    docker_p = Path("/data/raw/NRI_Table_Counties_Virginia.csv")
    if docker_p.exists():
        return docker_p

    local_p = repo_root() / "data" / "raw" / "NRI_Table_Counties_Virginia.csv"
    if local_p.exists():
        return local_p

    raise FileNotFoundError(
        f"Raw NRI file not found.\nChecked:\n - {docker_p}\n - {local_p}"
        + (f"\n - {Path(env_path).resolve()}" if env_path else "")
    )


def get_out_path() -> Path:
    """
    Priority:
      1) NRI_VA_CLEAN_CSV env var
      2) Docker default:  /data/clean/nri_va_clean.csv
      3) Local default:   <repo>/data/clean/nri_va_clean.csv
    """
    env_path = os.environ.get("NRI_VA_CLEAN_CSV")
    if env_path:
        return Path(env_path).resolve()

    if in_docker():
        return Path("/data/clean/nri_va_clean.csv")

    return (repo_root() / "data" / "clean" / "nri_va_clean.csv").resolve()


RAW_PATH: Path = get_raw_path()
OUT_PATH: Path = get_out_path()


# ---------- IO helpers ----------
def load_any(path: Path) -> pd.DataFrame:
    path_str = str(path)
    if path_str.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(path_str)
    return pd.read_csv(path_str)


# ---------- Config ----------
CANDIDATES = {
    "county_fips": ["COUNTYFIPS", "CountyFIPS", "FIPS", "GEOID", "County FIPS", "CountyFips"],
    "county":      ["COUNTY", "County", "County Name", "COUNTY_NAME"],
    "state":       ["STATE", "State", "STATEABBR", "State Abbr", "STATE_ABBR"],
    "risk_score":  ["RISK_SCORE", "RiskScore", "Risk Score", "RISK SCORE"],

    # Optional fields
    "sovi_score":        ["SOVI_SCORE", "SOVI Score", "SOVI"],
    "resilience_score":  ["RESL_SCORE", "RESILIENCE_SCORE", "RESL Score", "RESL"],

    # Optional hazard scores
    "flood_score":       ["RFLD_RISK_SCORE", "CFLD_RISK_SCORE", "FLD_RISK_SCORE", "RFLD_RISKSCORE"],
    "heat_score":        ["HWAV_RISK_SCORE", "HEAT_RISK_SCORE", "HWAV_RISKSCORE"],
    "wildfire_score":    ["WFIR_RISK_SCORE", "WFIR_RISKSCORE"],
    "tornado_score":     ["TRND_RISK_SCORE", "TORN_RISK_SCORE", "TRND_RISKSCORE"],
    "winter_score":      ["WNTR_RISK_SCORE", "WINTER_RISK_SCORE", "WNTR_RISKSCORE"],
    "hurricane_score":   ["HRCN_RISK_SCORE", "HURR_RISK_SCORE", "HRCN_RISKSCORE"],
}

STATE_TO_FIPS = {
    "VA": "51", "VIRGINIA": "51",
}


# ---------- ETL ----------
def _pick(colnames, options):
    """Pick the first matching column name; case-insensitive fallback."""
    cols_set = set(colnames)
    for opt in options:
        if opt in cols_set:
            return opt
    lower_map = {c.lower(): c for c in colnames}
    for opt in options:
        cand = lower_map.get(opt.lower())
        if cand:
            return cand
    return None


def run():
    if not RAW_PATH.exists():
        raise FileNotFoundError(
            f"Raw NRI file not found at {RAW_PATH}. "
            "Ensure Docker mounts ./data → /data or the local path exists."
        )

    df = load_any(RAW_PATH)
    cols = list(df.columns)

    # Map canonical names -> actual columns
    mapping = {key: _pick(cols, opts) for key, opts in CANDIDATES.items()}

    # Required columns
    required = ["county_fips", "county", "state", "risk_score"]
    missing = [k for k in required if mapping.get(k) is None]
    if missing:
        raise KeyError(f"Missing required columns {missing}. Found headers: {cols}")

    # Keep only the columns we mapped
    keep_keys = [k for k, v in mapping.items() if v is not None]
    slim = df[[mapping[k] for k in keep_keys]].copy()
    slim.columns = keep_keys

    # Normalize
    slim["county"] = slim["county"].astype(str).str.strip()
    slim["state"] = slim["state"].astype(str).str.strip()

    # Build full 5-digit FIPS: state(2) + county(3)
    def _mk_full_fips(row):
        raw = str(row["county_fips"]).replace(".0", "").strip()
        county3 = raw[-3:].zfill(3)
        state_key = str(row["state"]).strip()
        state_fips = STATE_TO_FIPS.get(state_key) or STATE_TO_FIPS.get(state_key.upper())
        if not state_fips:
            return raw.zfill(5)
        return f"{state_fips}{county3}"

    slim["county_fips"] = slim.apply(_mk_full_fips, axis=1)

    # Coerce numerics if present
    numeric_cols = [
        "risk_score", "sovi_score", "resilience_score",
        "flood_score", "heat_score", "wildfire_score",
        "tornado_score", "winter_score", "hurricane_score",
    ]
    for c in numeric_cols:
        if c in slim.columns:
            slim[c] = pd.to_numeric(slim[c], errors="coerce")

    # Write
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    slim.to_csv(OUT_PATH, index=False)
    print(
        f"Cleaned NRI VA → {OUT_PATH.resolve()} "
        f"with {len(slim)} rows; columns: {list(slim.columns)}"
    )


if __name__ == "__main__":
    run()
