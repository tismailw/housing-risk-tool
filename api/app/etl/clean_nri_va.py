import os
import pandas as pd

# Default paths (can be overridden with env vars)
RAW_PATH = os.environ.get(
    "NRI_VA_XLSX",
    "/data/raw/virginia/NRI_Table_Counties_Virginia.csv"  # accept .csv or .xlsx
)
OUT_PATH = os.environ.get("NRI_VA_CLEAN_CSV", "/data/clean/nri_va_clean.csv")

# Column name candidates (NRI releases vary a bit)
CANDIDATES = {
    "county_fips": ["COUNTYFIPS", "CountyFIPS", "FIPS", "GEOID", "County FIPS", "CountyFips"],
    "county":      ["COUNTY", "County", "County Name", "COUNTY_NAME"],
    "state":       ["STATE", "State", "STATEABBR", "State Abbr", "STATE_ABBR"],
    "risk_score":  ["RISK_SCORE", "RiskScore", "Risk Score", "RISK SCORE"],

    # Optional fields (keep if present)
    "sovi_score":        ["SOVI_SCORE", "SOVI Score", "SOVI"],
    "resilience_score":  ["RESL_SCORE", "RESILIENCE_SCORE", "RESL Score", "RESL"],

    # A few hazard scores you might use later (optional)
    "flood_score":       ["RFLD_RISK_SCORE", "CFLD_RISK_SCORE", "FLD_RISK_SCORE", "RFLD_RISKSCORE"],
    "heat_score":        ["HWAV_RISK_SCORE", "HEAT_RISK_SCORE", "HWAV_RISKSCORE"],
    "wildfire_score":    ["WFIR_RISK_SCORE", "WFIR_RISKSCORE"],
    "tornado_score":     ["TRND_RISK_SCORE", "TORN_RISK_SCORE", "TRND_RISKSCORE"],
    "winter_score":      ["WNTR_RISK_SCORE", "WINTER_RISK_SCORE", "WNTR_RISKSCORE"],
    "hurricane_score":   ["HRCN_RISK_SCORE", "HURR_RISK_SCORE", "HRCN_RISKSCORE"],
}

# Map state text -> 2-digit state FIPS (expand as you add states)
STATE_TO_FIPS = {
    "VA": "51", "VIRGINIA": "51",
}

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

def load_any(path: str) -> pd.DataFrame:
    if path.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(path)
    return pd.read_csv(path)

def run():
    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(
            f"Raw NRI file not found at {RAW_PATH}. "
            "Ensure docker-compose mounts ./data to /data and the path is correct."
        )

    df = load_any(RAW_PATH)
    cols = list(df.columns)

    # Build a mapping from our canonical names to actual file columns
    mapping = {key: _pick(cols, opts) for key, opts in CANDIDATES.items()}

    # Required columns
    required = ["county_fips", "county", "state", "risk_score"]
    missing = [k for k in required if mapping.get(k) is None]
    if missing:
        raise KeyError(
            f"Missing required columns {missing}. "
            f"Found headers: {cols}"
        )

    # Keep only the columns we successfully mapped
    keep_keys = [k for k, v in mapping.items() if v is not None]
    slim = df[[mapping[k] for k in keep_keys]].copy()
    slim.columns = keep_keys  # rename to our canonical names

    # Normalize strings
    slim["county"] = slim["county"].astype(str).str.strip()
    slim["state"] = slim["state"].astype(str).str.strip()

    # --- Fix county_fips to be FULL 5-digit FIPS (state+county) ---
    # Your raw file had county code like '00001' (no state). Build '51001' for Virginia.
    def _mk_full_fips(row):
        raw = str(row["county_fips"]).replace(".0", "").strip()
        # keep last 3 digits as county code; pad if necessary
        # handle cases like '1', '001', '00001'
        county3 = raw[-3:].zfill(3)
        state_key = str(row["state"]).strip()
        # map to 2-digit state FIPS (try exact, then uppercase)
        state_fips = STATE_TO_FIPS.get(state_key) or STATE_TO_FIPS.get(state_key.upper())
        if not state_fips:
            # If we don't know the state, attempt to preserve a 5-char code if given
            # or just return the raw (left-padded to 5 so it fits the PK)
            return raw.zfill(5)
        return f"{state_fips}{county3}"

    slim["county_fips"] = slim.apply(_mk_full_fips, axis=1)

    # Coerce numerics
    numeric_cols = [
        "risk_score", "sovi_score", "resilience_score",
        "flood_score", "heat_score", "wildfire_score",
        "tornado_score", "winter_score", "hurricane_score",
    ]
    for c in numeric_cols:
        if c in slim.columns:
            slim[c] = pd.to_numeric(slim[c], errors="coerce")

    # Write cleaned file
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    slim.to_csv(OUT_PATH, index=False)
    print(f"Cleaned NRI VA → {OUT_PATH} with {len(slim)} rows; columns: {list(slim.columns)}")

if __name__ == "__main__":
    run()
