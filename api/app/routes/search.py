from flask import Blueprint, request, jsonify
from sqlalchemy import func, or_
from app.services.db import SessionLocal
from app.models.models import NriCounty, CityCountyXwalk
import re
import traceback

search_bp = Blueprint("search", __name__)

# Support both code and name; expand as you add states
STATE_TO_FIPS = {"VA": "51"}
STATE_WORDS = {"virginia", "va"}
NOISE_WORDS = {"county", "city", "parish", "borough", "va", "virginia"}




# Virginia = VA
def normalize_state(raw):
    if not raw:
        return None, None
    s = raw.strip()
    # accept 'VA' or 'Virginia'
    if len(s) == 2:
        code = s.upper()
        return code, STATE_TO_FIPS.get(code)
    # full name
    name = s.lower()
    if name == "virginia":
        return "VA", STATE_TO_FIPS["VA"]
    return s.upper(), None  # unknown code; will fallback to state name match



# X, VA = X
def normalize_q(raw: str) -> str:
    """'Charlotte, Virginia' -> 'charlotte'; strip common suffixes."""
    if not raw:
        return ""
    s = raw.strip().lower()
    parts = [p.strip() for p in re.split(r"[,/]+", s) if p.strip()]
    if parts and parts[-1] in STATE_WORDS:
        parts = parts[:-1]
    s = " ".join(parts)
    tokens = [t for t in re.split(r"\s+", s) if t and t not in NOISE_WORDS]
    return " ".join(tokens)


'''
# Checks if the zip code is 5 digits or not 
def is_zip(txt: str) -> bool:
    return bool(re.fullmatch(r"\d{5}", (txt or "").strip()))
'''

# If the search x is contained within anny county or city, theyll be displayed 
def build_name_filters(norm: str):
    if not norm:
        return None
    first = norm.split()[0]
    prefix = f"{first}%"
    contains = f"%{norm}%"
    return or_(
        func.lower(NriCounty.county).like(prefix),
        func.lower(NriCounty.county).like(contains),
        func.lower(CityCountyXwalk.city).like(prefix),
        func.lower(CityCountyXwalk.city).like(contains),
    )

# --- add this helper near the top of the file ---
def build_state_rank_map(session, state_code, fips_prefix):
    """
    Returns (rank_map, total):
      rank_map[county_fips] = dense rank (1 = lowest risk)
      total = number of counties considered
    """
    # Scope by state (prefer FIPS)
    base = session.query(NriCounty.county_fips, NriCounty.risk_score)
    if fips_prefix:
        base = base.filter(NriCounty.county_fips.like(fips_prefix + "%"))
    elif state_code:
        base = base.filter(func.lower(NriCounty.state).like(f"%{state_code.lower()}%"))

    rows = base.all()
    # Sort by ascending risk (lower risk is better). Push None to the end.
    rows_sorted = sorted(
        rows,
        key=lambda r: (r.risk_score is None, float(r.risk_score) if r.risk_score is not None else 0.0)
    )

    rank_map = {}
    rank = 0
    prev = None
    for r in rows_sorted:
        # dense-rank: same risk score => same rank
        curr = None if r.risk_score is None else float(r.risk_score)
        if curr != prev:
            rank += 1
            prev = curr
        rank_map[r.county_fips] = rank
    return rank_map, len(rows_sorted)


@search_bp.route("/api/search", methods=["POST"])
def search():
    data = request.get_json(silent=True) or {}

    # Inputs
    state_in = (data.get("state") or "").strip()
    q_raw    = (data.get("q") or "").strip()
    limit_in = data.get("limit", None)   # None => no cap
    page_in  = data.get("page", 1)

    # Normalize inputs
    state_code, fips_prefix = normalize_state(state_in)
    q_norm = normalize_q(q_raw)

    s = SessionLocal()
    try:
        # Base query: VA via FIPS prefix if available; else try state column
        qry = s.query(NriCounty)

        if fips_prefix:
            qry = qry.filter(NriCounty.county_fips.like(fips_prefix + "%"))
        else:
            # Fallback: try matching the state column (handles 'Virginia' stored as text)
            if state_code:
                # allow both 'VA' and 'Virginia' style values
                qry = qry.filter(func.lower(NriCounty.state).like(f"%{state_code.lower()}%"))

        # If q provided, allow match by county OR via city crosswalk
        if q_norm:
            # join xwalk (left) to enable city-based filtering
            qry = qry.outerjoin(
                CityCountyXwalk,
                (CityCountyXwalk.county_fips == NriCounty.county_fips)
                # If your xwalk also stores state, you can add: & (CityCountyXwalk.state.ilike("%virginia%")) for safety
            )
            flt = build_name_filters(q_norm)
            if flt is not None:
                qry = qry.filter(flt)

        # Sort by lowest FEMA/NRI risk first (lower risk is better)
        qry = qry.order_by(NriCounty.risk_score.asc())        
        rows = qry.all()
        
        
        # Format response + scores
        results = []
        for r in rows:
            try:
                risk = float(r.risk_score) if r.risk_score is not None else None
            except Exception:
                risk = None

            overall = None if risk is None else round(100.0 - risk, 1)
            
            
            state_rank_map, state_total = build_state_rank_map(s, state_code, fips_prefix)
            sr = state_rank_map.get(r.county_fips)
            
            results.append({
                "geo_id": r.county_fips,
                "name": f"{r.county}, {r.state}",
                # keep both keys for compatibility with your UI/history
                "fema_risk_score": risk,
                "fema_risk_rating": risk,
                "state_rank": sr,          # NEW: rank among all counties in the state
                "overall_score": overall,
            })

        # Rank (higher overall is better)
        results.sort(key=lambda x: (x["overall_score"] is not None, x["overall_score"]), reverse=True)
        for i, r in enumerate(results, start=1):
            r["rank"] = i

        return jsonify(results), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({"code": "SERVER_ERROR", "message": str(e)}), 500
    finally:
        s.close()

