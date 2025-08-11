from flask import Blueprint, request, jsonify
from sqlalchemy.orm import Session
from sqlalchemy import asc
from app.services.db import SessionLocal
from app.models.models import NriCounty, CityCountyXwalk
import traceback

search_bp = Blueprint("search", __name__)



def compute_simple_score(m):
    # demo normalization so numbers look reasonable
    price_s   = max(0, 100 - (m.median_price / 10000))
    rent_s    = max(0, 100 - (m.median_rent / 50))
    crime_s   = max(0, 100 - (m.crime_index * 20))
    school_s  = min(100, m.school_index * 10)
    flood_s   = max(0, 100 - (m.flood_risk * 100))
    income_s  = min(100, m.income_median / 2000)

    overall = 0.2*price_s + 0.15*rent_s + 0.25*crime_s + 0.2*school_s + 0.1*flood_s + 0.1*income_s
    return overall, {
        "price": round(price_s,1),
        "crime": round(crime_s,1),
        "schools": round(school_s,1),
        "weather": round(flood_s,1),
        "income": round(income_s,1)
    }

STATE_TO_FIPS = {"VA":"51", }

@search_bp.route("/api/search", methods=["POST"])
def search():
    data = request.get_json(silent=True) or {}
    state = (data.get("state") or "").strip()
    q = (data.get("q") or "").strip()
    limit = int(data.get("limit", 10))

    s = SessionLocal()

    # FIPS prefix for Virginia (51)
    fips_prefix = "51"

    # Initial query by FIPS prefix
    qry = s.query(NriCounty).filter(NriCounty.county_fips.like(fips_prefix + "%"))

    # Apply city/county search if provided
    if q:
        qry = qry.filter(NriCounty.county.ilike(f"%{q}%"))

    rows = qry.order_by(NriCounty.risk_score.asc()).limit(limit).all()

    # Fallback if no rows found: try text match on state column
    if not rows:
        qry = s.query(NriCounty).filter(NriCounty.state.ilike("%virginia%"))
        if q:
            qry = qry.filter(NriCounty.county.ilike(f"%{q}%"))
        rows = qry.order_by(NriCounty.risk_score.asc()).limit(limit).all()

    formatted = [
        {
            "geo_id": row.county_fips,
            "name": f"{row.county}, {row.state}",
            "fema_risk_rating": row.risk_score,
            "explanation": f"FEMA risk rating for {row.county} is {row.risk_score}."
        }
        for row in rows
    ]

    s.close()
    return jsonify(formatted)
