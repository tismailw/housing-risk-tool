import csv
import os
from app.services.db import SessionLocal
from app.models.models import GeoUnit, Metrics

def upsert_geo(session, geo_type, code, name, state, county):
    gu = session.query(GeoUnit).filter_by(geo_type=geo_type, code=code).one_or_none()
    if not gu:
        gu = GeoUnit(geo_type=geo_type, code=code, name=name, state=state, county=county)
        session.add(gu)
        session.flush()
    else:
        gu.name = name or gu.name
        gu.state = state or gu.state
        gu.county = county or gu.county
    return gu.id

def run(path: str):
    session = SessionLocal()
    try:
        with open(path, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                geo_id = upsert_geo(session,
                                    row["geo_type"].strip(),
                                    row["code"].strip(),
                                    row["name"].strip(),
                                    row.get("state","").strip(),
                                    row.get("county","").strip())
                m = session.query(Metrics).filter_by(geo_id=geo_id).one_or_none()
                vals = dict(
                    median_rent=float(row["median_rent"]),
                    median_price=float(row["median_price"]),
                    crime_index=float(row["crime_index"]),
                    school_index=float(row["school_index"]),
                    flood_risk=float(row["flood_risk"]),
                    income_median=float(row["income_median"]),
                )
                if not m:
                    m = Metrics(geo_id=geo_id, **vals)
                    session.add(m)
                else:
                    for k,v in vals.items():
                        setattr(m, k, v)
        session.commit()
        print(f"Ingest complete: {path}")
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    path = os.getenv("METRICS_CSV", "/data/sample/metrics.csv")
    run(path)
