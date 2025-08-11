import os, csv
from app.services.db import SessionLocal
from app.models.models import NriCounty

CSV_PATH = os.getenv("NRI_VA_CLEAN_CSV", "/data/clean/nri_va_clean.csv")

FIELDS = ["county_fips","county","state","risk_score","flood_score","heat_score",
          "wildfire_score","tornado_score","winter_score","hurricane_score",
          "sovi_score","resilience_score"]

def run():
    session = SessionLocal()
    try:
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data = {k: row.get(k) for k in FIELDS if k in row}
                # coerce empties to None, floats where needed
                for k in data:
                    if k in ("county_fips","county","state"):  # keep strings
                        data[k] = (data[k] or "").strip()
                    else:
                        v = row.get(k, "")
                        data[k] = float(v) if (v not in ("", None)) else None

                obj = session.get(NriCounty, data["county_fips"])
                if obj:
                    for k,v in data.items():
                        setattr(obj, k, v)
                else:
                    obj = NriCounty(**data)
                    session.add(obj)
        session.commit()
        print(f"Ingested NRI VA counties from {CSV_PATH}")
    except:
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    run()
