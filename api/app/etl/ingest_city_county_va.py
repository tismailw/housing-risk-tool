import csv, os
from app.services.db import SessionLocal
from app.models.models import CityCountyXwalk

CSV_PATH = os.getenv("VA_CITY_COUNTY_CSV", "/data/clean/va_city_to_county.csv")

def run():
    s = SessionLocal()
    try:
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                city = (row["city"] or "").strip()
                state = (row["state"] or "").strip().upper()
                county = (row["county"] or "").strip()
                fips = (row["county_fips"] or "").strip().zfill(5)
                if not city or not state or not fips:
                    continue
                obj = s.get(CityCountyXwalk, {"city": city, "state": state})
                if obj:
                    obj.county = county
                    obj.county_fips = fips
                else:
                    s.add(CityCountyXwalk(city=city, state=state, county=county, county_fips=fips))
        s.commit()
        print(f"Ingested city→county rows from {CSV_PATH}")
    except:
        s.rollback()
        raise
    finally:
        s.close()

if __name__ == "__main__":
    run()
