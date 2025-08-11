from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Float, Integer

class Base(DeclarativeBase):
    pass

class GeoUnit(Base):
    __tablename__ = "geo_unit"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    geo_type: Mapped[str] = mapped_column(String(16))          # e.g., "ZIP", "TRACT"
    code: Mapped[str] = mapped_column(String(16), index=True)  # "20147"
    name: Mapped[str] = mapped_column(String(128))
    state: Mapped[str] = mapped_column(String(2))
    county: Mapped[str] = mapped_column(String(64))

class Metrics(Base):
    __tablename__ = "metrics"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    geo_id: Mapped[int] = mapped_column(Integer, index=True)   # (FK simplified for MVP)
    median_rent: Mapped[float] = mapped_column(Float)
    median_price: Mapped[float] = mapped_column(Float)
    crime_index: Mapped[float] = mapped_column(Float)
    school_index: Mapped[float] = mapped_column(Float)
    flood_risk: Mapped[float] = mapped_column(Float)
    income_median: Mapped[float] = mapped_column(Float)


class NriCounty(Base):
    __tablename__ = "nri_county"
    county_fips: Mapped[str] = mapped_column(String(5), primary_key=True)
    county: Mapped[str] = mapped_column(String(100), index=True)
    state: Mapped[str] = mapped_column(String(2), index=True)
    risk_score: Mapped[float] = mapped_column(Float)

    flood_score: Mapped[float] = mapped_column(Float, nullable=True)
    heat_score: Mapped[float] = mapped_column(Float, nullable=True)
    wildfire_score: Mapped[float] = mapped_column(Float, nullable=True)
    tornado_score: Mapped[float] = mapped_column(Float, nullable=True)
    winter_score: Mapped[float] = mapped_column(Float, nullable=True)
    hurricane_score: Mapped[float] = mapped_column(Float, nullable=True)

    sovi_score: Mapped[float] = mapped_column(Float, nullable=True)
    resilience_score: Mapped[float] = mapped_column(Float, nullable=True)


class CityCountyXwalk(Base):
    __tablename__ = "city_county_xwalk"
    # One row per (city, state)
    city: Mapped[str] = mapped_column(String(100), primary_key=True)
    state: Mapped[str] = mapped_column(String(20), index=True)
    county: Mapped[str] = mapped_column(String(100), index=True)
    county_fips: Mapped[str] = mapped_column(String(5), index=True)
