from sqlalchemy import func
from sqlalchemy.sql import literal_column


def normalize(value, p10, p90, inverse=False):
    # Guard against degenerate ranges
    if p90 <= p10:
        return 50.0
    x = (value - p10) / (p90 - p10)
    x = max(0.0, min(1.0, x))
    score = 100.0 * (1.0 - x if inverse else x)
    return round(score, 1)



def get_percentiles(session):
    # Using percentile_cont for robust ranges
    P = {}
    q = session.query(
        func.percentile_cont(0.10).within_group(Metrics.median_price).label("p10_price"),
        func.percentile_cont(0.90).within_group(Metrics.median_price).label("p90_price"),
        func.percentile_cont(0.10).within_group(Metrics.median_rent).label("p10_rent"),
        func.percentile_cont(0.90).within_group(Metrics.median_rent).label("p90_rent"),
        func.percentile_cont(0.10).within_group(Metrics.crime_index).label("p10_crime"),
        func.percentile_cont(0.90).within_group(Metrics.crime_index).label("p90_crime"),
        func.percentile_cont(0.10).within_group(Metrics.school_index).label("p10_school"),
        func.percentile_cont(0.90).within_group(Metrics.school_index).label("p90_school"),
        func.percentile_cont(0.10).within_group(Metrics.flood_risk).label("p10_flood"),
        func.percentile_cont(0.90).within_group(Metrics.flood_risk).label("p90_flood"),
        func.percentile_cont(0.10).within_group(Metrics.income_median).label("p10_income"),
        func.percentile_cont(0.90).within_group(Metrics.income_median).label("p90_income"),
    )
    row = q.one()
    for k in row._mapping:
        P[k] = float(row._mapping[k]) if row._mapping[k] is not None else 0.0
    return P
