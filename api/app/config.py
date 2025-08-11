import os
from dataclasses import dataclass

@dataclass
class Settings:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://airisk:airisk@db:5432/airisk"
    )
