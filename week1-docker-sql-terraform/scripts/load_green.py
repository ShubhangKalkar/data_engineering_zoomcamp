import pandas as pd
from sqlalchemy import create_engine

PARQUET_FILE = "week1-docker-sql-terraform\scripts\load_green.py"
TABLE_NAME = "green_tripdata_2025_11"

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5433/ny_taxi")

print("Reading parquet...")
df = pd.read_parquet(PARQUET_FILE)

print(f"Writing {len(df):,} rows to Postgres table {TABLE_NAME}...")
df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False, method="multi", chunksize=50_000)

print("Done.")
