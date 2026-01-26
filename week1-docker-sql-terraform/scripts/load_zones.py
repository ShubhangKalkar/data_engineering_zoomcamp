import pandas as pd
from sqlalchemy import create_engine

CSV_FILE = "week1-docker-sql-terraform\scripts\load_green.py"   # change if your file name is different
TABLE_NAME = "taxi_zone_lookup"

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5433/ny_taxi")

print("Reading zones CSV...")
df = pd.read_csv(CSV_FILE)
df.columns = [c.strip() for c in df.columns]

print(f"Writing {len(df):,} rows to Postgres table {TABLE_NAME}...")
df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False, method="multi", chunksize=10_000)

print("Done.")
