import os
import glob
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

# ---------- CONFIG ----------
INPUT_DIRS = {
    "green": r"data\green",
    "yellow": r"data\yellow",
}
OUT_DIRS = {
    "green": r"data_norm\green",
    "yellow": r"data_norm\yellow",
}

# Canonical schemas (BigQuery/dbt-friendly)
GREEN_SCHEMA = pa.schema([
    ("VendorID", pa.int64()),
    ("lpep_pickup_datetime", pa.timestamp("us")),
    ("lpep_dropoff_datetime", pa.timestamp("us")),
    ("store_and_fwd_flag", pa.string()),
    ("RatecodeID", pa.int64()),
    ("PULocationID", pa.int64()),
    ("DOLocationID", pa.int64()),
    ("passenger_count", pa.int64()),
    ("trip_distance", pa.float64()),
    ("fare_amount", pa.float64()),
    ("extra", pa.float64()),
    ("mta_tax", pa.float64()),
    ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()),
    ("ehail_fee", pa.float64()),                  # <-- force FLOAT64 always
    ("improvement_surcharge", pa.float64()),
    ("total_amount", pa.float64()),
    ("payment_type", pa.int64()),
    ("trip_type", pa.int64()),
    ("congestion_surcharge", pa.float64()),
])

YELLOW_SCHEMA = pa.schema([
    ("VendorID", pa.int64()),
    ("tpep_pickup_datetime", pa.timestamp("us")),
    ("tpep_dropoff_datetime", pa.timestamp("us")),
    ("passenger_count", pa.int64()),
    ("trip_distance", pa.float64()),
    ("RatecodeID", pa.int64()),
    ("store_and_fwd_flag", pa.string()),
    ("PULocationID", pa.int64()),
    ("DOLocationID", pa.int64()),
    ("payment_type", pa.int64()),
    ("fare_amount", pa.float64()),
    ("extra", pa.float64()),
    ("mta_tax", pa.float64()),
    ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()),
    ("improvement_surcharge", pa.float64()),
    ("total_amount", pa.float64()),
    ("congestion_surcharge", pa.float64()),
    ("Airport_fee", pa.float64()),
])

SCHEMAS = {"green": GREEN_SCHEMA, "yellow": YELLOW_SCHEMA}

def safe_cast_col(arr: pa.Array, target_type: pa.DataType) -> pa.Array:
    # If already correct, return
    if arr.type == target_type:
        return arr

    # Convert via string when messy (handles int/double/string mixes)
    # nulls remain nulls
    s = pc.cast(arr, pa.string(), safe=False)
    if pa.types.is_timestamp(target_type):
        # try parsing timestamps
        # Many taxi files already have proper timestamp physical types.
        # If it's string, attempt to parse with compute; if fails, produce nulls.
        # pc.strptime expects format; taxi timestamps are "YYYY-MM-DD HH:MM:SS"
        try:
            return pc.strptime(s, format="%Y-%m-%d %H:%M:%S", unit="us")
        except Exception:
            # fallback: keep nulls
            return pa.array([None] * len(arr), type=target_type)

    if pa.types.is_integer(target_type):
        return pc.cast(s, target_type, safe=False)
    if pa.types.is_floating(target_type):
        return pc.cast(s, target_type, safe=False)
    if pa.types.is_string(target_type):
        return s

    # Default fallback
    return pc.cast(arr, target_type, safe=False)

def normalize_file(in_path: str, out_path: str, schema: pa.Schema):
    table = pq.read_table(in_path)

    # Build normalized columns in required order, creating missing cols as nulls
    cols = []
    for field in schema:
        name = field.name
        target_type = field.type
        if name in table.column_names:
            arr = table[name]
            cols.append(safe_cast_col(arr, target_type))
        else:
            cols.append(pa.array([None] * table.num_rows, type=target_type))

    normalized = pa.Table.from_arrays(cols, schema=schema)

    pq.write_table(
        normalized,
        out_path,
        compression="snappy",
        use_dictionary=True
    )

def main():
    for svc in ["green", "yellow"]:
        in_dir = INPUT_DIRS[svc]
        out_dir = OUT_DIRS[svc]
        schema = SCHEMAS[svc]

        os.makedirs(out_dir, exist_ok=True)
        files = sorted(glob.glob(os.path.join(in_dir, "*.parquet")))

        if not files:
            print(f"[{svc}] No parquet files found in {in_dir}")
            continue

        print(f"[{svc}] Normalizing {len(files)} files...")
        for i, fp in enumerate(files, 1):
            base = os.path.basename(fp)
            out_fp = os.path.join(out_dir, base)
            normalize_file(fp, out_fp, schema)
            if i % 5 == 0 or i == len(files):
                print(f"  done {i}/{len(files)}: {base}")

    print("✅ Normalization complete.")

if __name__ == "__main__":
    main()
