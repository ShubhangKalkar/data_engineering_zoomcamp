Overview

This week focused on working with BigQuery and understanding how external tables, materialized tables, partitioning, and clustering affect query performance and cost.

The dataset used is the NYC Yellow Taxi Trip Records for January–June 2024, stored as Parquet files in Google Cloud Storage and queried using BigQuery.

Dataset

Source: NYC Taxi & Limousine Commission
Format: Parquet

Files used:
yellow_tripdata_2024-01.parquet
yellow_tripdata_2024-02.parquet
yellow_tripdata_2024-03.parquet
yellow_tripdata_2024-04.parquet
yellow_tripdata_2024-05.parquet
yellow_tripdata_2024-06.parquet

These were uploaded to a Google Cloud Storage bucket and accessed via BigQuery external tables.

Tables Created
    External Table
        Reads Parquet files directly from GCS without storing data inside BigQuery.

    Materialized Table
        Copies data into BigQuery storage for faster repeated querying.

    Partitioned + Clustered Table
    Optimized table:
        Partitioned by tpep_dropoff_datetime
        Clustered by VendorID

This improves query performance and reduces scanned bytes.

Homework Results
Question	Result
Q1 — Record count	20,332,093
Q2 — Data read estimation	0 MB external / 155.12 MB materialized
Q3 — Columnar storage concept	BigQuery scans only requested columns
Q4 — Zero fare trips	8,333
Q5 — Optimization strategy	Partition by datetime + cluster by VendorID
Q6 — Partition benefit	310.24 MB → 26.84 MB
Q7 — External storage location	GCP bucket
Q8 — Always cluster?	False
Q9 — COUNT(*) scan	0 B (metadata-based estimation)
SQL Scripts

All SQL used to create tables and run homework queries is included in:

/sql


Files:

1. create external table
2. create materialized table
3. partition + cluster table
4. homework queries

Key Concepts Practiced:
1. External vs materialized tables
2. Columnar storage behavior
3. Partition pruning
4. Query byte estimation
5. Cost optimization strategies

Notes:

Data was uploaded manually to GCS
External tables use Parquet format
No orchestration tools were used for ingestion

Outcome:

This exercise demonstrates how storage format, table design, and query structure directly impact performance and cost in a cloud data warehouse.