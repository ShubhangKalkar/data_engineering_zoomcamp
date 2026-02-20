# Week 5 — Data Platforms with Bruin

Overview

In Week 5, I implemented a declarative data pipeline using Bruin to process NYC Taxi data end-to-end — from ingestion to transformed analytical tables — using a lightweight local analytics engine (DuckDB).

This week focused on:
```
1. Data platform concepts (assets, lineage, contracts)
2. Declarative pipeline design
3. Incremental/time-based materialization
4. Built-in data quality checks
5. Reproducible analytics pipelines
```
```
Architecture
Raw Data (NYC Taxi Files)
        ↓
Bruin Assets (Python + SQL)
        ↓
DuckDB Processing Layer
        ↓
Cleaned / Deduplicated Staging Tables
        ↓
Analytics Tables
        ↓
Data Quality Checks + Lineage Graph
```

Bruin acts as:

1. Orchestrator
2. Transformation engine
3. Dependency manager
4. Data quality validator

```
📂 Project Structure
my-pipeline/
├── .bruin.yml                # Connection configuration (DuckDB)
└── pipeline/
    ├── pipeline.yml          # Pipeline definition + variables
    └── assets/
        ├── ingestion/        # Data ingestion logic
        ├── staging/          # Cleaning & deduplication
        └── marts/            # Analytical models
```

Required Components

Bruin projects must contain:
1. .bruin.yml
2. pipeline/pipeline.yml
3. pipeline/assets/

⚙️ Setup Instructions
1️⃣ Install Bruin CLI
curl -LsSf https://getbruin.com/install/cli | sh

Verify installation:

bruin --version
2️⃣ Initialize Project
bruin init zoomcamp my-pipeline
cd my-pipeline
3️⃣ Configure DuckDB Connection

Edit .bruin.yml:

connections:
  duckdb:
    type: duckdb
    path: zoomcamp.duckdb

This creates a local analytical warehouse (no cloud required).

4️⃣ Run the Pipeline
bruin run

First run should use:

bruin run --full-refresh

This ensures tables are created from scratch.

🧠 Materialization Strategy

For monthly NYC taxi data, we used:

time_interval

This allows:

Incremental processing

Partition-aware transformations

Efficient reprocessing by pickup_datetime

🔎 Running Specific Assets with Dependencies

If an upstream file changes:

bruin run --select ingestion.trips+

The + runs all downstream dependencies automatically.

📊 Data Quality Checks

Example check added to ensure required fields:

columns:
  pickup_datetime:
    not_null: true

Bruin automatically validates data contracts during execution.

🔧 Pipeline Variables

Defined inside pipeline.yml:

variables:
  taxi_types:
    default: ["yellow", "green"]

Override at runtime:

bruin run --var 'taxi_types=["yellow"]'
📈 Visualizing Lineage

Bruin provides built-in lineage visualization:

bruin lineage

This generates a dependency graph of the pipeline.