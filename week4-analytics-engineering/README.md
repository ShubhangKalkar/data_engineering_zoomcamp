# Week 4 — Analytics Engineering with dbt

## Overview

This project builds analytics models on NYC Taxi data using **dbt + BigQuery**.

The pipeline follows a layered approach:

* **Sources (raw)** → External tables in BigQuery (GCS-backed)
* **Staging** → Cleaned and typed datasets
* **Intermediate** → Unioned and standardized trip data
* **Marts** → Business-ready fact tables

## Models Built

### Staging

* `stg_green_tripdata`
* `stg_yellow_tripdata`
* `stg_fhv_tripdata` (Homework model)

### Intermediate

* `int_trips_unioned`

### Marts

* `fct_trips`
* `fct_monthly_zone_revenue`
* `dim_zones`

## Homework Tasks Completed

✔ Built dbt lineage and executed transformations
✔ Implemented tests and validated data quality
✔ Analyzed monthly zone revenue metrics
✔ Added FHV staging model (filtered NULL dispatch bases)
✔ Executed models using `dbt build --target prod`

## Data Location

Raw datasets are stored in **Google Cloud Storage** and queried via **BigQuery external tables**.

(Data files are not included in this repo.)
