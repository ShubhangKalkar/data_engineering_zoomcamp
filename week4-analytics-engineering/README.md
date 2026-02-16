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


### **Q1. dbt Lineage and Execution**

**Question:**
If you run:

```
dbt run --select int_trips_unioned
```

**Answer:**
**stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned (upstream dependencies)**

**Reason:**
dbt always builds the selected model **plus all upstream dependencies** required to materialize it.
It does NOT build downstream models unless explicitly selected.

---

### **Q2. dbt Tests**

**Answer:**
**dbt will fail the test, returning a non-zero exit code**

**Reason:**
The `accepted_values` test enforces strict validation.
When value `6` appears (not in `[1,2,3,4,5]`), dbt raises a **test failure**.

---

### **Q3. Count of Records in `fct_monthly_zone_revenue`**

**Query Used:**

```sql
SELECT COUNT(*)
FROM `zoomcamp-486506.nytaxi.fct_monthly_zone_revenue`;
```

**Answer:**
**14,120**

---

### **Q4. Best Performing Zone for Green Taxis (2020)**

**Query Used:**

```sql
SELECT pickup_zone,
       SUM(revenue_monthly_total_amount) AS total_revenue
FROM `zoomcamp-486506.nytaxi.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
```

**Answer:**
**East Harlem South**

---

### **Q5. Green Taxi Trip Counts (October 2019)**

**Query Used:**

```sql
SELECT SUM(total_monthly_trips) AS total_trips
FROM `zoomcamp-486506.nytaxi.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND revenue_month = '2019-10-01';
```

**Answer:**
**472,427**

---

### **Q6. Build a Staging Model for FHV Data (2019)**

#### Step 1 — External Table Created

```sql
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-486506.raw.fhv_tripdata`
OPTIONS (
  format = 'csv',
  uris = ['gs://zoomcamp-yellow-2024-shubhangkalkar/fhv/*.csv']
);
```

#### Step 2 — dbt Model Created (`stg_fhv_tripdata.sql`)

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'fhv_tripdata') }}
),

renamed AS (
    SELECT
        dispatching_base_num,
        SAFE_CAST(PULocationID AS INT64) AS pickup_location_id,
        SAFE_CAST(DOLocationID AS INT64) AS dropoff_location_id,
        pickup_datetime,
        dropOff_datetime
    FROM source
    WHERE dispatching_base_num IS NOT NULL
)

SELECT * FROM renamed;
```

#### Step 3 — Count Records

```sql
SELECT COUNT(*)
FROM `zoomcamp-486506.nytaxi.stg_fhv_tripdata`;
```

**Answer:**
**43,244,693**

---

## dbt Command Used to Build Models

```
dbt build --target prod
```

This created:

* `stg_*` models (cleaned staging)
* `int_trips_unioned`
* `fct_trips`
* `fct_monthly_zone_revenue`
* `dim_zones`

---


## Homework Tasks Completed

✔ Built dbt lineage and executed transformations
✔ Implemented tests and validated data quality
✔ Analyzed monthly zone revenue metrics
✔ Added FHV staging model (filtered NULL dispatch bases)
✔ Executed models using `dbt build --target prod`

## Data Location

Raw datasets are stored in **Google Cloud Storage** and queried via **BigQuery external tables**.

(Data files are not included in this repo.)
