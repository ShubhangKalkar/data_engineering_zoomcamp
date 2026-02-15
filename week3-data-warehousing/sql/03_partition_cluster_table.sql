CREATE OR REPLACE TABLE `zoomcamp_hw3.yellow_part_clust`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `zoomcamp_hw3.yellow_materialized`;
