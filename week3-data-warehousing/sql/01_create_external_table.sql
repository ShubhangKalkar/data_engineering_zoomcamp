CREATE OR REPLACE EXTERNAL TABLE `zoomcamp_hw3.yellow_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp-486506/yellow_tripdata_2024-*.parquet']
);
