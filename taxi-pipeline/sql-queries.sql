-- Q1: What is the start and end date of the data?
import duckdb

con = duckdb.connect("taxi_pipeline.duckdb")

con.execute("""
SELECT 
    MIN(tpep_pickup_datetime) AS start_date,
    MAX(tpep_pickup_datetime) AS end_date
FROM ny_taxi.yellow_taxi_data
""").fetchall()

-- Q2: What is the percentage of trips that were paid for with credit card?
con.execute("""
SELECT 
    ROUND(
        100.0 * SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS credit_card_percentage
FROM ny_taxi.yellow_taxi_data
""").fetchall()

-- Q3: What is the average trip distance for trips that were paid for with credit card?
con.execute("""
SELECT 
    ROUND(SUM(tip_amount), 2)
FROM ny_taxi.yellow_taxi_data
""").fetchall()