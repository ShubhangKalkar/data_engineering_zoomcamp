-- Q1: What is the percentage of trips that were paid for with credit card?
con.execute("""
SELECT 
    ROUND(
        100.0 * SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS credit_card_percentage
FROM ny_taxi.yellow_taxi_data
""").fetchall()