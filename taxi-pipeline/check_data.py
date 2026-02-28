import duckdb

con = duckdb.connect("taxi_pipeline.duckdb")

print(con.execute("SHOW TABLES").fetchall())

print(con.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall())

con.close()