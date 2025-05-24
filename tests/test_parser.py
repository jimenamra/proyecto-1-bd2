from backend.sql_parser import parse_sql

parse_sql("INSERT INTO customer VALUES (45)")
# → ("insert", 45)

parse_sql("SELECT * FROM customer WHERE DNI = 100")
# → ("search", 100)

parse_sql("SELECT * FROM customer WHERE DNI BETWEEN 10 AND 50")
# → ("range_search", 10, 50)

parse_sql("DELETE FROM customer WHERE DNI = 13")
# → ("delete", 13)

#parse_sql("CREATE TABLE customer FROM FILE 'empleados.csv' USING INDEX hash('DNI')")
# → ("create_from_file", "customer", "empleados.csv", "hash", "DNI")
