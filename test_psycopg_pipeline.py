import psycopg

# Connect to the database
conn = psycopg.connect("postgres://test:1234@localhost/test?sslmode=disable")

print("=== Small int (42) ===")
cur = conn.execute("SELECT %s", (42,))
result = cur.fetchone()
print(f"Value: {result[0]}, Type: {type(result[0])}, PG type: {cur.description[0].type_code}")

print("\n=== Large int (100000) ===")
cur = conn.execute("SELECT %s", (100000,))
result = cur.fetchone()
print(f"Value: {result[0]}, Type: {type(result[0])}, PG type: {cur.description[0].type_code}")

print("\n=== Very large int (10**18) ===")
cur = conn.execute("SELECT %s", (10**18,))
result = cur.fetchone()
print(f"Value: {result[0]}, Type: {type(result[0])}, PG type: {cur.description[0].type_code}")

print("\n=== Huge int (10**100) ===")
cur = conn.execute("SELECT %s", (10**100,))
result = cur.fetchone()
print(f"Value: {result[0]}, Type: {type(result[0])}, PG type: {cur.description[0].type_code}")

conn.close()
