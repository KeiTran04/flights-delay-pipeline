import os
import time

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = os.environ.get("TRINO_PORT", "8080")

try:
    from trino.dbapi import connect
except ImportError:
    import subprocess
    subprocess.run(["pip", "install", "trino"], check=True)
    from trino.dbapi import connect

SCHEMAS = [
    "CREATE SCHEMA IF NOT EXISTS delta.bronze WITH (location = 's3a://bronze/')",
    "CREATE SCHEMA IF NOT EXISTS delta.silver WITH (location = 's3a://silver/')",
    "CREATE SCHEMA IF NOT EXISTS delta.gold WITH (location = 's3a://gold/')",
]

TABLES = [
    ("bronze", "airlines", "s3a://bronze/airlines_parquet"),
    ("bronze", "airports", "s3a://bronze/airports_parquet"),
    ("bronze", "flights", "s3a://bronze/flights_parquet"),
    ("silver", "flights", "s3a://silver/flights_delta"),
    ("gold", "fact_flights_delay", "s3a://gold/fact_flights_delay"),
]

def wait_for_trino():
    for i in range(30):
        try:
            conn = connect(host=TRINO_HOST, port=int(TRINO_PORT), user="admin")
            conn.cursor().execute("SELECT 1")
            conn.close()
            print(f">>> Trino ready after {i}s")
            return True
        except Exception:
            print(f">>> Waiting for Trino... ({i}s)")
            time.sleep(2)
    raise RuntimeError("Trino not available after 60s")

def register_tables():
    wait_for_trino()
    conn = connect(host=TRINO_HOST, port=int(TRINO_PORT), user="admin", catalog="delta")
    cursor = conn.cursor()

    for sql in SCHEMAS:
        try:
            cursor.execute(sql)
            print(f" [OK] Schema: {sql[:50]}...")
        except Exception as e:
            print(f" [SKIP] Schema: {sql[:50]}... : {e}")

    for schema, table, location in TABLES:
        try:
            cursor.execute(f"CALL delta.system.register_table('{schema}', '{table}', '{location}')")
            print(f" [OK] Table: {schema}.{table}")
        except Exception as e:
            if "Table already registered" in str(e):
                print(f" [OK] Table already registered: {schema}.{table}")
            else:
                print(f" [SKIP] Table: {schema}.{table} : {e}")

    cursor.close()
    conn.close()
    print(">>> Dang ky table hoan tat!")

if __name__ == "__main__":
    register_tables()
