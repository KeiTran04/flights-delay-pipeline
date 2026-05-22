import os
import time

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = os.environ.get("TRINO_PORT", "8080")

try:
    from trino.dbapi import connect
    from trino.auth import BasicAuthentication
except ImportError:
    import subprocess
    subprocess.run(["pip", "install", "trino"], check=True)
    from trino.dbapi import connect
    from trino.auth import BasicAuthentication

STATEMENTS = [
    # Bronze tables
    "CREATE SCHEMA IF NOT EXISTS delta.bronze WITH (location = 's3a://bronze/')",
    "CREATE TABLE IF NOT EXISTS delta.bronze.airlines (IATA_CODE VARCHAR, AIRLINE VARCHAR) WITH (location = 's3a://bronze/airlines_parquet')",
    "CREATE TABLE IF NOT EXISTS delta.bronze.airports (IATA_CODE VARCHAR, AIRPORT VARCHAR, CITY VARCHAR, STATE VARCHAR, COUNTRY VARCHAR, LATITUDE DOUBLE, LONGITUDE DOUBLE) WITH (location = 's3a://bronze/airports_parquet')",
    "CREATE TABLE IF NOT EXISTS delta.bronze.flights (YEAR INTEGER, MONTH INTEGER, DAY INTEGER, DAY_OF_WEEK INTEGER, AIRLINE VARCHAR, FLIGHT_NUMBER INTEGER, TAIL_NUMBER VARCHAR, ORIGIN_AIRPORT VARCHAR, DESTINATION_AIRPORT VARCHAR, SCHEDULED_DEPARTURE INTEGER, DEPARTURE_TIME INTEGER, DEPARTURE_DELAY INTEGER, TAXI_OUT INTEGER, WHEELS_OFF INTEGER, SCHEDULED_TIME INTEGER, ELAPSED_TIME INTEGER, AIR_TIME INTEGER, DISTANCE INTEGER, WHEELS_ON INTEGER, TAXI_IN INTEGER, SCHEDULED_ARRIVAL INTEGER, ARRIVAL_TIME INTEGER, ARRIVAL_DELAY INTEGER, DIVERTED INTEGER, CANCELLED INTEGER, CANCELLATION_REASON VARCHAR, AIR_SYSTEM_DELAY VARCHAR, SECURITY_DELAY VARCHAR, AIRLINE_DELAY VARCHAR, LATE_AIRCRAFT_DELAY VARCHAR, WEATHER_DELAY VARCHAR) WITH (location = 's3a://bronze/flights_parquet')",

    # Silver tables
    "CREATE SCHEMA IF NOT EXISTS delta.silver WITH (location = 's3a://silver/')",
    "CREATE TABLE IF NOT EXISTS delta.silver.flights (FLIGHT_DATE DATE, AIRLINE_CODE VARCHAR, FLIGHT_NUMBER INTEGER, ORIGIN_AIRPORT VARCHAR, DESTINATION_AIRPORT VARCHAR, DEPARTURE_DELAY INTEGER, ARRIVAL_DELAY INTEGER, CANCELLED INTEGER) WITH (location = 's3a://silver/flights_delta')",

    # Gold tables
    "CREATE SCHEMA IF NOT EXISTS delta.gold WITH (location = 's3a://gold/')",
    "CREATE TABLE IF NOT EXISTS delta.gold.fact_flights_delay (FLIGHT_DATE DATE, AIRLINE_CODE VARCHAR, AIRLINE_NAME VARCHAR, FLIGHT_NUMBER INTEGER, ORIGIN_AIRPORT VARCHAR, DESTINATION_AIRPORT VARCHAR, DEPARTURE_DELAY INTEGER, ARRIVAL_DELAY INTEGER, CANCELLED INTEGER) WITH (location = 's3a://gold/fact_flights_delay')",
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
    for sql in STATEMENTS:
        try:
            cursor.execute(sql)
            print(f" [OK] {sql[:60]}...")
        except Exception as e:
            print(f" [SKIP] {sql[:60]}... : {e}")
    cursor.close()
    conn.close()
    print(">>> Dang ky table hoan tat!")

if __name__ == "__main__":
    register_tables()
