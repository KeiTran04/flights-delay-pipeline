from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadminpassword")

spark = SparkSession.builder \
    .appName("Flight-Delays-Analytics-Gold") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.apache.hadoop:hadoop-client:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.directory.marker.retention", "delete") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    print(">>> Nạp dữ liệu từ Silver + Bronze...")
    df_flights = spark.read.format("delta").load("s3a://silver/flights_delta")
    df_airlines = spark.read.format("delta").load("s3a://bronze/airlines_parquet")

    df_flights_ready = df_flights.withColumnRenamed("AIRLINE", "AIRLINE_CODE")

    df_gold = df_flights_ready.join(
        df_airlines,
        df_flights_ready["AIRLINE_CODE"] == df_airlines["IATA_CODE"],
        "left"
    )

    df_fact_flights = df_gold.select(
        df_flights_ready["FLIGHT_DATE"],
        df_flights_ready["AIRLINE_CODE"],
        df_airlines["AIRLINE"].alias("AIRLINE_NAME"),
        df_flights_ready["FLIGHT_NUMBER"],
        df_flights_ready["ORIGIN_AIRPORT"],
        df_flights_ready["DESTINATION_AIRPORT"],
        df_flights_ready["DEPARTURE_DELAY"],
        df_flights_ready["ARRIVAL_DELAY"],
        df_flights_ready["CANCELLED"]
    )

    print("--- Schema tầng Gold ---")
    df_fact_flights.printSchema()
    df_fact_flights.show(5)

    print(">>> Ghi Delta table tầng Gold...")
    df_fact_flights.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://gold/fact_flights_delay")

    print(" [SUCCESS] Dữ liệu tầng GOLD đã sẵn sàng phục vụ BI!")

except Exception as e:
    print(f" [ERROR] Lỗi tầng Gold: {e}")

finally:
    spark.stop()
