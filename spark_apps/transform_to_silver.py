from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lpad, to_date, when
from schemas import FLIGHTS_SCHEMA
import os

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadminpassword")

DELAY_MIN = -300
DELAY_MAX = 720

spark = SparkSession.builder \
    .appName("Flight-Delays-Transformation-Silver") \
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
    print(">>> Đọc dữ liệu flights từ tầng Bronze...")
    df_bronze = spark.read.format("delta").load("s3a://bronze/flights_parquet")
    rows_bronze = df_bronze.count()
    print(f">>> Bronze rows: {rows_bronze:,}")

    print(">>> Schema validation — checking for schema drift...")
    bronze_columns = set(df_bronze.schema.names)
    expected_columns = set(FLIGHTS_SCHEMA.names)
    if bronze_columns != expected_columns:
        missing = expected_columns - bronze_columns
        extra = bronze_columns - expected_columns
        if missing:
            print(f" [WARN] Missing columns: {missing}")
        if extra:
            print(f" [WARN] Unexpected columns: {extra}")
        if missing:
            raise ValueError(f"Schema drift detected — missing columns: {missing}")

    df_full = df_bronze.withColumn(
        "FLIGHT_DATE",
        to_date(
            concat_ws("-",
                      col("YEAR"),
                      lpad(col("MONTH"), 2, "0"),
                      lpad(col("DAY"), 2, "0")),
            "yyyy-MM-dd"
        )
    )

    rows_with_flight_date = df_full.count()
    rows_null_date = df_full.filter(col("FLIGHT_DATE").isNull()).count()
    print(f">>> Invalid FLIGHT_DATE: {rows_null_date:,} / {rows_with_flight_date:,}")

    df_cleaned = df_full.withColumn(
        "DEPARTURE_DELAY",
        when(col("CANCELLED") == 1, 0).otherwise(col("DEPARTURE_DELAY"))
    ).withColumn(
        "ARRIVAL_DELAY",
        when(col("CANCELLED") == 1, 0).otherwise(col("ARRIVAL_DELAY"))
    )

    rows_out_of_range = df_cleaned.filter(
        (col("DEPARTURE_DELAY").isNotNull() & ((col("DEPARTURE_DELAY") < DELAY_MIN) | (col("DEPARTURE_DELAY") > DELAY_MAX)))
        | (col("ARRIVAL_DELAY").isNotNull() & ((col("ARRIVAL_DELAY") < DELAY_MIN) | (col("ARRIVAL_DELAY") > DELAY_MAX)))
    ).count()
    if rows_out_of_range > 0:
        print(f" [WARN] Rows with delay outside [{DELAY_MIN}, {DELAY_MAX}] min: {rows_out_of_range:,}")

    df_final = df_cleaned.drop("YEAR", "MONTH", "DAY")
    rows_silver = df_final.count()
    print(f">>> Silver rows: {rows_silver:,}")

    print("--- Schema tầng Silver ---")
    df_final.printSchema()

    print(">>> Ghi Delta table tầng Silver...")
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://silver/flights_delta")

    print(f" [SUCCESS] Dữ liệu SILVER: {rows_silver:,} rows")

except Exception as e:
    print(f" [ERROR] {e}")
    raise

finally:
    spark.stop()
