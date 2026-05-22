from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lpad, to_date, when
import os

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadminpassword")

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
    df_bronze_flights = spark.read.format("delta").load("s3a://bronze/flights_parquet")

    df_transformed = df_bronze_flights.withColumn(
        "FLIGHT_DATE",
        to_date(
            concat_ws("-",
                      col("YEAR"),
                      lpad(col("MONTH"), 2, "0"),
                      lpad(col("DAY"), 2, "0")),
            "yyyy-MM-dd"
        )
    )

    df_cleaned = df_transformed.withColumn(
        "DEPARTURE_DELAY",
        when(col("CANCELLED") == 1, 0).otherwise(col("DEPARTURE_DELAY"))
    ).withColumn(
        "ARRIVAL_DELAY",
        when(col("CANCELLED") == 1, 0).otherwise(col("ARRIVAL_DELAY"))
    )

    df_final = df_cleaned.drop("YEAR", "MONTH", "DAY")

    print("--- Schema tầng Silver ---")
    df_final.printSchema()

    print(">>> Ghi Delta table tầng Silver...")
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/flights_delta")

    print(" [SUCCESS] Dữ liệu tầng SILVER đã sẵn sàng!")

except Exception as e:
    print(f" [ERROR] {e}")
    raise

finally:
    spark.stop()
