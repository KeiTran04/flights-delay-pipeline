from pyspark.sql import SparkSession
import os

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadminpassword")

spark = SparkSession.builder \
    .appName("Flight-Delays-Ingestion-Bronze") \
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
    .config("spark.sql.warehouse.dir", "s3a://warehouse/") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

datasets = [
    ("airlines.csv", "airlines_parquet"),
    ("airports.csv", "airports_parquet"),
    ("flights.csv", "flights_parquet")
]

for csv_file, parquet_dir in datasets:
    SRC_CSV_PATH = f"/opt/airflow/data/{csv_file}"
    TG_PATH = f"s3a://bronze/{parquet_dir}"

    try:
        print(f"\n>>> BẮT ĐẦU XỬ LÝ FILE: {csv_file}")
        print(f">>> Đọc dữ liệu từ: {SRC_CSV_PATH} ...")

        df = spark.read.csv(SRC_CSV_PATH, header=True, inferSchema=True)

        print(f">>> Ghi Delta table tại {TG_PATH} ...")
        df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(TG_PATH)

        print(f" [SUCCESS] File {csv_file} -> Bronze ({TG_PATH})")

    except Exception as e:
        print(f"[ERROR] {csv_file}: {e}")

spark.stop()
print("\n>>> INGESTION BRONZE HOÀN TẤT.")
