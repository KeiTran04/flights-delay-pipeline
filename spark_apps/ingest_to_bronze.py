from pyspark.sql import SparkSession
from schemas import FLIGHTS_SCHEMA, AIRLINES_SCHEMA, AIRPORTS_SCHEMA
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
    ("airlines.csv", "airlines_parquet", AIRLINES_SCHEMA),
    ("airports.csv", "airports_parquet", AIRPORTS_SCHEMA),
    ("flights.csv", "flights_parquet", FLIGHTS_SCHEMA),
]

for csv_file, parquet_dir, schema in datasets:
    SRC_CSV_PATH = f"/opt/airflow/data/{csv_file}"
    TG_PATH = f"s3a://bronze/{parquet_dir}"
    BAD_RECORDS_PATH = f"s3a://bronze/_bad_records/{csv_file.replace('.csv', '')}"

    try:
        print(f"\n>>> BẮT ĐẦU XỬ LÝ FILE: {csv_file}")

        df = spark.read \
            .option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .option("badRecordsPath", BAD_RECORDS_PATH) \
            .schema(schema) \
            .csv(SRC_CSV_PATH)

        rows_read = df.count()
        print(f">>> Rows read: {rows_read:,}")

        null_report = {}
        for col_name in df.schema.names:
            null_count = df.filter(df[col_name].isNull()).count()
            if null_count > 0:
                null_report[col_name] = null_count

        if null_report:
            print(">>> NULL values found:")
            for c, n in sorted(null_report.items()):
                print(f"    - {c}: {n:,} / {rows_read:,} ({(n/rows_read)*100:.2f}%)")
        else:
            print(">>> No NULL values — 100% clean")

        print(f">>> Ghi Delta table tại {TG_PATH} ...")
        df.write.mode("overwrite").format("delta").save(TG_PATH)

        print(f" [SUCCESS] File {csv_file} -> Bronze ({rows_read:,} rows)")

    except Exception as e:
        print(f"[ERROR] {csv_file}: {e}")

spark.stop()
print("\n>>> INGESTION BRONZE HOÀN TẤT.")
