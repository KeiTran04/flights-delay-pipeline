# Flight Delays Medallion Pipeline

> **Phân tích chậm chuyến bay Hoa Kỳ** — Pipeline xử lý 5.8 triệu chuyến bay theo mô hình Medallion Lakehouse (Bronze → Silver → Gold), chạy hoàn toàn trên Docker.

---

## 1. Tổng quan & Kiến trúc

### Mục tiêu

Tự động hóa thu thập, xử lý, và lưu trữ dữ liệu chậm chuyến bay từ nguồn CSV (31 cột, 5.8M records) qua 3 tầng Medallion, phục vụ phân tích KPIs trên Streamlit Dashboard và kết nối BI tools qua Trino.

### Công nghệ sử dụng

| Thành phần | Công nghệ | Vai trò |
| --- | --- | --- |
| **Object Storage** | MinIO (S3-compatible) | Data Lake — lưu Delta Lake files |
| **Metadata** | PostgreSQL 15 | Backend cho Hive Metastore & Airflow |
| **Schema Registry** | Hive Metastore 3.1.3 | Quản lý schema cho Delta tables |
| **Query Engine** | Trino 435 | SQL query siêu tốc, kết nối BI |
| **ETL Engine** | PySpark 3.5.0 + Delta Lake 3.2.0 | Xử lý dữ liệu qua các tầng |
| **Orchestrator** | Apache Airflow 2.7.2 | Điều phối pipeline tự động |
| **Dashboard** | Streamlit + Plotly | Visualize KPIs tương tác |
| **Containerization** | Docker + Docker Compose | Triển khai toàn bộ hạ tầng |

### Kiến trúc Medallion

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AIRFLOW (Orchestrator)                        │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌───────┐ │
│  │ ingest_to    │ → │ transform_to │ → │ transform_to │ → │register│ │
│  │ _bronze      │   │ _silver      │   │ _gold        │   │_tables │ │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘   └───┬───┘ │
└─────────┼──────────────────┼──────────────────┼───────────────┼─────┘
          │                  │                  │               │
          ▼                  ▼                  ▼               ▼
    ┌──────────┐      ┌──────────┐       ┌──────────┐     ┌──────────┐
    │  BRONZE  │ ──→  │  SILVER  │ ──→   │   GOLD   │     │  Trino   │
    │  layer   │      │  layer   │       │  layer   │     │  Tables  │
    └──────────┘      └──────────┘       └──────────┘     └──────────┘
         │                 │                  │                │
         ▼                 ▼                  ▼                ▼
    ╔═══════════════════════════════════════════════════════════════╗
    ║                    MinIO (S3 Data Lake)                       ║
    ║  s3://bronze/  s3://silver/  s3://gold/  s3://warehouse/     ║
    ╚═══════════════════════════════════════════════════════════════╝
                              │
                              ▼
                    ┌─────────────────┐
                    │ Hive Metastore  │ ← PostgreSQL
                    └─────────────────┘
```

#### Vai trò từng tầng

**Bronze Layer** — Raw data nguyên bản từ CSV (31 cột):

- `flights`: 5,819,079 records — thông tin chuyến bay gốc (YEAR, MONTH, AIRLINE, DEPARTURE_DELAY, ARRIVAL_DELAY, CANCELLATION_REASON, ...)
- `airlines`: 14 hãng — mã IATA + tên hãng
- `airports`: 322 sân bay — mã IATA, tên, thành phố, tiểu bang, tọa độ

**Silver Layer** — Dữ liệu đã làm sạch:

- Chuyển đổi YEAR/MONTH/DAY → cột `FLIGHT_DATE` (DATE type)
- Xử lý null: `DEPARTURE_DELAY = 0` nếu chuyến bị hủy (CANCELLED = 1)
- Loại bỏ cột raw dư thừa (YEAR, MONTH, DAY)
- 8 cột cốt lõi: FLIGHT_DATE, AIRLINE_CODE, FLIGHT_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, DEPARTURE_DELAY, ARRIVAL_DELAY, CANCELLED

**Gold Layer** — Business-ready fact table:

- `fact_flights_delay`: JOIN với airlines để enrich tên hãng (`AIRLINE_NAME`)
- 9 cột sẵn sàng cho BI, phân tích, dashboard

---

## 2. Cấu trúc thư mục

```
flight_delays_pipeline/
├── dags/
│   └── flight_delays_pipeline.py     # Định nghĩa DAG Airflow (4 tasks)
├── data/
│   ├── flights.csv                   # 5.8M records (raw input)
│   ├── airlines.csv                  # 14 hãng
│   └── airports.csv                  # 322 sân bay
├── init/
│   └── create-dbs.sh                 # Init script PostgreSQL
├── spark_apps/
│   ├── ingest_to_bronze.py           # ETL: CSV → Delta Bronze
│   ├── transform_to_silver.py        # ETL: Bronze → Silver (clean)
│   ├── transform_to_gold.py          # ETL: Silver → Gold (join enrich)
│   └── register_trino_tables.py      # Đăng ký tables vào Hive Metastore
├── trino/
│   └── catalog/
│       ├── delta.properties          # Trino Delta Lake catalog config
│       └── hive.properties           # Trino Hive catalog config
├── Dockerfile.airflow                # Airflow + Java 11 + PySpark
├── Dockerfile.hive                   # Hive 3.1.3 + S3A + PostgreSQL JDBC
├── docker-compose.yml                # 8 services orchestration
├── app.py                            # Streamlit Dashboard
├── .gitignore
└── README.md
```

---

## 3. Hướng dẫn cài đặt & Khởi chạy

### Yêu cầu

| Yêu cầu | Phiên bản tối thiểu |
| --- | --- |
| Docker Desktop | 24+ (WSL2 backend) |
| RAM | 8 GB (khuyến nghị 16 GB cho 5.8M records) |
| Git | 2.x |
| Python | 3.10+ (cho Streamlit app local) |

### Deploy

```bash
# 1. Clone repo
git clone https://github.com/KeiTran04/flights-delay-pipeline.git
cd flights-delay-pipeline

# 2. Đặt file dữ liệu vào thư mục data/
#    flights.csv, airlines.csv, airports.csv

# 3. Khởi động toàn bộ hạ tầng
docker-compose up -d --build

# 4. Kiểm tra trạng thái
docker-compose ps
```

> **Lưu ý:** Lần đầu build có thể mất 5–10 phút để download images và build custom images.

### Truy cập các giao diện

| Service | URL | Thông tin đăng nhập |
| --- | --- | --- |
| **Airflow Web UI** | http://localhost:8080 | `admin` / `adminpassword` |
| **Trino CLI** | `docker exec -it de_trino trino` | — |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadminpassword` |
| **Streamlit Dashboard** | http://localhost:8501 | — (chạy local `streamlit run app.py`) |

> **Lưu ý về port:** Trino expose port **8081** trên host (do Airflow đã dùng 8080). Kết nối BI tools tới `localhost:8081`.

### Chạy Streamlit Dashboard (local)

```bash
pip install streamlit pandas plotly trino
cd flights-delay-pipeline
streamlit run app.py
```

> Dashboard kết nối tới Trino tại `localhost:8081` mặc định. Có thể ghi đè qua biến môi trường `TRINO_HOST` / `TRINO_PORT`.

---

## 4. Data Pipeline — DAG Workflow

### Cấu trúc DAG

- **DAG ID:** `flight_delays_medallion_pipeline`
- **Schedule:** `@daily` (chạy mỗi ngày 0h UTC)
- **Catchup:** `False` (không chạy bù các ngày trước)

### Các Task

```
ingest_to_bronze >> transform_to_silver >> transform_to_gold >> register_trino_tables
```

| Task | Script | Mô tả | Thời gian chạy (ước lượng) |
| --- | --- | --- | --- |
| `ingest_to_bronze` | `ingest_to_bronze.py` | Đọc CSV → Delta Lake trên MinIO | \~30s |
| `transform_to_silver` | `transform_to_silver.py` | Làm sạch, transform → Delta Silver | \~12s |
| `transform_to_gold` | `transform_to_gold.py` | JOIN airlines → Delta Gold | \~18s |
| `register_trino_tables` | `register_trino_tables.py` | Đăng ký schema & tables vào Hive Metastore qua Trino | \~1s |

### Xử lý lỗi

**Kiểm tra log:**

```bash
# Xem log task cụ thể
docker exec de_airflow_webserver cat /opt/airflow/logs/dag_id=flight_delays_medallion_pipeline/run_id=<run_id>/task_id=<task_id>/attempt=1.log

# Hoặc qua Airflow UI: vào DAG → chọn run → click task → Log
```

**Lỗi thường gặp:**

1. `PATH_NOT_FOUND` — File CSV không tồn tại ở path đã mount. Kiểm tra volume mount trong `docker-compose.yml` (`./data:/opt/airflow/data`).

2. `DELTA_FAILED_TO_MERGE_FIELDS` — Schema mismatch giữa Delta log cũ và DataFrame mới. Xóa Delta log cũ trên MinIO:

   ```bash
   docker exec de_minio mc rm --recursive --force myminio/<bucket>/<path>
   ```

3. `ClassNotFoundException: S3AFileSystem` — Hive Metastore thiếu hadoop-aws jar. Kiểm tra `HADOOP_CLASSPATH` trong entrypoint của `Dockerfile.hive`.

4. `CREATE TABLE … is disallowed` — Trino không cho CREATE TABLE khi Delta data đã tồn tại. Dùng `CALL delta.system.register_table()` — đã được xử lý trong `register_trino_tables.py`.

---

## 5. Truy vấn & Phân tích dữ liệu

### Kết nối Trino CLI

```bash
docker exec -it de_trino trino
```

### Các schemas có sẵn

```sql
SHOW SCHEMAS FROM delta;
-- bronze, silver, gold, information_schema, default
```

### Kiểm tra số dòng

```sql
SELECT 'bronze_flights'  AS layer, COUNT(*) FROM delta.bronze.flights
UNION ALL
SELECT 'bronze_airlines', COUNT(*) FROM delta.bronze.airlines
UNION ALL
SELECT 'bronze_airports', COUNT(*) FROM delta.bronze.airports
UNION ALL
SELECT 'silver_flights',  COUNT(*) FROM delta.silver.flights
UNION ALL
SELECT 'gold_fact',       COUNT(*) FROM delta.gold.fact_flights_delay;
```

### Các câu truy vấn phân tích mẫu

**Top 10 hãng bay trễ nhất:**

```sql
SELECT a.airline,
       COUNT(*)                                        AS num_flights,
       ROUND(AVG(f.arrival_delay), 2)                  AS avg_arrival_delay,
       ROUND(SUM(CASE WHEN f.arrival_delay > 15 THEN 1 ELSE 0 END)
           * 100.0 / NULLIF(COUNT(*), 0), 2)           AS delay_pct
FROM delta.gold.fact_flights_delay f
JOIN delta.bronze.airlines a ON f.airline_code = a.iata_code
GROUP BY a.airline
ORDER BY avg_arrival_delay DESC;
```

**Top 10 sân bay xuất phát có tỷ lệ trễ cao nhất:**

```sql
SELECT ap.airport,
       ap.city,
       ap.state,
       COUNT(*)                                         AS num_departures,
       ROUND(AVG(f.arrival_delay), 2)                   AS avg_arrival_delay,
       ROUND(SUM(CASE WHEN f.arrival_delay > 15 THEN 1 ELSE 0 END)
           * 100.0 / NULLIF(COUNT(*), 0), 2)            AS delay_pct
FROM delta.gold.fact_flights_delay f
JOIN delta.bronze.airports ap ON f.origin_airport = ap.iata_code
GROUP BY ap.airport, ap.city, ap.state
ORDER BY delay_pct DESC;
```

**Xu hướng trễ chuyến theo tháng:**

```sql
SELECT date_trunc('month', flight_date) AS month,
       COUNT(*)                                         AS num_flights,
       ROUND(AVG(arrival_delay), 2)                     AS avg_arrival_delay
FROM delta.gold.fact_flights_delay
GROUP BY 1
ORDER BY 1;
```

**Phân tích nguyên nhân trễ chuyến:**

```sql
SELECT 'Airline Delay'       AS reason, ROUND(SUM(COALESCE(airline_delay,0)),2) AS minutes FROM delta.bronze.flights
UNION ALL
SELECT 'Weather Delay',             ROUND(SUM(COALESCE(weather_delay,0)),2)      FROM delta.bronze.flights
UNION ALL
SELECT 'NAS Delay',                 ROUND(SUM(COALESCE(air_system_delay,0)),2)   FROM delta.bronze.flights
UNION ALL
SELECT 'Late Aircraft Delay',       ROUND(SUM(COALESCE(late_aircraft_delay,0)),2) FROM delta.bronze.flights
UNION ALL
SELECT 'Security Delay',            ROUND(SUM(COALESCE(security_delay,0)),2)     FROM delta.bronze.flights;
```

### Kết nối BI tools

| Tool | Connection String / JDBC URL |
| --- | --- |
| **Tableau / Power BI** | Host: `localhost`, Port: `8081`, Catalog: `delta`, Database: `gold`, User: `admin` |
| **JDBC Driver** | `trino://admin@localhost:8081/delta/gold` |
| **Trino CLI** | `docker exec -it de_trino trino` |

---

## 6. Tham khảo Service Ports & Credentials

| Service | Container name | Host port | Internal port |
| --- | --- | --- | --- |
| MinIO API | `de_minio` | 9000 | 9000 |
| MinIO Console | `de_minio` | 9001 | 9001 |
| PostgreSQL | `de_postgres` | 5432 | 5432 |
| Hive Metastore | `de_hive_metastore` | 9083 | 9083 |
| Trino | `de_trino` | 8081 | 8080 |
| Airflow Webserver | `de_airflow_webserver` | 8080 | 8080 |
| Airflow Scheduler | `de_airflow_scheduler` | — | — |
| Streamlit Dashboard | — (local) | 8501 | 8501 |

**Credentials mặc định:**

| Service | Username | Password |
| --- | --- | --- |
| MinIO | `minioadmin` | `minioadminpassword` |
| PostgreSQL (hive) | `hive` | `hivepassword` |
| PostgreSQL (airflow) | `hive` | `hivepassword` |
| Trino | `admin` | — (no auth) |
| Airflow | `admin` | `adminpassword` |

---

## 7. Troubleshooting

### Container Hive Metastore crash khi tạo schema

```
Cause: ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem
Fix:  Đảm bảo hadoop-aws và aws-java-sdk-bundle version khớp với Hadoop trong Hive image
      (Hive 3.1.3 ships Hadoop 3.1.0 → dùng hadoop-aws 3.1.0 + aws-java-sdk-bundle 1.11.271)
```

### Trino query báo 0 rows dù data đã được ghi

```
Cause: Delta log cũ (từ lần chạy trước) bị schema mismatch với data mới
Fix:  Thêm .option("overwriteSchema", "true") khi Spark write
      Hoặc xóa Delta log cũ: mc rm --recursive --force myminio/<bucket>/<path>
```

### Port conflict giữa Airflow và Trino

```
Cause: Cả Airflow webserver và Trino đều dùng port 8080
Fix:  Trong docker-compose.yml, map Trino ra port 8081 host (đã được cấu hình sẵn)
```

---

## License

MIT