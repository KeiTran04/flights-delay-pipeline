# Kiến trúc hệ thống — Flight Delays Medallion Pipeline

```
┌────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    DOCKER COMPOSE (de_network)                                  │
│                                                                                                │
│  ┌───────────────────────┐          ┌───────────────────────┐          ┌───────────────────┐  │
│  │    PostgreSQL 15       │◄────────►│   Hive Metastore      │◄────────►│     Trino 435     │  │
│  │  (metadata backend)    │  JDBC    │   3.1.3 (schema reg)  │  Thrift  │  (SQL query eng)  │  │
│  │  - hive_db             │          │                       │          │  port 8081        │  │
│  │  - airflow_db          │          └──────────┬────────────┘          └────────┬──────────┘  │
│  └───────────────────────┘                     │                              │              │
│                                                │                              │              │
│                                                ▼                              ▼              │
│  ┌───────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              MINIO (S3-compatible)                                     │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │  │
│  │  │  s3://bronze │  │ s3://silver  │  │  s3://gold   │  │s3://warehouse│               │  │
│  │  │  Delta Lake  │  │  Delta Lake  │  │  Delta Lake  │  │  (Trino)     │               │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘               │  │
│  └───────────────────────────────────────────────────────────────────────────────────────┘  │
│                                ▲                        ▲                                   │
│                                │                        │                                   │
│                  ┌─────────────┴─────────────┐          │                                   │
│                  │                           │          │                                   │
│  ┌───────────────┴────────────────┐  ┌───────┴──────────────┐                               │
│  │     Airflow Webserver :8080    │  │ Airflow Scheduler    │                               │
│  │  ┌─────────────────────────┐   │  │                      │                               │
│  │  │  DAG: flight_delays_    │   │  │  Triggers & monitors │                               │
│  │  │  medallion_pipeline     │   │  │  Spark tasks          │                               │
│  │  │                         │   │  └──────────────────────┘                               │
│  │  │  ingest_to_bronze ────┐ │   │                                                         │
│  │  │  transform_to_silver ─┤ │   │                                                         │
│  │  │  transform_to_gold ───┤ │   │                                                         │
│  │  │  register_trino_tables│ │   │                                                         │
│  │  └───────────────────────┘ │   │                                                         │
│  └────────────────────────────┘   │                                                         │
│                                   │                                                         │
│  ┌────────────────────────────────┴───────────────────────┐                                 │
│  │                 PySpark ETL Scripts                     │                                 │
│  │  ingest_to_bronze.py → CSV → Bronze (Delta)            │                                 │
│  │  transform_to_silver.py → Bronze → Silver (clean)      │                                 │
│  │  transform_to_gold.py → Silver → Gold (join, enrich)   │                                 │
│  │  register_trino_tables.py → Hive Metastore registration │                                 │
│  └────────────────────────────────────────────────────────┘                                 │
│                                                                                                │
│  ┌───────────────────────┐          ┌───────────────────────┐                                │
│  │   init-buckets (mc)   │          │     data/ (CSV)       │                                │
│  │  Tạo 4 buckets trên   │          │  - flights.csv        │                                │
│  │  MinIO sau khi khởi   │          │  - airlines.csv       │                                │
│  │  động                 │          │  - airports.csv       │                                │
│  └───────────────────────┘          └───────────────────────┘                                │
│                                                                                                │
└────────────────────────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────────────────┐
                              │  Streamlit Dashboard    │
                              │  (localhost:8501)        │
                              │  - KPIs                 │
                              │  - Top airlines         │
                              │  - Trends by month      │
                              │  - Delay reasons (pie)  │
                              │  - Airport map (geo)    │
                              │                         │
                              │  Trino client ←───┐     │
                              └──────────────────┘ │     │
                                                   │     │
                              BI Tools (Tableau,   │     │
                              Power BI, JDBC) ─────┘     │
                                                   │
                              Trino CLI ───────────┘
                              (port 8081)
```

---

## Luồng dữ liệu (Data Flow)

```
[CSV files] ──► ingest_to_bronze ──► s3://bronze/ (Delta)
                    │
                    ▼
          transform_to_silver ──► s3://silver/ (Delta, cleaned)
                    │
                    ▼
          transform_to_gold ──► s3://gold/ (Delta, enriched)
                    │
                    ▼
          register_trino_tables ──► Hive Metastore ──► Trino
                                                          │
                    ┌─────────────────────────────────────┘
                    ▼
          Streamlit Dashboard / BI Tools
```

---

## Bảng thành phần

| Service | Container | Port | Vai trò |
|---------|-----------|------|---------|
| MinIO | `de_minio` | 9000 (API), 9001 (Console) | Object storage — Data Lake |
| PostgreSQL | `de_postgres` | 5432 | Metadata backend cho Hive & Airflow |
| Hive Metastore | `de_hive_metastore` | 9083 | Schema registry (Delta tables) |
| Trino | `de_trino` | 8081 | SQL query engine |
| Airflow Webserver | `de_airflow_webserver` | 8080 | Pipeline UI & orchestration |
| Airflow Scheduler | `de_airflow_scheduler` | — | Trigger & monitor tasks |
| init-buckets | `de_init_buckets` | — | Tạo buckets trên MinIO (1 lần) |
