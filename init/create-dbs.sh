#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "hive" --dbname "postgres" <<-EOSQL
    CREATE DATABASE metastore_db;
    CREATE DATABASE airflow_db;
EOSQL
