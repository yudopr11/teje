# Teje - ETL Pipeline with Airflow + PostgreSQL

ETL project for processing Transjakarta transaction data using Apache Airflow and PostgreSQL. This pipeline performs extraction, transformation, and loading of bus and station transaction data into a data warehouse with aggregations based on card type, route, and fare.

## 📋 Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Running the Project](#running-the-project)
- [Data Structure](#data-structure)
- [Pipeline Flow](#pipeline-flow)
- [Stopping Services](#stopping-services)

## Overview

- **Automated ETL Pipeline**: Pipeline runs daily at 07:00 WIB
- **Data Warehouse**: Uses PostgreSQL with staging and cube schemas
- **Data Aggregation**: Generates aggregations based on:
  - Card type
  - Route
  - Fare
- **CSV Export**: Aggregated results are exported to CSV files
- **Docker-based**: All components run in Docker containers


- **Infrastructure**: The ETL stack infrastructure consists of multiple Docker containers orchestrated via Docker Compose

  [![](https://mermaid.ink/img/pako:eNqVVdtuozAQ_RXLUqVWSpoLSSBoVakNaRZt2rAhbaRNVpUbXGIFMDLQq_rvO9iEJbR9KA_IPjrH45kzA294wz2KTYzk4wsSb9HiYh3luyS7V8AaW3yzowKNeBjzhKJx9MgEj0IapWusyDXBORMPAX9CLhWPbEOTKi1_zh17RRSnSWKWAI2KH_eidbak9-jGRqbRNtp_D0Xu6OfYKmXJZku9LChkC5LskKsgFvk1pXU-uXPms1Ep9ojfjAWHmyVcHQAU5BCRfBQvZ_Nf43kpfeJiVwQdUQj_gpYSqakWc3syqchSwXyfikJp0QcqBPVQfu-kJrWv7UWpYxFLpcSGBSMBeyUp41FFQiPv_-ZTNyySEuSmXBCf1p1wJqtjhyepL6j7e4rMfk_rynhNVFwBWRcF4D1t76BqHiAntTvPx5btro7n1GMJMgeaPqxW6EJwqNDJty59xSFhLsCP-pUvp7MlVPYS7gZNafbhqQYrhN-v0C0PsvBjr9rXzs1i5QGjxaI4S1sqmHuLLllA6-bNbhYlnWdpyR8_x1yk4DgIk4_t6YLCT1plK0KD5M6D03XydAbkgO_JsvGnsP0sX7lQy6MjtB9KFqVURCRAGx5FdCODKJKqLGo2z5ShCoVhlZAzUXs5h18gFdl-6A6Yapi-girqYn4OmPlgHADVNyQoXcxT3NPBCclXIRTqTCSkfKofAJUvPgt53-3TcKVin88n2cn866WoBi0jgFN-eXQl89zVWk64gX3BPGw-kCChDRxSEZJ8j99yzhqnWxrCPJuw9IjY5X37DqKYRH84D7GZigxkgmf-tjwki6EzqcUIdH5YogIahYoRz6IUm9qgLw_B5ht-xmZXG54ODb2nGZ1-t631dL2BX7Cpd06HnW7b6Ax6_WG_3TfeG_hVhm2fGrpuGFpHG3T1bk9r9xoYPgswklfqZyP_Oe__AD1y2PQ?type=png)](https://mermaid.live/edit#pako:eNqVVdtuozAQ_RXLUqVWSpoLSSBoVakNaRZt2rAhbaRNVpUbXGIFMDLQq_rvO9iEJbR9KA_IPjrH45kzA294wz2KTYzk4wsSb9HiYh3luyS7V8AaW3yzowKNeBjzhKJx9MgEj0IapWusyDXBORMPAX9CLhWPbEOTKi1_zh17RRSnSWKWAI2KH_eidbak9-jGRqbRNtp_D0Xu6OfYKmXJZku9LChkC5LskKsgFvk1pXU-uXPms1Ep9ojfjAWHmyVcHQAU5BCRfBQvZ_Nf43kpfeJiVwQdUQj_gpYSqakWc3syqchSwXyfikJp0QcqBPVQfu-kJrWv7UWpYxFLpcSGBSMBeyUp41FFQiPv_-ZTNyySEuSmXBCf1p1wJqtjhyepL6j7e4rMfk_rynhNVFwBWRcF4D1t76BqHiAntTvPx5btro7n1GMJMgeaPqxW6EJwqNDJty59xSFhLsCP-pUvp7MlVPYS7gZNafbhqQYrhN-v0C0PsvBjr9rXzs1i5QGjxaI4S1sqmHuLLllA6-bNbhYlnWdpyR8_x1yk4DgIk4_t6YLCT1plK0KD5M6D03XydAbkgO_JsvGnsP0sX7lQy6MjtB9KFqVURCRAGx5FdCODKJKqLGo2z5ShCoVhlZAzUXs5h18gFdl-6A6Yapi-girqYn4OmPlgHADVNyQoXcxT3NPBCclXIRTqTCSkfKofAJUvPgt53-3TcKVin88n2cn866WoBi0jgFN-eXQl89zVWk64gX3BPGw-kCChDRxSEZJ8j99yzhqnWxrCPJuw9IjY5X37DqKYRH84D7GZigxkgmf-tjwki6EzqcUIdH5YogIahYoRz6IUm9qgLw_B5ht-xmZXG54ODb2nGZ1-t631dL2BX7Cpd06HnW7b6Ax6_WG_3TfeG_hVhm2fGrpuGFpHG3T1bk9r9xoYPgswklfqZyP_Oe__AD1y2PQ)

  - **Component Descriptions**
    - **PostgreSQL**: Stores both Airflow metadata and the data warehouse (`dwh_prod`) with `staging` and `cube` schemas
    - **Redis**: Message broker for Celery executor, enabling distributed task execution
    - **Airflow Scheduler**: Monitors DAGs and triggers tasks based on schedule
    - **Airflow DAG Processor**: Parses and validates DAG files
    - **Airflow Worker**: Executes tasks using Celery executor
    - **Airflow API Server**: Provides web UI and REST API
    - **Airflow Triggerer**: Handles deferred tasks and sensors
    - **Flower**: Optional monitoring tool for Celery workers

## Prerequisites

Before starting, make sure you have installed:
- **Docker** (version 20.10 or newer)
- **Docker Compose** (version 2.0 or newer)
- **Git** (for cloning the repository)

To verify installation:

```bash
docker --version
docker compose version
```

## Installation

1. **Clone or download this repository**

2. **Create `.env` file in the root directory**

   Create a `.env` file with the following content:

   ```env
    # This file stores environment variables for docker-compose

    # 1. Airflow User
    # These are the credentials for the Airflow web UI
    AIRFLOW_WWW_USER=airflow
    AIRFLOW_WWW_PASS=airflow

    # 2. Database Credentials
    # This is the user and password for BOTH the Airflow metadata DB
    # and dwh_prod database.
    POSTGRES_USER=airflow
    POSTGRES_PASSWORD=airflow
    POSTGRES_DB_AIRFLOW=airflow
    POSTGRES_DB_DWH=dwh_prod
    AIRFLOW_CONN_POSTGRES_DWH=postgresql://airflow:airflow@postgres:5432/dwh_prod

    # 3. Airflow Internal UID
    # Set this to user ID on Linux/macOS to fix permissions
    # Run `id -u` in terminal to get this value
    AIRFLOW_UID=50000
   ```

   **Note**: Change passwords as needed for security.

3. **Prepare input data**

   Ensure the following CSV files exist in the `data/input/` folder:
   - `dummy_routes.csv`
   - `dummy_realisasi_bus.csv`
   - `dummy_shelter_corridor.csv`
   - `dummy_transaksi_bus.csv`
   - `dummy_transaksi_halte.csv`


## Running the Project

### 1. Start Services

Run all services with Docker Compose:

```bash
docker compose up -d
```

This command will:
- Create and run PostgreSQL container
- Create and run Airflow containers (scheduler, webserver, worker, etc.)
- Initialize Airflow database and user

### 2. Wait for Initialization to Complete

Wait a few minutes until all services are ready. You can monitor logs:

```bash
docker compose logs -f
```

Or check service status:

```bash
docker compose ps
```

### 3. Access Airflow UI

After all services are running, open your browser and access:

```
http://localhost:8080
```

Login with:
- **Username**: `airflow` (or match `AIRFLOW_WWW_USER` in `.env`)
- **Password**: `airflow` (or match `AIRFLOW_WWW_USER_PASSWORD` in `.env`)

### 4. Enable DAG

1. In Airflow UI, find the DAG `dag_datapelangan`
2. Toggle the switch on the left side of the DAG name to enable it
3. The DAG will start running according to schedule (daily at 07:00)

### 5. Manual Trigger (Optional)

To run the DAG manually:

1. Click on the DAG `dag_datapelangan`
2. Click the **Play** (▶) button at the top right
3. Select **Trigger DAG w/ config**
4. Select execution date (e.g., `2025-07-01`)
5. Click **Trigger**


## Data Structure

### Input Data

Required CSV files in `data/input/`:

- **dummy_routes.csv**: Bus route data
- **dummy_realisasi_bus.csv**: Bus operation realization data
- **dummy_shelter_corridor.csv**: Shelter to corridor mapping
- **dummy_transaksi_bus.csv**: Bus transaction data
- **dummy_transaksi_halte.csv**: Station transaction data

### Output Data

Generated CSV files in `data/output/`:

- **dummy_agg_by_card_type_{YYYYMMDD}.csv**: Aggregation by card type
- **dummy_agg_by_route_{YYYYMMDD}.csv**: Aggregation by route
- **dummy_agg_by_tarif_{YYYYMMDD}.csv**: Aggregation by fare

File naming format: `tablename_YYYYMMDD_.csv` (example: `dummy_agg_by_card_type_20250701.csv`)

## Pipeline Flow

The pipeline consists of 4 tasks that run sequentially:

```
init_schemas → load_to_staging → transform_in_postgres → export_to_csv
```

### 1. **init_schemas**
- Creates `staging` and `cube` schemas if they don't exist
- Creates required tables:
  - `staging.dummy_union_transaksi`
  - `cube.dummy_agg_by_card_type`
  - `cube.dummy_agg_by_route`
  - `cube.dummy_agg_by_tarif`

### 2. **load_to_staging**
- Reads CSV files from `data/input/`
- Loads data into staging tables:
  - `staging.dummy_routes`
  - `staging.dummy_realisasi_bus`
  - `staging.dummy_shelter_corridor`
  - `staging.dummy_transaksi_bus`
  - `staging.dummy_transaksi_halte`
- Performs minimal data type conversion; does not load raw data exactly

### 3. **transform_in_postgres**
- Combines bus and station transaction data
- Standardizes `no_body_var` format (example: `ABC123` → `ABC-123`)
- Removes duplicates
- Aggregates jumlah_pelanggan and jumlah_amount based on:
  - waktu_transaksi, card_type, gate_in_boo
  - waktu_transaksi, route_code, route_name, gate_in_boo
  - waktu_transaksi, fare_int, gate_in_boo
- Loads aggregation results into cube tables
- Transformation is performed in PostgreSQL (DWH), not in Airflow

### 4. **export_to_csv**
- Exports data from cube tables to CSV files
- Files are saved in `data/output/` with format `tablename_YYYYMMDD.csv`


## Stopping Services

To stop all services:

```bash
docker compose down
```

To stop and remove volumes (data will be lost):

```bash
docker compose down -v
```

## Important Notes

1. **Schedule**: DAG runs daily at 07:00 WIB (UTC+7)
2. **Date Filter**: Pipeline processes data based on DAG execution date
3. **Data Replacement**: Data for the same date will be replaced (delete + insert)
4. **Status Filter**: Only transactions with `status_var = 'S'` are processed
5. The `docker-compose.yaml` file is based on the configuration provided in the official [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html), with several custom modifications applied.

## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
