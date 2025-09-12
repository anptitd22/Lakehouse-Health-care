# Lakehouse Health‑Care

MinIO + Apache Iceberg (catalog Project Nessie) + Spark 3.5 + Trino 463 + Airflow 3.0 + dbt‑spark (method: **session**)

> **TL;DR**: Dự án này triển khai một **data lakehouse** trên MinIO (S3‑compatible) với **Iceberg** làm table format, **Nessie** quản lý metadata/branching, **Spark** để ETL (Bronze→Silver), **dbt‑spark** (session) build **Gold**, và **Trino** để truy vấn/BI. Orchestration bằng **Airflow**.

---

## Kiến trúc tổng quan

```
[Source CSV/Parquet] \
                    \
                     > Airflow.upload_data  ->  MinIO  (bronze/*)
                                                     |
                                                     v
                                      Spark ETL (apps/silver/*)
                                                     |
                                                     v
                                       Iceberg tables (silver)
                                                     |
                                                     v
                                   dbt‑spark (session) build Gold
                                                     |
                                                     v
                               Trino / SparkSQL / BI tools (read Gold)
```

* **Bronze**: lưu trữ thô trên MinIO theo ngày nạp (`ingest_dt=YYYY-MM-DD`).
* **Silver**: chuẩn hoá schema, làm sạch bằng Spark, materialize thành bảng **Iceberg**.
* **Gold**: mô hình hoá phân tích (star/truy vấn nhanh) bằng **dbt‑spark** (incremental MERGE trên Iceberg v2).

---
## Ảnh tổng quan

<div align="center">
  <img width="680" height="772" alt="image" src="https://github.com/user-attachments/assets/051325c4-1e1a-4db1-858d-9dcc9bda595e" />
</div>

## Thành phần & cổng dịch vụ

| Component          | Image / Version                                    | Port (host)   | Notes                             |
| ------------------ | -------------------------------------------------- | ------------- | --------------------------------- |
| MinIO              | `quay.io/minio/minio:RELEASE.2025-07-23T15-54-02Z` | `9000/9001`   | `9000` = S3 API, `9001` = Console |
| Nessie API         | `ghcr.io/projectnessie/nessie:latest`              | `19120`       | REST API `/api/v2/...`            |
| Postgres (Nessie)  | `postgres:17`                                      | internal      | Metadata store for Nessie         |
| Spark Master       | `bitnami/spark:3.5.6`                              | `7077, 18080` | `7077` = RPC, `18080` = UI        |
| Spark Workers (x2) | `bitnami/spark:3.5.6`                              | `8082/8083`   | Worker UIs                        |
| Trino              | `trinodb/trino:463`                                | `8088`        | Web UI & JDBC                     |
| Airflow 3.0        | `apache/airflow:3.0.0`                             | `8080`        | Webserver/API                     |

> Lưu ý: Nếu chạy **2 Postgres** riêng rẽ, đảm bảo **khác cổng/volume** để tránh xung đột.
> 
> All containers join the same Docker network: lakehouse_network.
---

## Quickstart

### 1) Chuẩn bị

* Docker & Docker Compose
* Python 3.10+ (để chạy dbt‑spark)
* install library trong requirements.txt
* Create network:

```bash
docker network create lakehouse_network
```

*) Peter check (thiếu thì đúng david óc):

```bash
docker network ls
docker network inspect lakehouse_net
```

### 2) Biến môi trường (`.env` ở root và airflow ví dụ)

```env
# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin12345

# Nessie (Postgres)
POSTGRES_USER=nessie
POSTGRES_PASSWORD=nessie
POSTGRES_DB=nessie
QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://postgres-nessie:5432/nessie
QUARKUS_DATASOURCE_USERNAME=nessie
QUARKUS_DATASOURCE_PASSWORD=nessie
```

```env
AIRFLOW_UID=50000
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

### 3) Khởi chạy

```bash
# main project
~/coder/projects/package_github/Health_Care_Linux$ docker compose build --no-cache 
~/coder/projects/package_github/Health_Care_Linux$ docker compose up -d 

# airflow
~/coder/projects/package_github/Health_Care_Linux/airflow$ docker compose build --no-cache
~/coder/projects/package_github/Health_Care_Linux/airflow$ docker compose up -d
```

Truy cập:

* MinIO Console: [http://localhost:9001](http://localhost:9001)
* Trino UI: [http://localhost:8088](http://localhost:8088)
* Spark UI: [http://localhost:18080](http://localhost:18080)
* Airflow: [http://localhost:8080](http://localhost:8080)
* Nessie REST: [http://localhost:19120/api/v2](http://localhost:19120/api/v2)

---

## Data layout (S3 paths)

* **Bronze**: `s3a://lakehouse/bronze/health_care_raw/ingest_dt=YYYY-MM-DD/*.csv|*.parquet`
* **Warehouse (Iceberg)**: `s3a://lakehouse/warehouse/<namespace>/<table>`

  * `silver.*`  (Spark viết)
  * `gold.*`    (dbt‑spark viết)

---

## Spark + Iceberg + Nessie (catalog `local`)

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.local.type=rest
spark.sql.catalog.local.uri=http://nessie:19120/api/v2
spark.sql.catalog.local.ref=main
spark.sql.catalog.local.warehouse=s3a://lakehouse/warehouse
spark.sql.catalog.local.io-impl=org.apache.iceberg.aws.s3.S3FileIO

# MinIO S3A
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.access.key={YOUR_ACCESS}
spark.hadoop.fs.s3a.secret.key={YOUR_SECRET}
```

---

## Airflow

* `bronze_to_silver`→  nạp file local lên MinIO **bronze** (Boto3) → Spark apps đọc Bronze → chuẩn hoá → ghi **Iceberg silver**
* `dwh_gold_dbt` → chạy `dbt debug/run/test` cho **gold**
* `iceberg_mainenance_daily` → giảm nhẹ metadata

Ví dụ lịch:

```
0 0 * * *   # bronze_to_silver
0 0 * * *   # dwh_gold_dbt
0 1 * * *   # iceberg_mainenance_daily
```

---

## dbt‑spark (method: session)

### `~/.dbt/profiles.yml`

```yaml
spark_local:
  target: session
  outputs:
    session:
      type: spark
      method: session
      host: NA
      schema: gold
      catalog: local
      connect_retries: 3
      connect_timeout: 10
      threads: 4
      spark_conf:
        spark.master: spark://spark-master:7077
```

### `dbt_project.yml`

```yaml
models:
  healthcare_dwh_gold:
    +materialized: table
    gold_dbt:
      +catalog: local
      +file_format: iceberg

snapshots:
  +catalog: local
  +schema: snapshots
  +file_format: iceberg
```

### Mẫu model Gold (incremental MERGE trên Iceberg v2)

```sql
-- models/gold/dim_diseases.sql
{{ config(materialized='table') }}

WITH src AS (
  SELECT * FROM {{ source('silver', 'diseases') }}
)
SELECT
  abs(xxhash64(cast(disease_id as string)))                AS disease_sk,
  disease_id                                               AS disease_nk,
  first(disease_name, true)                                AS disease_name
FROM src
GROUP BY disease_id;
```

---

## Trino (đọc Iceberg)

`/etc/trino/catalog/iceberg.properties`:

```
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie.uri=http://nessie:19120/api/v2
iceberg.nessie.ref=main
iceberg.file-format=PARQUET
iceberg.s3.endpoint=http://minio:9000
iceberg.s3.path-style-access=true
s3.aws-access-key={YOUR_ACCESS}
s3.aws-secret-key={YOUR_SECRET}
```

Ví dụ truy vấn:

* Vào container trino 

```bash
docker exec -it trino trino
```

```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.gold;
SELECT status, COUNT(*) FROM iceberg.gold.appointments_gold GROUP BY 1;
```

---

## Bảo trì Iceberg (maintenance)

```sql
-- Giảm số lượng file nhỏ
CALL local.system.rewrite_data_files('gold.appointments_gold');
CALL local.system.rewrite_delete_files('gold.appointments_gold');

-- Dọn snapshot cũ (giữ 7 ngày)
CALL local.system.expire_snapshots('gold.appointments_gold', TIMESTAMPADD('DAY', -7, CURRENT_TIMESTAMP));

-- Xoá file mồ côi (tuỳ phiên bản)
CALL local.system.remove_orphan_files('gold.appointments_gold', TIMESTAMPADD('DAY', -7, CURRENT_TIMESTAMP));
```

---

## Data Quality

* **dbt tests**: `unique`, `not_null`, `accepted_values`, `relationships`…
* **Freshness**: `dbt source freshness` cho Silver.
* (Tuỳ chọn) **Great Expectations/Deequ** gần Bronze/Silver hoặc trước khi MERGE vào Gold.

Ví dụ `schema.yml`:

```yaml
models:
  - name: dim_date
    columns:
      - name: date_key
        tests: [not_null, unique]
  - name: dim_patient
    columns:
      - name: patient_sk
        tests: [not_null, unique]
      - name: patient_nk
        tests: [not_null]
  - name: dim_doctor
    columns:
      - name: doctor_sk
        tests: [not_null, unique]
      - name: doctor_nk
        tests: [not_null]
  - name: dim_disease
    columns:
      - name: disease_sk
        tests: [not_null, unique]
      - name: disease_nk
        tests: [not_null]
...
```

---

## Cấu trúc thư mục dự án

```
.
├─ airflow/
│  
├─ dataset/
│  
├─ spark/
│   
├─ trino/
│  
├─ iceberg/ 
│  
├─ docker/
│  └─ compose.yaml
│  
└─ README.md
```

---

## FAQ

**Iceberg có phải database không?** Không. Iceberg là **table format** (quản lý file/manifest/metadata) cho data lake. Metadata được quản lý bởi **Nessie** (branching, versioned catalog) lưu trong **Postgres**.

**DWH nằm ở đâu?** Dữ liệu vật lý (Parquet) nằm trên **MinIO**. Schema/metadata nằm trong Iceberg+Nessie. Truy vấn qua **Trino** hoặc **SparkSQL**.

**Khi nào dùng Trino vs Spark?**

* **Trino**: truy vấn ad‑hoc/BI, low‑latency, federation.
* **Spark**: ETL, batch nặng, write‑heavy, ML.

**Lỗi `hash_partition_count` trong Trino?** Thuộc tính đó **không phải** session property hợp lệ trong bản triển khai hiện tại. Dùng partitioning/sort order ở **table level** (Iceberg) hoặc điều chỉnh tính song song qua cấu hình engine.

**Hai Postgres bị trùng cổng?** Đổi cổng (ví dụ 5432/5433) hoặc gộp vào một service (xem kỹ volume & network).

**S3A bị lỗi path‑style/creds?** Đảm bảo:

* `fs.s3a.path.style.access=true`
* `endpoint=http://minio:9000`
* Access/secret key trùng với MinIO.

**Thiếu jar S3 trên Spark?** Cần `hadoop-aws` + `aws-java-sdk-bundle` phù hợp bản Hadoop.

---

## Lộ trình mở rộng

* Thêm **Kafka** + Schema Registry (ingest streaming) → Silver bằng Spark Structured Streaming. 
* **Great Expectations** kiểm thử dữ liệu tự động trước khi nhập Gold.
* **BI**: Kết nối Superset/Metabase vào Trino (schema `gold`).
* **Perf**: Optimize sort order, clustering, Z‑order (tuỳ engine), compaction lịch tuần.

---

## License & Đóng góp

* (Tuỳ chọn) MIT / Apache‑2.0
* PR/Issue: đặt tiêu đề rõ ràng, đính kèm log, nêu bước tái hiện.

---

## Result (update gold later)

- Airflow:

<img width="1352" height="945" alt="Screenshot from 2025-09-07 15-43-06" src="https://github.com/user-attachments/assets/68b62023-776c-4bb9-a709-3c8047ed9401" />

<img width="1798" height="852" alt="Screenshot from 2025-09-12 09-29-53" src="https://github.com/user-attachments/assets/ec47c038-167c-40cb-810c-1badca634c18" />

<img width="1798" height="967" alt="Screenshot from 2025-09-12 09-39-18" src="https://github.com/user-attachments/assets/c51a47b7-85d0-41d4-8a1a-6c0c2737158a" />

<img width="1798" height="967" alt="Screenshot from 2025-09-12 09-43-54" src="https://github.com/user-attachments/assets/78e61eee-be70-4b78-95a7-883194943841" />

- Spark(limit):

<img width="1704" height="687" alt="image" src="https://github.com/user-attachments/assets/d6e2e250-21db-4f86-9088-108a6939f234" />

- minIO:

<img width="1782" height="965" alt="image" src="https://github.com/user-attachments/assets/0ce9aad2-47e7-49d0-8974-f236d8609aed" />

<img width="1798" height="967" alt="Screenshot from 2025-09-12 09-45-03" src="https://github.com/user-attachments/assets/41802208-4cdf-468d-8e5b-d1db4b375ac3" />

- Trino

<img width="693" height="887" alt="Screenshot from 2025-09-07 15-46-16" src="https://github.com/user-attachments/assets/68e96f32-ca12-49df-91c0-febc185d7b62" />

- nessie

<img width="1704" height="687" alt="image" src="https://github.com/user-attachments/assets/b1c88d50-4afc-4d3f-9109-9a1b8bd7045c" />

<img width="1798" height="967" alt="Screenshot from 2025-09-12 09-45-35" src="https://github.com/user-attachments/assets/d8356f36-2a6a-4608-ba29-44f4013e8b0e" />

<img width="1798" height="967" alt="Screenshot from 2025-09-12 09-45-58" src="https://github.com/user-attachments/assets/dc371cb6-90fb-4302-bb71-5cc1ddaecb54" />

1) Xử lí bigdata (no distributed)

2) Xử lí phân tán (Distributed)