# Lakehouse Data Platform — README
A lightweight, containerized data lakehouse for local development that stitches together MinIO (S3), Apache Iceberg with Project Nessie (Git for data), Apache Spark 3.5, Trino, and Airflow 3.0. It’s designed for ELT on Parquet files into Iceberg tables with versioned catalog metadata, queryable via Trino/Spark, and orchestrated by Airflow.

1) Architecture (high level)

<img width="547" height="623" alt="image" src="https://github.com/user-attachments/assets/c15adf01-d2cf-4548-b465-b6659e27e1cc" />

Key ideas:

+ Storage: MinIO emulates S3. Buckets/paths like s3a://lakehouse/bronze/... and s3a://lakehouse/warehouse.

+ Table format: Apache Iceberg (format v2).

+ Catalog: Project Nessie (backed by Postgres) to version table metadata.

+ Compute: Spark master + 2 workers (Bitnami images), plus Trino for interactive SQL.

+ Orchestration: Airflow DAG runs PySpark jobs to move bronze → silver.

2) Stack & Ports

| Component          | Image / Version                                    | Port (host)   | Notes                             |
| ------------------ | -------------------------------------------------- | ------------- | --------------------------------- |
| MinIO              | `quay.io/minio/minio:RELEASE.2025-07-23T15-54-02Z` | `9000/9001`   | `9000` = S3 API, `9001` = Console |
| Nessie API         | `ghcr.io/projectnessie/nessie:latest`              | `19120`       | REST API `/api/v2/...`            |
| Postgres (Nessie)  | `postgres:17`                                      | internal      | Metadata store for Nessie         |
| Spark Master       | `bitnami/spark:3.5.6`                              | `7077, 18080` | `7077` = RPC, `18080` = UI        |
| Spark Workers (x2) | `bitnami/spark:3.5.6`                              | `8082/8083`   | Worker UIs                        |
| Trino              | `trinodb/trino:463`                                | `8088`        | Web UI & JDBC                     |
| Airflow 3.0        | `apache/airflow:3.0.0`                             | `8080`        | Webserver/API                     |

All containers join the same Docker network: lakehouse_network.

3) Folder layout (suggested)

+ airflow - schedule (include private .env difficult .env in root) 

+ data - minIO's data

+ dataset(self-created) - your input data

+ iceberg - snapshot

+ logs_build - save log 

+ spark - compute engine

+ tests - test data (bronze, silver, gold)

+ trino - setup with nessie

+ other (docker, requirements,env)

4) Environment variables (.env sample)

+ one for airflow

+ one for tech main

5) Querying data

ex:

SHOW NAMESPACES IN local;
SHOW TABLES IN local.silver;

SELECT * FROM local.silver.appointment LIMIT 10;

6) Tuning & gotchas

+ Nessie not reachable (ConnectException / UnresolvedAddress)

+ Ensure nessie container is running and on the same Docker network as Spark/Airflow/Trino.

+ If you set NESSIE_VERSION_STORE_PERSIST_JDBC_DATASOURCE, you must also set the named datasource env vars (QUARKUS_DATASOURCE__<NAME>__...). Easiest: remove that var and use the default datasource vars only.

+ AWS SDK v1 deprecation warnings

+ Benign for local use. Ignore or upgrade Hadoop/AWS libs later.

+ OOM during Parquet write (e.g., ByteArrayOutputStream in Parquet compressor)

+ Avoid coalesce(1)—let Iceberg write multiple files.

Prefer moderate partitioning: df.repartition(4-12) based on cluster size & data volume.

Tune writer:

+ write.parquet.compression-codec=snappy

+ write.parquet.row-group-size-bytes=8MB

+ write.parquet.page-size-bytes=128KB

+ Optionally write.target-file-size-bytes=128MB (Iceberg will split as needed).

+ Ensure container/worker memory is sufficient (see Docker limits below).

Docker resource limits

+ spark-master: mem_limit: "2g" (example)

+ spark-worker-*: mem_limit: "8g", set SPARK_WORKER_MEMORY=4g

+ Align Spark job configs: spark.executor.memory, spark.executor.memoryOverhead, spark.driver.memory to fit inside container limits.

S3A + MinIO

+ Must use path.style.access=true and non-SSL endpoint (http://minio:9000) unless you set up TLS.

7) Development workflow

+ Drop new raw files under s3a://lakehouse/bronze/health_care_raw/ingest_dt=YYYY-MM-DD/...

+ Trigger Airflow DAG etl_pipeline (UI or CLI) to populate Iceberg silver tables.

+ Explore data via Spark SQL or Trino.

+ Use Nessie to manage branches/tags for safe schema/data evolution if desired.

8) Troubleshooting checklist

Nessie up?
docker logs -f nessie and curl http://nessie:19120/api/v2/config from a container in the same network.

DNS inside Docker?
docker exec -it <container> getent hosts nessie should return an IP.

Permissions to MinIO?
Confirm keys/endpoint match in Spark/Trino configs. Check MinIO Console (9001) for buckets & objects.

Iceberg table not found from Spark
Ensure you fully qualify names: local.silver.appointment. Run SHOW NAMESPACES IN local; SHOW TABLES IN local.silver;.

Airflow logs show [TABLE_OR_VIEW_NOT_FOUND] local.silver.diseases.diseases
Use writeTo("local.silver.diseases") (table name once, not suffixed). Create namespace first.

9) Security notes

Credentials in this README are placeholders. For real usage, use secrets management and TLS for MinIO/Nessie.

Nessie logs warn if AuthN/AuthZ is disabled. That’s fine for local dev; enable it for shared environments.

10) Future extensions

Add Kafka + schema registry for streaming bronze ( có cc tại ah Đạt bảo dell cần lắm )

Enable Iceberg row-level deletes or CDC.

Use Nessie branches per feature for safe schema evolution & backfills.

Wire BI tools to Trino (JDBC).

11) License / Ownership

This repository is intended for local development and education. Adapt to your organization’s standards before production use.

12) Result

- Airflow:

<img width="1352" height="945" alt="Screenshot from 2025-09-07 15-43-06" src="https://github.com/user-attachments/assets/68b62023-776c-4bb9-a709-3c8047ed9401" />

- Spark(limit):

<img width="1704" height="687" alt="image" src="https://github.com/user-attachments/assets/d6e2e250-21db-4f86-9088-108a6939f234" />

- minIO:

<img width="1782" height="965" alt="image" src="https://github.com/user-attachments/assets/0ce9aad2-47e7-49d0-8974-f236d8609aed" />

- Trino

<img width="693" height="887" alt="Screenshot from 2025-09-07 15-46-16" src="https://github.com/user-attachments/assets/68e96f32-ca12-49df-91c0-febc185d7b62" />

- nessie

<img width="1704" height="687" alt="image" src="https://github.com/user-attachments/assets/b1c88d50-4afc-4d3f-9109-9a1b8bd7045c" />

13) Continuous ... (data warehouse)

# Set up (linux)

Step 1:

+ create .env in folder airflow
+ create .env in folder root

Step 2:

Run both two terminal in project:

~/coder/projects/package_github/Health_Care_Linux$:

+ docker compose build -no--cache
+ docker compose up -d

~/coder/projects/package_github/Health_Care_Linux/airflow: 

+ docker compose build -no--cache
+ docker compose up -d