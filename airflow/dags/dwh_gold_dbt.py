from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/spark/apps/gold"
DBT_TARGET = "session"
DBT_BIN = "/home/airflow/.local/bin/dbt"

DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,
    "DBT_TARGET": DBT_TARGET,
    "PYSPARK_SUBMIT_ARGS": " --master spark://spark-master:7077 "
                           "--jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar,"
                                   "/opt/spark/jars/iceberg-nessie-1.9.2.jar,"
                                   "/opt/spark/jars/iceberg-aws-bundle-1.9.2.jar,"
                                   "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
                                   "/opt/spark/jars/aws-java-sdk-bundle-1.12.787.jar "
                           "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                           "--conf spark.sql.defaultCatalog=local "
                           "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog "
                           "--conf spark.sql.catalog.local.type=nessie "
                           "--conf spark.sql.catalog.local.uri=http://nessie:19120/api/v1 "
                           "--conf spark.sql.catalog.local.warehouse=s3a://lakehouse/warehouse "
                           "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
                           "--conf spark.hadoop.fs.s3a.access.key=admin "
                           "--conf spark.hadoop.fs.s3a.secret.key=admin12345 "
                           "--conf spark.hadoop.fs.s3a.path.style.access=true "
                           "--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
                           "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
                           "pyspark-shell"
}

with DAG(
    dag_id="dwh_gold_dbt",
    start_date=datetime(2025, 9, 1),
    schedule="30 1 * * *",
    catchup=False,
    default_args={"retries": 1},
    tags=["dwh","dbt","gold","scd2"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug_gold",
        env=DBT_ENV,
        bash_command=f"""
          set -euo pipefail
          {DBT_BIN} debug \
            --project-dir {DBT_DIR} \
            --profiles-dir {DBT_DIR} \
            --target {DBT_TARGET} \
            --no-partial-parse
        """,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        env=DBT_ENV,
        bash_command=f"""
          set -euo pipefail
          {DBT_BIN} snapshot \
            --project-dir {DBT_DIR} \
            --profiles-dir {DBT_DIR} \
            --target {DBT_TARGET} \
            --no-partial-parse \
            --threads 1
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_gold",
        env=DBT_ENV,
        bash_command=f"""
          set -euo pipefail
          {DBT_BIN} run \
            --project-dir {DBT_DIR} \
            --profiles-dir {DBT_DIR} \
            --target {DBT_TARGET} \
            --select gold_dbt \
            --no-partial-parse \
            --threads 4
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test_gold",
        env=DBT_ENV,
        bash_command=f"""
          set -euo pipefail
          {DBT_BIN} test \
            --project-dir {DBT_DIR} \
            --profiles-dir {DBT_DIR} \
            --target {DBT_TARGET} \
            --select gold_dbt \
            --no-partial-parse \
            --threads 2
        """,
    )

    dbt_debug >> dbt_snapshot >> dbt_run >> dbt_test
