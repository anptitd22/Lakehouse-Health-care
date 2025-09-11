from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from iceberg.expire_snapshots import iceberg_expire_and_cleanup, RETENTION_DAYS
from spark.conf.spark_conf import get_spark

default_args = {
    "owner": "TEST",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 1),
    "retries": 0,
}

with DAG(
    dag_id="iceberg_maintenance_daily",
    description="Expire snapshots sau 7 ngày + cleanup metadata cho các bảng Iceberg",
    default_args=default_args,
    schedule="30 2 * * *",  # chạy hàng ngày 02:30 (điều chỉnh nếu cần)
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "iceberg"],
) as dag:

    expire_snapshots_task = PythonOperator(
        task_id="expire_snapshots_and_cleanup",
        python_callable=iceberg_expire_and_cleanup,
        op_kwargs={"retention_days": RETENTION_DAYS},
    )

    expire_snapshots_task
