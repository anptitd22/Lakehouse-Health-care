from datetime import datetime, timedelta
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from spark.apps.bronze.upload_raw import upload_folder
from airflow import DAG
from spark.apps.silver.appointments_bronze_to_silver import appointments_bronze_to_silver
from spark.apps.silver.diseases_bronze_to_silver import diseases_bronze_to_silver
from spark.apps.silver.doctors_bronze_to_silver import doctors_bronze_to_silver
from spark.apps.silver.hospital_fee_bronze_to_silver import hospital_fee_bronze_to_silver
from spark.apps.silver.lab_results_bronze_to_silver import lab_results_bronze_to_silver
from spark.apps.silver.patients_bronze_to_silver import patients_bronze_to_silver
from spark.apps.silver.treatments_bronze_to_silver import treatments_bronze_to_silver
from spark.apps.silver.vital_signs_bronze_to_silver import vital_signs_bronze_to_silver
from spark.conf.spark_conf import install_spark_dependencies

default_args = {
    'owner': 'TEST',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'bronze_to_silver',
    default_args=default_args,
    description='bronze_to_silver for health_care data',
    tags=['silver'],
    schedule='*/30 * * * *',  #'0 10 * * *' Chạy hàng ngày 10h
    catchup=False
) as dag:
    with TaskGroup("data_lake") as data_lake_group:
        upload_task = PythonOperator(
            task_id='upload_data',
            python_callable=upload_folder,
            op_kwargs=dict(
                local_folder="/opt/airflow/dataset/local_samples/health_care_raw",
                bucket="lakehouse",
                prefix="bronze/health_care_raw",
                endpoint="http://minio:9000",
                access_key="admin",
                secret_key="admin12345",
            ),
        )
        setup_spark_task = PythonOperator(
            task_id='setup_spark',
            python_callable=install_spark_dependencies,
        )
        appointment_task = PythonOperator(
            task_id='appointment_to_silver',
            python_callable=appointments_bronze_to_silver,
        )
        diseases_task = PythonOperator(
            task_id='diseases_to_silver',
            python_callable=diseases_bronze_to_silver,
        )
        doctors_task = PythonOperator(
            task_id='doctors_to_silver',
            python_callable=doctors_bronze_to_silver,
        )
        hospital_fee_task = PythonOperator(
            task_id='hospital_fee_to_silver',
            python_callable=hospital_fee_bronze_to_silver,
        )
        lab_results_task = PythonOperator(
            task_id='lab_results_to_silver',
            python_callable=lab_results_bronze_to_silver,
        )
        patients_task = PythonOperator(
            task_id='patients_to_silver',
            python_callable=patients_bronze_to_silver,
        )
        treatments_task = PythonOperator(
            task_id='treatments_to_silver',
            python_callable=treatments_bronze_to_silver,
        )
        vital_signs_task = PythonOperator(
            task_id='vital_signs_to_silver',
            python_callable=vital_signs_bronze_to_silver,
        )

        upload_task >> setup_spark_task >> \
            appointment_task >> \
            diseases_task >> \
            doctors_task >> \
            hospital_fee_task >> \
            lab_results_task >> \
            patients_task >> \
            treatments_task >> \
            vital_signs_task

    data_lake_group