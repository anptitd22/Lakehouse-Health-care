import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.connect.functions import to_date
from pyspark.sql.functions import col, struct, current_timestamp, lit
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

from logs_build.build_logging import setup_logging
from spark.conf.spark_conf import get_spark

APPOINTMENTS_PATH = f"s3a://lakehouse/bronze/health_care_raw/ingest_dt={date.today().isoformat()}/appointments*.parquet"
CATALOG = "local"
DB = "silver"
TABLE = "appointment"

# setup_logging()
# logger = logging.getLogger(__name__)

def appointments_bronze_to_silver():

    try:
        spark = get_spark()
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")

        appointments_raw = (
            spark.read.parquet(APPOINTMENTS_PATH)
        )
        df = (appointments_raw
        .withColumn(
            'appointment'
            , struct(
                col('appointment_id').cast('int').alias('appointment_id')
                , col('patient_id').cast('int').alias('patient_id')
                , col('doctor_id').cast('int').alias('doctor_id')
                , col('appointment_date').cast('string').alias('appointment_date')
                , col('start_time').cast('string').alias('start_time')
                , col('end_time').cast('string').alias('end_time')
                , col('reason').cast('string').alias('reason')
                , col('status').cast('string').alias('status')
                , col('phone').cast('string').alias('phone')
                , col('location').cast('string').alias('location')
                , col('appointment_type').cast('string').alias('appointment_type')
                , lit(current_timestamp()).alias("ingested_at")
            )
        ))

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.{TABLE} (
          appointment_id INTEGER
          , patient_id INTEGER
          , doctor_id INTEGER
          , appointment_date STRING
          , start_time STRING
          , end_time STRING
          , reason STRING
          , status STRING
          , phone STRING
          , location STRING
          , appointment_type STRING
          , ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (day(ingested_at))
        TBLPROPERTIES ('format-version'='2')
        """)

        (df.select(col("appointment.*")).coalesce(6)
            .writeTo(f"{CATALOG}.{DB}.{TABLE}")
            .option("fanout-enabled", "false")
            .option("write.parquet.dictionary-enabled", "false")
            .option("write.parquet.compression-codec", "snappy")
            .option("write.parquet.row-group-size-bytes", str(8 * 1024 * 1024))
            .option("write.parquet.page-size-bytes", str(128 * 1024))
            .option("distribution-mode", "none")
            .append())

        # logger.info(f"Created {CATALOG}.{DB}.appointment successfully.")
        print(f"Created {CATALOG}.{DB}.{TABLE} successfully.")

        today = date.today().isoformat()
        try:
            spark.sql(f"""
                      CALL {CATALOG}.system.rewrite_data_files(
                        table => '{CATALOG}.{DB}.{TABLE}',
                        where => 'day(ingested_at) = DATE ''{today}'''
                      )
                    """)
        except Exception:
            # Fallback: compact toàn bảng (nặng hơn)
            spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{CATALOG}.{DB}.{TABLE}')")

        # Gộp manifests cho nhẹ metadata
        spark.sql(f"CALL {CATALOG}.system.rewrite_manifests(table => '{CATALOG}.{DB}.{TABLE}')")

        print(f"[OK] Wrote & compacted {CATALOG}.{DB}.{TABLE}")

    except Exception as ex:
        # logger.error(ex)
        print(ex)
        raise

if __name__ == "__main__":
    appointments_bronze_to_silver()