import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, current_timestamp, lit
from logs_build.build_logging import setup_logging
from spark.conf.spark_conf import get_spark

PATIENT_PATH = "s3a://lakehouse/bronze/health_care_raw/ingest_dt=*/patient*.parquet"
CATALOG = "local"
DB = "silver"
TABLE = "patient"
# setup_logging()
# logger = logging.getLogger(__name__)

def patients_bronze_to_silver():

    try:
        spark = get_spark()
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")

        patient_raw = (
            spark.read.parquet(PATIENT_PATH)
        )

        df = patient_raw.withColumn(
            'patient'
            , struct(
                col('patient_id').cast('int').alias('patient_id')
                , col('name').cast('string').alias('patient_name')
                , col('gender').cast('string').alias('patient_gender')
                , col('dob').cast('string').alias('patient_dob')
                , col('address').cast('string').alias('patient_address')
                , col('phone').cast('string').alias('patient_phone')
                , col('email').cast('string').alias('patient_email')
                , lit(current_timestamp()).alias("ingested_at")
            )
        )

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.{TABLE} (
          patient_id INTEGER
          , patient_name STRING
          , patient_gender STRING
          , patient_dob STRING
          , patient_address STRING
          , patient_phone STRING
          , patient_email STRING
          , ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (day(ingested_at))
        TBLPROPERTIES ('format-version'='2')
        """)

        (df.select(col("patient.*")).coalesce(6)
            .writeTo(f"{CATALOG}.{DB}.{TABLE}")
         .option("fanout-enabled", "false")
         .option("write.parquet.dictionary-enabled", "false")
         .option("write.parquet.compression-codec", "snappy")
         .option("write.parquet.row-group-size-bytes", str(8 * 1024 * 1024))
         .option("write.parquet.page-size-bytes", str(128 * 1024))
         .option("distribution-mode", "none")
         .append())

        # logger.info(f'Created {CATALOG}.{DB}.patient successfully')
        print(f'Created {CATALOG}.{DB}.{TABLE} successfully')

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
    patients_bronze_to_silver()

