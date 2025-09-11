import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, current_timestamp, lit, coalesce
from logs_build.build_logging import setup_logging
from spark.conf.spark_conf import get_spark

LAB_RESULTS_PATH = f"s3a://lakehouse/bronze/health_care_raw/ingest_dt={date.today().isoformat()}/lab_results*.parquet"
CATALOG = "local"
DB = "silver"
TABLE = "lab_results"
setup_logging()
logger = logging.getLogger(__name__)

def lab_results_bronze_to_silver():
    try:
        spark = get_spark()
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")

        lab_results_raw = (
            spark.read.parquet(LAB_RESULTS_PATH)
        )

        df = lab_results_raw.withColumn(
            'lab_results'
            , struct(
                col('lab_result_id').cast('string').alias('lab_result_id')
                , col('appointment_id').cast('int').alias('appointment_id')
                , col('patient_id').cast('int').alias('patient_id')
                , col('test_type').cast('string').alias('test_type')
                , col('parameter').cast('string').alias('parameter')
                , col('value').cast('string').alias('value')
                , coalesce(col("unit").cast("string"), lit("Unknown")).alias('unit')
                , col('normal_range').cast('string').alias('normal_range')
                , col('interpretation').cast('string').alias('interpretation')
                , col('test_date').cast('string').alias('test_date')
                , lit(current_timestamp()).alias('ingested_at')
            )
        )

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.{TABLE} (
            lab_result_id STRING
            , appointment_id INT
            , patient_id INT
            , test_type STRING
            , parameter STRING
            , value STRING
            , unit STRING
            , normal_range STRING
            , interpretation STRING
            , test_date STRING
            , ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (day(ingested_at))
        TBLPROPERTIES ('format-version'='2')
        """)

        (df.select(col("lab_results.*")).coalesce(6)
            .writeTo(f"{CATALOG}.{DB}.{TABLE}")
         .option("fanout-enabled", "false")
         .option("write.parquet.dictionary-enabled", "false")
         .option("write.parquet.compression-codec", "snappy")
         .option("write.parquet.row-group-size-bytes", str(8 * 1024 * 1024))
         .option("write.parquet.page-size-bytes", str(128 * 1024))
         .option("distribution-mode", "none")
         .append())

        # logger.info(f'Created {CATALOG}.{DB}.lab_results successfully')
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
    lab_results_bronze_to_silver()

