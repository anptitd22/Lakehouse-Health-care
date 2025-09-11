import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, current_timestamp, lit
from logs_build.build_logging import setup_logging
from spark.conf.spark_conf import get_spark

VITAL_SIGNS_PATH = f"s3a://lakehouse/bronze/health_care_raw/ingest_dt={date.today().isoformat()}/vital_signs*.parquet"
CATALOG = "local"
DB = "silver"
TABLE = "vital_signs"
# setup_logging()
# logger = logging.getLogger(__name__)

def vital_signs_bronze_to_silver():

    try:
        spark = get_spark()
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")

        vital_signs_raw = (
            spark.read.parquet(VITAL_SIGNS_PATH)
        )

        df = vital_signs_raw.withColumn(
            "vital_signs"
            , struct(
                col('vital_id').cast('int').alias("vital_id")
                , col('patient_id').cast('int').alias("patient_id")
                , col('measurement_date').cast('string').alias("measurement_date")
                , col('blood_pressure').cast('string').alias("blood_pressure")
                , col('heart_rate').cast('int').alias("heart_rate")
                , col('respiratory_rate').cast('int').alias("respiratory_rate")
                , col('temperature').cast('float').alias("temperature")
                , col('oxygen_saturation').cast('int').alias("oxygen_saturation")
                , col('blood_sugar').cast('int').alias("blood_sugar")
                , lit(current_timestamp()).alias('ingested_at')
            )
        )

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.{TABLE} (
            vital_id INTEGER
            , patient_id INTEGER
            , measurement_date STRING
            , blood_pressure STRING
            , heart_rate INT
            , respiratory_rate INT
            , temperature FLOAT
            , oxygen_saturation INT
            , blood_sugar INT
            , ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (day(ingested_at))
        TBLPROPERTIES ('format-version'='2')
        """)

        (df.select(col('vital_signs.*')).coalesce(6)
            .writeTo(f"""{CATALOG}.{DB}.{TABLE}""")
         .option("fanout-enabled", "false")
         .option("write.parquet.dictionary-enabled", "false")
         .option("write.parquet.compression-codec", "snappy")
         .option("write.parquet.row-group-size-bytes", str(8 * 1024 * 1024))
         .option("write.parquet.page-size-bytes", str(128 * 1024))
         .option("distribution-mode", "none")
         .append())

        # logger.info(f"Created {CATALOG}.{DB}.vital_signs successfully.")
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
    vital_signs_bronze_to_silver()