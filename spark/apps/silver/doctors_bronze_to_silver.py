import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, current_timestamp, lit
from logs_build.build_logging import setup_logging
from spark.conf.spark_conf import get_spark

DOCTORS_PATH = f"s3a://lakehouse/bronze/health_care_raw/ingest_dt={date.today().isoformat()}/doctors*.parquet"
CATALOG = "local"
DB = "silver"
TABLE = "doctors"

# setup_logging()
# logger = logging.getLogger(__name__)

def doctors_bronze_to_silver():

    try:
        spark = get_spark()
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")

        doctors_raw = (
            spark.read.parquet(DOCTORS_PATH)
        )

        df = doctors_raw.withColumn(
            'doctors'
            , struct(
                col('doctor_id').cast('int').alias('doctor_id')
                , col('Name').cast('string').alias('doctor_name')
                , col('specialization').cast('string').alias('doctor_specialization')
                , col('phone').cast('string').alias('doctor_phone')
                , col('email').cast('string').alias('doctor_email')
                , lit(current_timestamp()).alias("ingested_at")
            )
        )

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.{TABLE} (
          doctor_id INTEGER
          , doctor_name STRING
          , doctor_specialization STRING
          , doctor_phone STRING
          , doctor_email STRING
          , ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (day(ingested_at))
        TBLPROPERTIES ('format-version'='2')
        """)

        (df.select(col("doctors.*")).coalesce(6)
            .writeTo(f"{CATALOG}.{DB}.{TABLE}")
         .option("fanout-enabled", "false")
         .option("write.parquet.dictionary-enabled", "false")
         .option("write.parquet.compression-codec", "snappy")
         .option("write.parquet.row-group-size-bytes", str(8 * 1024 * 1024))
         .option("write.parquet.page-size-bytes", str(128 * 1024))
         .option("distribution-mode", "none")
         .append())

        #logger.info(f'Created {CATALOG}.{DB}.doctors successfully')
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
    doctors_bronze_to_silver()

