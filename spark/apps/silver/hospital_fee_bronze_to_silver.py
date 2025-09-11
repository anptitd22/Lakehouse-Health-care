import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.connect.functions import to_date
from pyspark.sql.functions import col, struct, current_timestamp, lit
from logs_build.build_logging import setup_logging
from spark.conf.spark_conf import get_spark

HOSPITAL_FEE_PATH = f"s3a://lakehouse/bronze/health_care_raw/ingest_dt={date.today().isoformat()}/hospital_fee*.parquet"
CATALOG = "local"
DB = "silver"
TABLE = 'hospital_fee'
# setup_logging()
# logger = logging.getLogger(__name__)

def hospital_fee_bronze_to_silver():

    try:
        spark = get_spark()
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")

        hospital_fee_raw = (
            spark.read.parquet(HOSPITAL_FEE_PATH)
        )

        df = (hospital_fee_raw
        .withColumn(
            "hospital_fee"
            , struct(
                col('fee_id').cast('string').alias("fee_id")
                , col('appointment_id').cast('int').alias('appointment_id')
                , col('patient_id').cast('int').alias('patient_id')
                , col('service_type').cast('string').alias('service_type')
                , col('description').cast('string').alias('description')
                , col('amount').cast('float').alias('amount')
                , col('fee_date').cast('string').alias('fee_date')
                , col('phone').cast('string').alias('phone')
                , lit(current_timestamp()).alias('ingested_at')
            )
        ))

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.{TABLE} (
            fee_id STRING
            , appointment_id INT
            , patient_id INT
            , service_type STRING
            , description STRING
            , amount FLOAT
            , fee_date STRING
            , phone STRING
            , ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (day(ingested_at))
        TBLPROPERTIES ('format-version'='2')
        """)

        (df.select(col("hospital_fee.*")).coalesce(6)
            .writeTo(f"{CATALOG}.{DB}.{TABLE}")
         .option("fanout-enabled", "false")
         .option("write.parquet.dictionary-enabled", "false")
         .option("write.parquet.compression-codec", "snappy")
         .option("write.parquet.row-group-size-bytes", str(8 * 1024 * 1024))
         .option("write.parquet.page-size-bytes", str(128 * 1024))
         .option("distribution-mode", "none")
         .append())

        # logger.info(f'Created {CATALOG}.{DB}.hospital_fee successfully')
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
    hospital_fee_bronze_to_silver()

