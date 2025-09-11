from datetime import datetime, timedelta, timezone
from spark.conf.spark_conf import get_spark

# Danh sách bảng bảo trì (bạn bổ sung/giảm tuỳ ý)
ICEBERG_TABLES = [
    "local.silver.appointment",
    "local.silver.diseases",
    "local.silver.doctors",
    "local.silver.hospital_fee",
    "local.silver.lab_results",
    "local.silver.patient",
    "local.silver.treatments",
    "local.silver.vital_signs",
    # "local.gold.dim_patient",
    # "local.gold.dim_doctor",
    # "local.gold.fact_lab_results",
]

RETENTION_DAYS = 7  # Giữ snapshot trong 7 ngày

def iceberg_expire_and_cleanup(retention_days: int = RETENTION_DAYS):
    spark = get_spark()
    for t in ICEBERG_TABLES:
        # Expire snapshots cũ hơn N ngày
        spark.sql(f"""
            CALL local.system.expire_snapshots(
              table => '{t}',
              older_than => current_timestamp() - INTERVAL {retention_days} DAYS
            )
        """)
        # Dọn file mồ côi nếu bản Iceberg hỗ trợ
        try:
            spark.sql(f"CALL local.system.remove_orphan_files(table => '{t}')")
        except Exception:
            # Không phải bản nào cũng có thủ tục này; bỏ qua nếu thiếu
            pass
        # Gộp manifest để metadata gọn
        spark.sql(f"CALL local.system.rewrite_manifests(table => '{t}')")

    spark.stop()