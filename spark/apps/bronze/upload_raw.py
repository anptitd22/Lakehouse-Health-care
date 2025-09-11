import os
import tempfile
from datetime import date
import boto3
import pandas as pd
from dotenv import load_dotenv

path_env = '../../../.env'
load_dotenv(path_env)

ACCESS_KEY = os.getenv('ACCESS_KEY')
ACCESS_SECRET = os.getenv('ACCESS_SECRET')

# Các cột nghi ngờ date/time để ép về string nếu xuất hiện
DATE_TIME_COLS = {
    "start_time",
    "end_time",
    "appointment_date",
    "fee_date",
    "dob",
    "treatment_date",
    "measurement_date",
}

def upload_folder(
        local_folder="../../../dataset/local_samples/health_care_raw",
        bucket="lakehouse",
        prefix="bronze/health_care_raw",
        endpoint="http://minio:9000",
        access_key=ACCESS_KEY,
        secret_key=ACCESS_SECRET,
        ingest_dt=date.today().isoformat(),
        rewrite_parquet_time_to_string=True
):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)

    def upload_file(local_path: str, s3_key: str):
        s3.upload_file(local_path, bucket, s3_key)
        print(f"upload {local_path} -> s3://{bucket}/{s3_key}")

    for root, _, files in os.walk(local_folder):
        for fname in files:
            lp = os.path.join(root, fname)
            rel = os.path.relpath(lp, local_folder).replace("\\", "/")
            name, ext = os.path.splitext(rel)
            ext_lower = ext.lower()

            if ext_lower == ".csv":
                # Đường dẫn đích: đổi .csv -> .parquet và partition theo ingest_dt
                out_key = f"{prefix}/ingest_dt={ingest_dt}/{name}.parquet"

                try:
                    # Đọc CSV (để mặc định; không ép toàn bộ thành string)
                    df = pd.read_csv(lp)

                    # Ép các cột date/time-like (nếu tồn tại) sang string dtype
                    for col in DATE_TIME_COLS.intersection(df.columns):
                        df[col] = df[col].astype("string")

                    # Ghi parquet tạm
                    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
                    tmp.close()
                    df.to_parquet(tmp.name, engine="pyarrow", index=False, compression="snappy")

                    # Upload parquet
                    upload_file(tmp.name, out_key)

                except Exception as e:
                    print(f"[ERROR] CSV -> Parquet fail: {lp} -> {e}")
                finally:
                    try:
                        if 'tmp' in locals() and os.path.exists(tmp.name):
                            os.remove(tmp.name)
                    except Exception:
                        pass

            else:
                # File không phải CSV: upload thẳng (hoặc bỏ qua nếu bạn chỉ muốn xử lý CSV)
                out_key = f"{prefix}/ingest_dt={ingest_dt}/{rel}"
                try:
                    upload_file(lp, out_key)
                except Exception as e:
                    print(f"[ERROR] Upload fail: {lp} -> {e}")

if __name__ == "__main__":
    upload_folder()
