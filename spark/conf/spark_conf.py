from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os

def get_spark():
    # JAR mới cần thêm (KHÔNG xoá JAR cũ)
    iceberg_runtime = "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar"
    iceberg_aws_bundle = "/opt/spark/jars/iceberg-aws-bundle-1.9.2.jar"
    iceberg_nessie = "/opt/spark/jars/iceberg-nessie-1.9.2.jar"
    all_opt = '/opt/spark/jars/*'

    existing_jars = "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.787.jar,/opt/spark/jars/ojdbc11.jar"
    all_jars = f"{existing_jars},{iceberg_runtime},{iceberg_aws_bundle},{iceberg_nessie}"

    spark = ( SparkSession.builder
        .appName("TEST")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.memoryOverhead", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))  # 64MB
        .config("spark.executor.memoryOverhead", "1g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.shuffle.partitions", "2")  # chỉnh theo cluster
        .config("spark.hadoop.fs.s3a.threads.max", "16")
        .config("spark.hadoop.fs.s3a.connection.maximum", "32")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "nessie")
        .config("spark.sql.catalog.local.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.local.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin12345")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.jars", all_jars)
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.part.upload.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.idle.time", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
        .config("spark.driver.extraClassPath", all_opt)
        .config("spark.executor.extraClassPath", all_opt)
        .config("spark.hadoop.fs.s3a.vectored.read.min.seek.size", "131072")
        .config("spark.hadoop.fs.s3a.vectored.read.max.merged.size", "2097152")
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.retry.limit", "5")
        .config("spark.hadoop.fs.s3a.retry.interval", "500")
        .config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536")
        .config("spark.hadoop.fs.s3a.socket.send.buffer", "65536")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.sql.catalog.local.write.fanout.enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

    try:
        jvm = spark._jvm
        for cls in [
            "org.apache.iceberg.spark.SparkCatalog",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "org.apache.iceberg.nessie.NessieCatalog",
        ]:
            try:
                jvm.java.lang.Class.forName(cls);
                print("[OK] ", cls)
            except Exception as e:
                print("[MISS]", cls, "->", e)
    except Exception as e:
        print("Không tìm thấy Iceberg SparkCatalog:", e)
        raise

    return spark

def install_spark_dependencies():
    spark = get_spark()
    print("Spark session started.")
    confs = spark.sparkContext.getConf().getAll()
    for k, v in confs:
        print(f"{k} = {v}")
    print("Spark session finished.")
    spark.stop()