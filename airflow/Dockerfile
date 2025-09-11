FROM apache/airflow:3.0.0

USER root

RUN apt-get update && apt-get install -y curl && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl gcc python3-dev libffi-dev libssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# location java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN mkdir -p /opt/spark/jars && rm -f /opt/spark/jars/*

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.787.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.787/aws-java-sdk-bundle-1.12.787.jar && \
    curl -L -o /opt/spark/jars/ojdbc11.jar https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/21.9.0.0/ojdbc11-21.9.0.0.jar

RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar && \
    curl -L -o /opt/spark/jars/iceberg-aws-bundle-1.9.2.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.2/iceberg-aws-bundle-1.9.2.jar  && \
    curl -L -o /opt/spark/jars/iceberg-nessie-1.9.2.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-nessie/1.9.2/iceberg-nessie-1.9.2.jar

RUN chown -R airflow:root /opt/spark/jars

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"

# install excel
RUN pip install --no-cache-dir openpyxl

# install pyspark
RUN pip install --default-timeout=1200 --no-cache-dir pyspark==3.5.6

# install my_minio & boto3
RUN pip install --no-cache-dir minio boto3

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.9.0

RUN pip install --no-cache-dir "dbt-core==1.9.3"

RUN pip install --no-cache-dir "dbt-spark[session]==1.9.3"

# file dataset
COPY ../dataset /opt/airflow/dataset

COPY ../spark /opt/airflow/spark

COPY ../logs_build /opt/airflow/logs_build

COPY ../iceberg /opt/airflow/iceberg

#COPY ../install /opt/spark/jars

ENV PYTHONPATH="/opt/airflow/spark/apps:/opt/airflow/dags:/opt/airflow/plugins:/opt/airflow/logs_build:/opt/airflow/iceberg:${PYTHONPATH}"
