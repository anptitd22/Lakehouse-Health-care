FROM bitnami/spark:3.5.6

USER root

# install java
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

USER 1001

RUN pip install --no-cache-dir boto3 minio