# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# syntax=docker/dockerfile:1
FROM docker.io/library/python:3.10-bullseye

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      openjdk-17-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Jupyter and other python deps
COPY spark/requirements.txt .
RUN pip3 install -r requirements.txt

# Add scala kernel via spylon-kernel
RUN python3 -m spylon_kernel install

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

WORKDIR ${SPARK_HOME}

ENV SPARK_VERSION=3.5.6
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.10.1

# Download spark (with local fallback)
COPY bin/spark-3.5.6-bin-hadoop3.tgz /tmp/spark-local.tgz

RUN mkdir -p ${SPARK_HOME} \
 && (curl -L --retry 5 --retry-delay 10 --connect-timeout 30 "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz \
     && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
     && rm /tmp/spark.tgz) \
 || (echo "Download failed, using local fallback..." \
     && cp /tmp/spark-local.tgz /tmp/spark.tgz \
     && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
     && rm /tmp/spark.tgz)

# RUN mkdir -p /opt/spark \
#  && curl -L --retry 5 --retry-delay 10 https://archive.apache.org/dist/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz -o spark.tgz \
#  && tar -xzf spark.tgz -C /opt/spark --strip-components=1 \
#  && rm spark.tgz
 
# Download iceberg spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# Download AWS bundle
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Download Hadoop AWS and AWS SDK for S3AFileSystem
RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -Lo /opt/spark/jars/hadoop-aws-3.3.4.jar
RUN curl -s https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -Lo /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Download GCP bundle
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar

# Download Azure bundle
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-azure-bundle/${ICEBERG_VERSION}/iceberg-azure-bundle-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-azure-bundle-${ICEBERG_VERSION}.jar

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
 && unzip awscliv2.zip \
 && sudo ./aws/install \
 && rm awscliv2.zip \
 && rm -rf aws/

RUN mkdir -p /home/iceberg/data \
 && curl https://data.cityofnewyork.us/resource/tg4x-b46p.json > /home/iceberg/data/nyc_film_permits.json \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-04.parquet -o /home/iceberg/data/yellow_tripdata_2022-04.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet -o /home/iceberg/data/yellow_tripdata_2022-03.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet -o /home/iceberg/data/yellow_tripdata_2022-02.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet -o /home/iceberg/data/yellow_tripdata_2022-01.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-12.parquet -o /home/iceberg/data/yellow_tripdata_2021-12.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-11.parquet -o /home/iceberg/data/yellow_tripdata_2021-11.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-10.parquet -o /home/iceberg/data/yellow_tripdata_2021-10.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-09.parquet -o /home/iceberg/data/yellow_tripdata_2021-09.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-08.parquet -o /home/iceberg/data/yellow_tripdata_2021-08.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-07.parquet -o /home/iceberg/data/yellow_tripdata_2021-07.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-06.parquet -o /home/iceberg/data/yellow_tripdata_2021-06.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-05.parquet -o /home/iceberg/data/yellow_tripdata_2021-05.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-04.parquet -o /home/iceberg/data/yellow_tripdata_2021-04.parquet

RUN mkdir -p /home/iceberg/notebooks /home/iceberg/spark-events /home/iceberg
COPY spark/notebooks/ /home/iceberg/notebooks

# Bind-mount local notebooks for live edit (Jupyter uses --notebook-dir=/home/iceberg/notebooks below).
# Run from this directory (spark/), after: docker build -t lppm-datalakehouse .
#
# Linux / macOS / Git Bash:
#   docker run --rm -it -p 8888:8888 \
#     -v "$(pwd)/notebooks:/home/iceberg/notebooks" \
#     lppm-datalakehouse
#
# Windows PowerShell (from spark/):
#   docker run --rm -it -p 8888:8888 `
#     -v "${PWD}/notebooks:/home/iceberg/notebooks" `
#     lppm-datalakehouse
#
# Optional: persist Iceberg warehouse on the host (adjust host paths as needed):
#   -v "$(pwd)/warehouse:/home/iceberg/warehouse"
#   -v "$(pwd)/localwarehouse:/home/iceberg/localwarehouse"
#
# Avoid -v "$(pwd):/home" — that does not map to the notebook directory Jupyter opens.

# Add a notebook command
RUN echo '#! /bin/sh' >> /bin/notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook \
 && echo "pyspark" >> /bin/notebook \
 && chmod u+x /bin/notebook

# Add a pyspark-notebook command (alias for notebook command for backwards-compatibility)
RUN echo '#! /bin/sh' >> /bin/pyspark-notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/pyspark-notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/pyspark-notebook \
 && echo "pyspark" >> /bin/pyspark-notebook \
 && chmod u+x /bin/pyspark-notebook

RUN mkdir -p /root/.ipython/profile_default/startup
COPY spark/ipython/startup/00-prettytables.py /root/.ipython/profile_default/startup
COPY spark/ipython/startup/README /root/.ipython/profile_default/startup

COPY spark/spark-defaults.conf /opt/spark/conf
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

COPY spark/.pyiceberg.yaml /root/.pyiceberg.yaml

COPY spark/entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
CMD ["notebook"]
