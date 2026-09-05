#!/bin/bash
#
# Licensed to the Apache Software Foundation under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

start-master.sh -p 7077
start-worker.sh spark://spark-iceberg:7077

# Spark Connect server (Spark 3.4+). The 3.5.6 binary distribution does not
# bundle the server jars, so pull them (and gRPC/protobuf transitives) from
# Maven Central via --packages.
start-connect-server.sh --master spark://spark-iceberg:7077 \
  --total-executor-cores 1 \
  --packages org.apache.spark:spark-connect_2.12:${SPARK_VERSION}

start-history-server.sh

# Clean stale Derby metastore to avoid "Directory already exists" errors
rm -rf /tmp/derby
mkdir -p /tmp/derby

start-thriftserver.sh \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.sql.hive.server2.thrift.sasl.enabled=false \
  --conf spark.sql.hive.server2.thrift.busy.wait.duration=0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.default=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.default.type=rest \
  --conf spark.sql.catalog.default.uri=http://rest:8181 \
  --conf spark.sql.catalog.default.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.default.warehouse=s3://warehouse/ \
  --conf spark.sql.catalog.default.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.default.default-namespace=silver \
  --conf spark.sql.defaultCatalog=default \
  --driver-java-options "-Dderby.system.home=/tmp/derby"

# Entrypoint, for example notebook, pyspark or spark-sql
if [[ $# -gt 0 ]] ; then
    eval "$1"
fi
