# Start the lakehouse stack
docker compose up -d

# Spark defaults (master URL, app name, Iceberg extensions, REST catalog,
# S3A/MinIO, defaultCatalog, spark-excel package, executorEnv.PYTHONPATH for
# UDFs) are loaded from spark/spark-defaults.conf and pipeline/setup/setup_spark.py
# at runtime — spark-submit inherits them, so no --master / --name / --conf here.

# Initialize gold schema
docker compose exec spark-iceberg spark-submit \
  --deploy-mode client \
  /home/iceberg/pipeline/schema/goldSchema.py

# Run the full pipeline (silver + gold)
docker compose exec spark-iceberg spark-submit \
  --deploy-mode client \
  /home/iceberg/pipeline/index.py
