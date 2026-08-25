# Start the lakehouse stack
docker compose up -d

# Initialize gold schema
docker compose exec spark-iceberg spark-submit \
  --deploy-mode client \
  /home/iceberg/pipeline/schema/goldSchema.py

# Run the full pipeline (silver + gold)
docker compose exec spark-iceberg spark-submit \
  --deploy-mode client \
  /home/iceberg/pipeline/index.py
