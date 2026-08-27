# Start the lakehouse stack
docker compose up -d

# Run the full pipeline (silver + gold)
docker compose exec spark-iceberg spark-submit \
  --deploy-mode client \
  /home/iceberg/pipeline/index.py
