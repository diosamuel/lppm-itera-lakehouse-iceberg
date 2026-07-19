#sudo docker compose up -d

# Initialize gold schema
sudo docker compose exec spark-iceberg python /home/iceberg/pipeline/schema/goldSchema.py
# Initialize silver schema
sudo docker exec -it lppm-spark-iceberg python3 /home/iceberg/pipeline/index.py
