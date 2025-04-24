
```bash
pip install -r requirements.txt
docker compose up --build -d
docker compose up -d
docker-compose up -d kafka
docker exec kafka kafka-topics --create --topic nametopic  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 
```
