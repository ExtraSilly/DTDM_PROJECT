
```bash
pip install -r requirements.txt
docker compose up --build -d
docker compose up -d
docker-compose up -d kafka
docker exec kafka kafka-topics --create --topic nametopic  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 
$env:GOOGLE_APPLICATION_CREDENTIALS="D:\PROJECT_CK_DTDM_BIGQUERY\load_dataset_bigquery\iconic-valve-446108-v7-fc1ac39df54a.json"
echo $env:GOOGLE_APPLICATION_CREDENTIALS
```
### $env:GOOGLE_APPLICATION_CREDENTIALS="D:\PROJECT_CK_DTDM_BIGQUERY\load_dataset_bigquery\iconic-valve-446108-v7-fc1ac39df54a.json"
### echo $env:GOOGLE_APPLICATION_CREDENTIALS