# Integrated Flood and Heatwave Prediction System

## Architecture Overview

This is an integrated big data system for real-time flood and heatwave prediction combining:

- **Frontend**: REST API for weather data ingestion, Gradio dashboard
- **Streaming**: Apache Kafka with batch processing (batch size: 100)
- **Storage**: HDFS for data lake, Apache Cassandra for real-time queries
- **Processing**: Apache Spark for distributed data processing
- **Orchestration**: Apache Airflow for pipeline scheduling
- **ML Models**: XGBoost and LSTM for predictions

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     INTEGRATED PREDICTION SYSTEM                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────────────────────┐   │
│  │ Weather API │────▶│    Kafka    │────▶│  Spark Streaming Consumer   │   │
│  │  Producer   │     │   Broker    │     │  (Batches of 100 messages)  │   │
│  └─────────────┘     └─────────────┘     └──────────────┬──────────────┘   │
│                                                          │                   │
│                                                          ▼                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                           HDFS                                       │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐    │   │
│  │  │ streaming/ │  │ processed/ │  │  features/ │  │predictions/│    │   │
│  │  │  (raw)     │  │  (cleaned) │  │(engineered)│  │  (output)  │    │   │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                          │                                   │
│                                          ▼                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      Apache Airflow DAGs                             │   │
│  │                                                                       │   │
│  │  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────┐  │   │
│  │  │  Data Pipeline    │  │ Training Pipeline │  │ Inference DAG   │  │   │
│  │  │  (10 min)         │  │ (1 minute)        │  │ (2 sec loop)    │  │   │
│  │  │                   │  │                   │  │                 │  │   │
│  │  │ • Kafka→HDFS     │  │ • XGBoost Train   │  │ • Load latest   │  │   │
│  │  │ • Feature Eng    │  │ • LSTM Train      │  │   checkpoint    │  │   │
│  │  │ • Labeling       │  │ • Save checkpoint │  │ • Run inference │  │   │
│  │  └───────────────────┘  └───────────────────┘  └─────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                          │                                   │
│                                          ▼                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     Gradio Dashboard                                 │   │
│  │  • Real-time predictions from HDFS                                   │   │
│  │  • Cassandra query results                                           │   │
│  │  • Risk visualization                                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Features

### 1. Kafka Batching (100 messages)
- Weather data is collected via REST API
- Kafka batches messages in groups of 100
- Spark Streaming writes batches to HDFS

### 2. Inference via Airflow (2-second intervals)
- Airflow DAG runs every minute
- Internal loop executes inference every 2 seconds
- Uses latest model checkpoint from HDFS

### 3. Training (1-minute intervals)
- Separate training DAG runs every minute
- Updates XGBoost and LSTM models
- Saves checkpoints to HDFS

## Quick Start

### Option 1: Unified Docker Compose

```bash
# Start all services
cd final-big-data
docker-compose -f docker-compose.unified.yml up -d

# Start with frontend services
docker-compose -f docker-compose.unified.yml --profile frontend up -d

# Start with Spark consumer
docker-compose -f docker-compose.unified.yml --profile consumer up -d
```

### Option 2: Separate Components

```bash
# Start HDFS
cd Flood-And-HeatWave-Predictor/BigDataCluster/infra/hdfs
docker-compose -f hdfs-compose.yml up -d

# Start Spark
cd ../spark
docker-compose -f spark-compose.yml up -d

# Start Kafka
cd ../kafka
docker-compose -f kafka-compose.yml up -d

# Start Airflow
cd ../airflow
docker-compose -f airflow-compose.yml up -d

# Start Frontend Kafka services
cd ../../../../frontend_big_data
docker-compose up -d
```

## Service URLs

| Service | URL | Description |
|---------|-----|-------------|
| Kafka Producer API | http://localhost:8000 | Submit weather data |
| Kafka UI | http://localhost:8080 | Monitor Kafka topics |
| HDFS NameNode | http://localhost:9870 | HDFS web interface |
| Spark Master | http://localhost:8081 | Spark cluster UI |
| Airflow | http://localhost:8082 | Airflow DAG management |
| Gradio Dashboard | http://localhost:7860 | Real-time predictions |

## API Usage

### Submit Weather Data

```bash
# Single observation
curl -X POST http://localhost:8000/weather \
  -H "Content-Type: application/json" \
  -d '{
    "district": "Kathmandu",
    "date": "2025-12-11",
    "max_temp": 28.5,
    "precipitation": 5.2,
    "humidity": 75.0
  }'

# Batch observations
curl -X POST http://localhost:8000/weather/batch \
  -H "Content-Type: application/json" \
  -d '{"observations": [...]}'

# Fetch live weather and send to Kafka
curl http://localhost:8000/fetch/Kathmandu
```

## Airflow DAGs

### 1. `flood_heatwave_data_pipeline`
- **Schedule**: Every 10 minutes
- **Tasks**:
  1. `kafka_to_hdfs` - Process Kafka batch to HDFS (Spark)
  2. `merge_data` - Merge streaming with historical data (Spark)
  3. `ingest_data` - Data ingestion (Spark)
  4. `feature_engineering` - Feature engineering (Spark)
  5. `labeling` - Create labels (Spark)

### 2. `flood_heatwave_training`
- **Schedule**: Every 1 minute
- **Tasks**:
  - `train_xgb_heatwave` - Train XGBoost for heatwave
  - `train_xgb_flood` - Train XGBoost for flood
  - `train_lstm_heatwave` - Train LSTM for heatwave
  - `train_lstm_flood` - Train LSTM for flood

### 3. `flood_heatwave_inference`
- **Schedule**: Every 1 minute (with 2-second internal loop)
- **Tasks**:
  - `inference_loop` - Runs inference every 2 seconds for 58 seconds

## Environment Variables

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=weather-data
KAFKA_BATCH_SIZE=100

# HDFS
HDFS_NAMENODE=hdfs://namenode:9000
HDFS_BASE_PATH=hdfs://namenode:9000/user/airflow/weather_data
MODEL_CHECKPOINT_PATH=hdfs://namenode:9000/user/airflow/models

# Spark
SPARK_MASTER=spark://spark-spark-1:7077

# Intervals
INFERENCE_INTERVAL_SECONDS=2
TRAINING_INTERVAL=*/1 * * * *
```

## Directory Structure

```
final-big-data/
├── docker-compose.unified.yml      # Unified deployment
├── Flood-And-HeatWave-Predictor/
│   └── BigDataCluster/
│       ├── dags/
│       │   ├── flood_heatwave_pipeline.py    # All DAGs
│       │   └── tasks/
│       │       ├── data_ingest_task.py       # Spark task
│       │       ├── feature_engineering_task.py # Spark task
│       │       ├── labeling_task.py          # Spark task
│       │       ├── train_lstm_task.py        # PyTorch training
│       │       ├── spark_kafka_hdfs_task.py  # Kafka→HDFS
│       │       └── inference_task.py         # Inference task
│       └── infra/
│           ├── hdfs/
│           ├── spark/
│           ├── kafka/
│           └── airflow/
└── frontend_big_data/
    ├── kafka_producer_api.py          # REST API
    ├── spark_kafka_consumer.py        # Spark consumer
    ├── hdfs_inference_client.py       # HDFS predictions client
    ├── kafka_consumer_gradio.py       # Gradio dashboard
    └── docker-compose.yml
```

## Model Checkpoints

Models are saved to HDFS with timestamps:
```
hdfs://namenode:9000/user/airflow/models/
├── xgb_heatwave/
│   └── model_20251211_120000.joblib
├── xgb_flood_proxy/
│   └── model_20251211_120000.joblib
├── lstm_heatwave/
│   └── checkpoint_20251211_120000.pt
└── lstm_flood_proxy/
    └── checkpoint_20251211_120000.pt
```

Inference automatically uses the latest checkpoint.

## Monitoring

### Check Kafka Messages
```bash
# View messages in topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather-data \
  --from-beginning
```

### Check HDFS Data
```bash
# List HDFS directories
docker exec -it namenode hdfs dfs -ls /user/airflow/weather_data/

# Check streaming data
docker exec -it namenode hdfs dfs -ls /user/airflow/weather_data/streaming/
```

### Check Airflow DAGs
```bash
# List DAGs
docker exec -it airflow-scheduler airflow dags list

# Trigger DAG manually
docker exec -it airflow-scheduler airflow dags trigger flood_heatwave_inference
```

## Troubleshooting

### Kafka Connection Issues
```bash
# Check Kafka is running
docker-compose logs kafka

# Test producer
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic weather-data
```

### HDFS Connection Issues
```bash
# Check HDFS status
docker exec -it namenode hdfs dfsadmin -report

# Safe mode off
docker exec -it namenode hdfs dfsadmin -safemode leave
```

### Spark Connection Issues
```bash
# Check Spark master
docker logs spark-master

# Submit test job
docker exec -it spark-master /opt/spark/bin/spark-submit --version
```

## License

MIT License
