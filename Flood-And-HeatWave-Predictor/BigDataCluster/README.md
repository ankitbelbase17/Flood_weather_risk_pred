# Flood & Heatwave Prediction - Big Data Cluster

A machine learning pipeline for predicting flood and heatwave events using LSTM models with Apache Spark, HDFS, Kafka, and Airflow orchestration.

## Architecture

This project uses a distributed Big Data architecture:
- **HDFS**: Distributed storage for weather data and trained models
- **Apache Spark**: Distributed data processing and feature engineering
- **Apache Kafka**: Real-time data streaming (optional)
- **Apache Airflow**: Workflow orchestration and scheduling
- **PyTorch LSTM**: Deep learning models for time series prediction

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 8GB RAM available for containers
- Windows PowerShell or Linux/Mac terminal

### 1. Start All Services

```bash
cd BigDataCluster
make start-all
```

This will start:
1. **HDFS Cluster** - http://localhost:9870 (NameNode UI)
2. **Spark Cluster** - http://localhost:8081 (Master UI)
3. **Kafka Cluster** - kafka:9092
4. **Airflow Standalone** - http://localhost:8090 (admin/admin)

### 2. Upload Data to HDFS

```bash
# Copy dataset to namenode container
docker cp ../data/terai_districts_weather.csv namenode:/tmp/

# Create HDFS directory and upload
docker exec namenode hdfs dfs -mkdir -p /user/airflow/weather_data/raw
docker exec namenode hdfs dfs -put /tmp/terai_districts_weather.csv /user/airflow/weather_data/raw/

# Create output directories
docker exec namenode hdfs dfs -mkdir -p /user/airflow/weather_data/features
docker exec namenode hdfs dfs -mkdir -p /user/airflow/weather_data/labeled
docker exec namenode hdfs dfs -mkdir -p /user/airflow/models
```

### 3. Deploy DAGs to Airflow

```bash
# Copy DAG files to Airflow container
docker cp dags/flood_heatwave_pipeline.py airflow-airflow-standalone-1:/opt/airflow/dags/
docker cp dags/tasks airflow-airflow-standalone-1:/opt/airflow/dags/
```

### 4. Trigger the Pipeline

1. Open Airflow UI: http://localhost:8090
2. Login: `admin` / `admin`
3. Find `flood_heatwave_pipeline` DAG
4. Click "Trigger DAG" button
5. Monitor task execution in the Graph view

### 5. View Results

Once the pipeline completes, run the inference notebook:

```bash
cd ..
jupyter notebook inference_and_results.ipynb
```

## Pipeline Stages

The `flood_heatwave_pipeline` DAG executes these tasks:

1. **ingest_data** - Loads CSV from HDFS, converts to Parquet format
2. **feature_engineering** - Creates rolling averages and lag features using Spark
3. **labeling** - Generates heatwave (95th percentile) and flood (99th percentile) labels
4. **train_heatwave_model** - Trains LSTM model for heatwave prediction
5. **train_flood_model** - Trains LSTM model for flood prediction (parallel)

## Project Structure

```
BigDataCluster/
├── Makefile                          # Service orchestration commands
├── requirements.txt                   # Python dependencies
├── requirements-dev.txt               # Development dependencies
├── .dockerignore                      # Docker ignore patterns
├── README.md                          # This file
├── infra/                             # Infrastructure configs
│   ├── hdfs/
│   │   ├── hdfs-compose.yml          # HDFS containers
│   │   └── config                     # Hadoop configuration
│   ├── spark/
│   │   └── spark-compose.yml         # Spark cluster
│   ├── kafka/
│   │   └── kafka-compose.yml         # Kafka broker + Zookeeper
│   └── airflow/
│       ├── airflow-compose.yml       # Airflow standalone
│       ├── Dockerfile                 # Custom Airflow image
│       └── requirements.txt           # Airflow Python packages
└── dags/
    ├── flood_heatwave_pipeline.py    # Main DAG definition
    └── tasks/
        ├── data_ingest_task.py       # Data ingestion
        ├── feature_engineering_task.py  # Feature creation
        ├── labeling_task.py          # Label generation
        └── train_lstm_task.py        # Model training
```

## Service URLs

- **Airflow UI**: http://localhost:8090 (admin/admin)
- **HDFS NameNode**: http://localhost:9870
- **Spark Master**: http://localhost:8081
- **Jupyter Notebook**: http://localhost:8888 (if running)

## HDFS Data Paths

- Raw data: `/user/airflow/weather_data/raw/terai_districts_weather.csv`
- Features: `/user/airflow/weather_data/features/` (Parquet)
- Labeled data: `/user/airflow/weather_data/labeled/` (Parquet)
- Models: `/user/airflow/models/heatwave_lstm.pt`, `flood_lstm.pt`

## Makefile Commands

```bash
make start-all      # Start all services (HDFS, Spark, Kafka, Airflow)
make stop-all       # Stop all services
make restart-all    # Restart all services
make logs-airflow   # View Airflow logs
make logs-spark     # View Spark logs
make clean          # Remove all containers and volumes
```

## Troubleshooting

### Services not starting
```bash
# Check Docker is running
docker ps

# Check network exists
docker network ls | grep custom_network

# If network doesn't exist
docker network create -d bridge custom_network
```

### HDFS connection issues
```bash
# Check HDFS is healthy
docker exec namenode hdfs dfs -ls /

# Check namenode logs
docker logs namenode
```

### Airflow DAG not appearing
```bash
# Refresh Airflow database
docker exec airflow-airflow-standalone-1 airflow db upgrade

# Check DAG syntax
docker exec airflow-airflow-standalone-1 python /opt/airflow/dags/flood_heatwave_pipeline.py
```

### Spark connection failed
```bash
# Test Spark connection from Airflow
docker exec airflow-airflow-standalone-1 python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('spark://spark-spark-1:7077').getOrCreate(); print(spark.version); spark.stop()"
```

## Development

### Running Tests
```bash
make test
```

### Adding New Features
1. Modify feature engineering in `dags/tasks/feature_engineering_task.py`
2. Update labeling logic in `dags/tasks/labeling_task.py`
3. Adjust LSTM architecture in `dags/tasks/train_lstm_task.py`
4. Re-run the pipeline from Airflow UI

### Adjusting Model Hyperparameters
Edit `dags/tasks/train_lstm_task.py`:
- `hidden_size`: LSTM hidden layer size (default: 64)
- `fc_size`: Fully connected layer size (default: 32)
- `num_layers`: Number of LSTM layers (default: 2)
- `dropout`: Dropout rate (default: 0.2)
- `epochs`: Training epochs (default: 50)
- `batch_size`: Batch size (default: 32)

## System Requirements

- **RAM**: Minimum 8GB, recommended 16GB
- **CPU**: Minimum 4 cores, recommended 8 cores
- **Disk**: Minimum 10GB free space
- **Docker**: Version 20.10+
- **Docker Compose**: Version 2.0+

## Performance Notes

- Default configuration uses 2GB memory per Spark executor
- Increase memory in DAG tasks if processing larger datasets
- LSTM training is CPU-based (no GPU required)
- For GPU training, modify Airflow Dockerfile to include CUDA

## License

This project is part of the Flood-And-HeatWave-Predictor system.

## Support

For issues or questions:
1. Check Airflow UI logs for task failures
2. Review HDFS NameNode UI for data availability
3. Check Spark Master UI for job status
4. View container logs: `docker logs <container_name>`
