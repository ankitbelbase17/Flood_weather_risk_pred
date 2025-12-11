# Flood & Heatwave Prediction System

A complete, standalone Big Data machine learning system for predicting flood and heatwave events in Nepal's Terai region using LSTM neural networks, Apache Spark, HDFS, Kafka, and Airflow orchestration.

## 🎯 Project Overview

This system uses distributed Big Data infrastructure to train LSTM models on weather data, providing accurate predictions for extreme weather events. The entire pipeline runs in Docker containers with no external dependencies.

## 🚀 Quick Start (3 Commands)

### Step 1: Start Big Data Infrastructure
```powershell
cd BigDataCluster
make start-all
```

Wait 30 seconds, then verify:
- **Airflow**: http://localhost:8090 (admin/admin)
- **HDFS**: http://localhost:9870
- **Spark**: http://localhost:8081

### Step 2: Upload Data & Deploy Pipeline
```powershell
# Upload dataset to HDFS
docker cp ../data/terai_districts_weather.csv namenode:/tmp/
docker exec namenode hdfs dfs -mkdir -p /user/airflow/weather_data/raw
docker exec namenode hdfs dfs -put /tmp/terai_districts_weather.csv /user/airflow/weather_data/raw/

# Create output directories
docker exec namenode hdfs dfs -mkdir -p /user/airflow/weather_data/features /user/airflow/weather_data/labeled /user/airflow/models

# Deploy DAGs to Airflow
docker cp dags/flood_heatwave_pipeline.py airflow-airflow-standalone-1:/opt/airflow/dags/
docker cp dags/tasks airflow-airflow-standalone-1:/opt/airflow/dags/
```

### Step 3: Run the Pipeline
1. Open Airflow UI: http://localhost:8090
2. Login: `admin` / `admin`
3. Find `flood_heatwave_pipeline` DAG
4. Click "Trigger DAG" ▶️
5. Monitor execution (~10-20 minutes)

## 📊 View Results

### Quick Check
```powershell
cd ..
python quick_inference.py
```

### Full Analysis (Recommended)
```powershell
jupyter notebook inference_and_results.ipynb
```

**You'll get:**
- ✅ Classification metrics (Accuracy, Precision, Recall, F1, ROC-AUC)
- ✅ Confusion matrices
- ✅ ROC curves  
- ✅ Sample predictions with probabilities
- ✅ Error analysis

## 🏗️ Architecture

```
CSV Dataset 
  ↓ Spark Ingestion
HDFS Parquet Storage
  ↓ Spark Feature Engineering
Features Dataset
  ↓ Spark Labeling
Labeled Dataset
  ↓ PyTorch LSTM Training
Trained Models → HDFS
```

### Services
- **HDFS**: Distributed storage (Namenode: 9000, 9870)
- **Spark**: Distributed processing (Master: 7077, UI: 8081)
- **Kafka**: Stream processing (Broker: 9092)
- **Airflow**: Workflow orchestration (UI: 8090)

## 📁 Project Structure

```
Flood-And-HeatWave-Predictor/
├── data/
│   └── terai_districts_weather.csv        # Input dataset
├── BigDataCluster/                         # Infrastructure
│   ├── Makefile                            # Service commands
│   ├── requirements.txt                    # Dependencies
│   ├── README.md                           # Infrastructure guide
│   ├── infra/                              # Docker configs
│   │   ├── hdfs/                           # HDFS cluster
│   │   ├── spark/                          # Spark cluster
│   │   ├── kafka/                          # Kafka streaming
│   │   └── airflow/                        # Airflow setup
│   └── dags/
│       ├── flood_heatwave_pipeline.py      # Main DAG
│       └── tasks/                          # Pipeline tasks
│           ├── data_ingest_task.py
│           ├── feature_engineering_task.py
│           ├── labeling_task.py
│           └── train_lstm_task.py
├── inference_and_results.ipynb            # Results notebook
├── quick_inference.py                      # Quick status check
├── INFERENCE_GUIDE.md                      # How to view results
└── README.md                               # This file
```

## 🔧 Pipeline Tasks

1. **ingest_data**: Loads CSV, converts to Parquet, partitions by district
2. **feature_engineering**: Creates rolling averages, lag features using Spark
3. **labeling**: Generates heatwave (95th percentile) & flood (99th percentile) labels
4. **train_heatwave_model**: Trains LSTM for heatwave prediction
5. **train_flood_model**: Trains LSTM for flood prediction (parallel)

## 💻 System Requirements

- **OS**: Windows 10+, Linux, or macOS
- **RAM**: 8GB minimum, 16GB recommended
- **CPU**: 4 cores minimum
- **Disk**: 10GB free space
- **Docker**: 20.10+

## 🛠️ Common Commands

### Check Services
```powershell
docker ps
```

### View Logs
```powershell
docker logs airflow-airflow-standalone-1 --tail 100
```

### Check HDFS Data
```powershell
docker exec namenode hdfs dfs -ls -R /user/airflow/
```

### Stop All Services
```powershell
cd BigDataCluster
make stop-all
```

## 🐛 Troubleshooting

### Services won't start
```powershell
docker network create -d bridge custom_network
cd BigDataCluster
make restart-all
```

### DAG not appearing
```powershell
docker exec airflow-airflow-standalone-1 airflow db upgrade
```

### HDFS connection failed
```powershell
docker exec namenode hdfs dfs -ls /
docker logs namenode --tail 50
```

## 🎓 Documentation

- **Infrastructure Guide**: `BigDataCluster/README.md`
- **Inference Guide**: `INFERENCE_GUIDE.md`
- **Quick Reference**: This file

## ✨ Features

- ✅ **Fully Dockerized**: One-command deployment
- ✅ **Distributed Processing**: Spark for big data
- ✅ **Deep Learning**: LSTM time series models
- ✅ **Automated Workflow**: Airflow orchestration
- ✅ **Complete Independence**: No external dependencies
- ✅ **Production Ready**: Scalable architecture

## 📈 Model Performance

After training, expect:
- **Accuracy**: > 85%
- **ROC-AUC**: > 0.85
- **Precision/Recall**: Balanced for both classes

## 🔄 Customization

Edit hyperparameters in `BigDataCluster/dags/tasks/train_lstm_task.py`:
- `hidden_size`: 64 (LSTM hidden units)
- `fc_size`: 32 (fully connected layer)
- `num_layers`: 2 (LSTM layers)
- `dropout`: 0.2
- `epochs`: 50
- `batch_size`: 32

## 📞 Support

Check logs and UI for debugging:
1. Airflow UI: http://localhost:8090
2. HDFS UI: http://localhost:9870  
3. Spark UI: http://localhost:8081
4. Container logs: `docker logs <container_name>`

---

**Built with**: Apache Spark • HDFS • Kafka • Airflow • PyTorch • Docker