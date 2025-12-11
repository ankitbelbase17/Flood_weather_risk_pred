"""
Airflow DAG for Flood and Heatwave Prediction Pipeline
Integrated with Kafka streaming, Spark processing, and HDFS storage

Pipeline Architecture:
1. Kafka Consumer (Spark Streaming) - Batches of 100 messages to HDFS
2. Data Ingestion - Load and process raw data
3. Feature Engineering - Create time-based, rolling features
4. Labeling - Create heatwave and flood labels
5. Training - Update models every 1 minute
6. Inference - Run predictions every 2 seconds using latest checkpoint

Training is triggered via REST APIs exposed by Colab notebooks or local training.
"""
import os
import time
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ============================================
# CONFIGURATION
# ============================================

# Colab Training API URLs
XGB_TRAINING_API_URL = os.environ.get('XGB_TRAINING_API_URL', 'https://5ad308344aef.ngrok-free.app')
LSTM_TRAINING_API_URL = os.environ.get('LSTM_TRAINING_API_URL', 'https://ludivina-preacid-submicroscopically.ngrok-free.dev')

# HDFS Configuration
HDFS_BASE_PATH = os.environ.get('HDFS_BASE_PATH', 'hdfs://namenode:9000/user/airflow/weather_data')
MODEL_CHECKPOINT_PATH = os.environ.get('MODEL_CHECKPOINT_PATH', 'hdfs://namenode:9000/user/airflow/models')

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'weather-data')
KAFKA_BATCH_SIZE = int(os.environ.get('KAFKA_BATCH_SIZE', '100'))

# Intervals
TRAINING_INTERVAL = '*/1 * * * *'  # Every 1 minute
INFERENCE_INTERVAL_SECONDS = 2      # 2 seconds

# Training parameters
TRAINING_TIMEOUT = 600  # 10 minutes timeout

# Headers for ngrok free tier
NGROK_HEADERS = {
    'ngrok-skip-browser-warning': 'true',
    'Content-Type': 'application/json'
}


# ============================================
# DEFAULT ARGS
# ============================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}


# ============================================
# DAG 1: DATA PIPELINE (runs every 10 minutes)
# ============================================

data_pipeline_dag = DAG(
    'flood_heatwave_data_pipeline',
    default_args=default_args,
    description='Data ingestion, feature engineering, and labeling pipeline',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    catchup=False,
    tags=['data', 'spark', 'hdfs'],
)

# Task 1: Process Kafka batch to HDFS
kafka_to_hdfs = BashOperator(
    task_id='kafka_to_hdfs',
    bash_command='cd /opt/airflow && python dags/tasks/spark_kafka_hdfs_task.py batch',
    dag=data_pipeline_dag,
)

# Task 2: Merge streaming with historical data
merge_data = BashOperator(
    task_id='merge_data',
    bash_command='cd /opt/airflow && python dags/tasks/spark_kafka_hdfs_task.py merge',
    dag=data_pipeline_dag,
)

# Task 3: Data Ingestion (from merged data)
ingest_data_dp = BashOperator(
    task_id='ingest_data',
    bash_command='cd /opt/airflow && python dags/tasks/data_ingest_task.py',
    dag=data_pipeline_dag,
)

# Task 4: Feature Engineering
feature_engineering_dp = BashOperator(
    task_id='feature_engineering',
    bash_command='cd /opt/airflow && python dags/tasks/feature_engineering_task.py',
    dag=data_pipeline_dag,
)

# Task 5: Labeling
labeling_dp = BashOperator(
    task_id='labeling',
    bash_command='cd /opt/airflow && python dags/tasks/labeling_task.py',
    dag=data_pipeline_dag,
)

# Data pipeline dependencies
kafka_to_hdfs >> merge_data >> ingest_data_dp >> feature_engineering_dp >> labeling_dp


# ============================================
# DAG 2: TRAINING PIPELINE (runs every 10 minutes)
# ============================================

training_dag = DAG(
    'flood_heatwave_training',
    default_args=default_args,
    description='Model training pipeline - updates checkpoints every 10 minutes',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    catchup=False,
    tags=['training', 'ml', 'lstm', 'xgboost'],
)


def call_xgb_training_api(target: str, **context):
    """Call XGBoost training API in Colab."""
    print(f"🚀 Triggering XGBoost training for: {target}")
    print(f"   API URL: {XGB_TRAINING_API_URL}")
    
    try:
        # Health check first
        health_resp = requests.get(f"{XGB_TRAINING_API_URL}/health", headers=NGROK_HEADERS, timeout=30)
        health_resp.raise_for_status()
        print(f"   ✅ API is healthy")
        
        # Trigger training with checkpoint saving
        response = requests.post(
            f"{XGB_TRAINING_API_URL}/train",
            json={
                'target': target,
                'test_days': 365,
                'save_to_hdfs': True,
                'hdfs_path': f"{MODEL_CHECKPOINT_PATH}/xgb_{target}"
            },
            headers=NGROK_HEADERS,
            timeout=TRAINING_TIMEOUT
        )
        response.raise_for_status()
        result = response.json()
        
        print(f"   ✅ Training completed!")
        print(f"   Result: {result}")
        
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"   ❌ API call failed: {e}")
        return {'status': 'failed', 'error': str(e)}


def call_lstm_training_api(target: str, **context):
    """Call LSTM training API in Colab."""
    print(f"🚀 Triggering LSTM training for: {target}")
    print(f"   API URL: {LSTM_TRAINING_API_URL}")
    
    try:
        # Health check first
        health_resp = requests.get(f"{LSTM_TRAINING_API_URL}/health", headers=NGROK_HEADERS, timeout=30)
        health_resp.raise_for_status()
        print(f"   ✅ API is healthy")
        
        # Trigger training with checkpoint saving
        response = requests.post(
            f"{LSTM_TRAINING_API_URL}/train",
            json={
                'target': target,
                'timesteps': 14,
                'epochs': 5,  # Fewer epochs for frequent updates
                'save_to_hdfs': True,
                'hdfs_path': f"{MODEL_CHECKPOINT_PATH}/lstm_{target}"
            },
            headers=NGROK_HEADERS,
            timeout=TRAINING_TIMEOUT
        )
        response.raise_for_status()
        result = response.json()
        
        print(f"   ✅ Training completed!")
        print(f"   Result: {result}")
        
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"   ❌ API call failed: {e}")
        return {'status': 'failed', 'error': str(e)}


# Training tasks
train_xgb_heatwave_t = PythonOperator(
    task_id='train_xgb_heatwave',
    python_callable=call_xgb_training_api,
    op_kwargs={'target': 'heatwave'},
    dag=training_dag,
)

train_xgb_flood_t = PythonOperator(
    task_id='train_xgb_flood',
    python_callable=call_xgb_training_api,
    op_kwargs={'target': 'flood_proxy'},
    dag=training_dag,
)

train_lstm_heatwave_t = PythonOperator(
    task_id='train_lstm_heatwave',
    python_callable=call_lstm_training_api,
    op_kwargs={'target': 'heatwave'},
    dag=training_dag,
)

train_lstm_flood_t = PythonOperator(
    task_id='train_lstm_flood',
    python_callable=call_lstm_training_api,
    op_kwargs={'target': 'flood_proxy'},
    dag=training_dag,
)

# All training tasks run in parallel
[train_xgb_heatwave_t, train_xgb_flood_t, train_lstm_heatwave_t, train_lstm_flood_t]


# ============================================
# DAG 3: INFERENCE PIPELINE (runs every minute with 2-second internal loop)
# ============================================

inference_dag = DAG(
    'flood_heatwave_inference',
    default_args=default_args,
    description='Inference pipeline - runs every minute with 2-second internal loop',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
    tags=['inference', 'prediction', 'realtime'],
    max_active_runs=1,
)


def run_inference_loop(duration_seconds=58, interval_seconds=2, **context):
    """
    Run inference in a loop for the specified duration.
    This achieves 2-second inference intervals within Airflow's 1-minute minimum.
    """
    import sys
    sys.path.append('/opt/airflow/dags/tasks')
    from inference_task import run_inference
    
    start_time = time.time()
    iteration = 0
    
    print(f"🔮 Starting inference loop")
    print(f"   Duration: {duration_seconds}s")
    print(f"   Interval: {interval_seconds}s")
    
    while (time.time() - start_time) < duration_seconds:
        iteration += 1
        iter_start = time.time()
        
        print(f"\n--- Iteration {iteration} at {datetime.now().isoformat()} ---")
        
        try:
            # Run inference for all targets
            for target in ['heatwave', 'flood_proxy']:
                # Alternate between XGB and LSTM each iteration
                model_type = 'xgb' if iteration % 2 == 1 else 'lstm'
                try:
                    run_inference(model_type=model_type, target=target)
                except Exception as e:
                    print(f"⚠️  {model_type} {target} inference error: {e}")
        
        except Exception as e:
            print(f"❌ Iteration {iteration} failed: {e}")
        
        # Wait for next interval
        elapsed = time.time() - iter_start
        sleep_time = max(0, interval_seconds - elapsed)
        if sleep_time > 0 and (time.time() - start_time + sleep_time) < duration_seconds:
            time.sleep(sleep_time)
    
    print(f"\n✅ Inference loop completed. Total iterations: {iteration}")
    return {'iterations': iteration, 'duration': time.time() - start_time}


# Inference task with 2-second loop
inference_loop = PythonOperator(
    task_id='inference_loop',
    python_callable=run_inference_loop,
    op_kwargs={
        'duration_seconds': 58,
        'interval_seconds': INFERENCE_INTERVAL_SECONDS
    },
    dag=inference_dag,
)


# ============================================
# DAG 4: COMBINED PIPELINE (original - for backwards compatibility)
# ============================================

dag = DAG(
    'flood_heatwave_pipeline',
    default_args=default_args,
    description='Combined end-to-end flood and heatwave prediction pipeline',
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['combined', 'full-pipeline'],
)

# Task definitions for combined DAG
ingest_data = BashOperator(
    task_id='ingest_data',
    bash_command='cd /opt/airflow && python dags/tasks/data_ingest_task.py',
    dag=dag,
)

feature_engineering = BashOperator(
    task_id='feature_engineering',
    bash_command='cd /opt/airflow && python dags/tasks/feature_engineering_task.py',
    dag=dag,
)

labeling = BashOperator(
    task_id='labeling',
    bash_command='cd /opt/airflow && python dags/tasks/labeling_task.py',
    dag=dag,
)

train_xgb_heatwave = PythonOperator(
    task_id='train_xgb_heatwave',
    python_callable=call_xgb_training_api,
    op_kwargs={'target': 'heatwave'},
    dag=dag,
)

train_xgb_flood = PythonOperator(
    task_id='train_xgb_flood',
    python_callable=call_xgb_training_api,
    op_kwargs={'target': 'flood_proxy'},
    dag=dag,
)

train_lstm_heatwave = PythonOperator(
    task_id='train_lstm_heatwave',
    python_callable=call_lstm_training_api,
    op_kwargs={'target': 'heatwave'},
    dag=dag,
)

train_lstm_flood = PythonOperator(
    task_id='train_lstm_flood',
    python_callable=call_lstm_training_api,
    op_kwargs={'target': 'flood_proxy'},
    dag=dag,
)


def run_single_inference(**context):
    """Run a single inference pass for all models and targets."""
    import sys
    sys.path.append('/opt/airflow/dags/tasks')
    from inference_task import run_all_inference
    run_all_inference()


run_inference_task = PythonOperator(
    task_id='run_inference',
    python_callable=run_single_inference,
    dag=dag,
)

# Combined DAG dependencies
ingest_data >> feature_engineering >> labeling
labeling >> [train_xgb_heatwave, train_xgb_flood, train_lstm_heatwave, train_lstm_flood]
[train_xgb_heatwave, train_xgb_flood, train_lstm_heatwave, train_lstm_flood] >> run_inference_task
