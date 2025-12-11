"""
Airflow DAG for Flood and Heatwave Prediction Pipeline
Runs data ingestion, feature engineering, labeling, and triggers model training via Colab APIs

Training is triggered via REST APIs exposed by Colab notebooks:
- XGBoost Training API: Set XGB_TRAINING_API_URL environment variable
- LSTM Training API: Set LSTM_TRAINING_API_URL environment variable
"""
import os
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ============================================
# CONFIGURATION - COLAB TRAINING API URLs
# ============================================
# Set these environment variables or replace with actual ngrok URLs from Colab
XGB_TRAINING_API_URL = os.environ.get('XGB_TRAINING_API_URL', 'https://your-xgb-ngrok-url.ngrok.io')
LSTM_TRAINING_API_URL = os.environ.get('LSTM_TRAINING_API_URL', 'https://285555dd41dd.ngrok-free.app')

# Training parameters
TRAINING_TIMEOUT = 600  # 10 minutes timeout for training requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'flood_heatwave_pipeline',
    default_args=default_args,
    description='End-to-end flood and heatwave prediction pipeline with Colab training',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    catchup=False,
)


# ============================================
# TRAINING API CALL FUNCTIONS
# ============================================

# Headers for ngrok free tier (bypasses browser warning page)
NGROK_HEADERS = {
    'ngrok-skip-browser-warning': 'true',
    'Content-Type': 'application/json'
}

def call_xgb_training_api(target: str, **context):
    """Call XGBoost training API in Colab."""
    print(f"🚀 Triggering XGBoost training for: {target}")
    print(f"   API URL: {XGB_TRAINING_API_URL}")
    
    try:
        # Health check first
        health_resp = requests.get(f"{XGB_TRAINING_API_URL}/health", headers=NGROK_HEADERS, timeout=30)
        health_resp.raise_for_status()
        print(f"   ✅ API is healthy")
        
        # Trigger training
        response = requests.post(
            f"{XGB_TRAINING_API_URL}/train",
            json={
                'target': target,
                'test_days': 365
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
        raise


def call_lstm_training_api(target: str, **context):
    """Call LSTM training API in Colab."""
    print(f"🚀 Triggering LSTM training for: {target}")
    print(f"   API URL: {LSTM_TRAINING_API_URL}")
    
    try:
        # Health check first
        health_resp = requests.get(f"{LSTM_TRAINING_API_URL}/health", headers=NGROK_HEADERS, timeout=30)
        health_resp.raise_for_status()
        print(f"   ✅ API is healthy")
        
        # Trigger training
        response = requests.post(
            f"{LSTM_TRAINING_API_URL}/train",
            json={
                'target': target,
                'timesteps': 14,
                'epochs': 20
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
        raise


# ============================================
# TASK DEFINITIONS
# ============================================

# Task 1: Data Ingestion
ingest_data = BashOperator(
    task_id='ingest_data',
    bash_command='cd /opt/airflow && python dags/tasks/data_ingest_task.py',
    dag=dag,
)

# Task 2: Feature Engineering
feature_engineering = BashOperator(
    task_id='feature_engineering',
    bash_command='cd /opt/airflow && python dags/tasks/feature_engineering_task.py',
    dag=dag,
)

# Task 3: Labeling
labeling = BashOperator(
    task_id='labeling',
    bash_command='cd /opt/airflow && python dags/tasks/labeling_task.py',
    dag=dag,
)

# Task 4: Train XGBoost Heatwave Model (via Colab API)
train_xgb_heatwave = PythonOperator(
    task_id='train_xgb_heatwave',
    python_callable=call_xgb_training_api,
    op_kwargs={'target': 'heatwave'},
    dag=dag,
)

# Task 5: Train XGBoost Flood Model (via Colab API)
train_xgb_flood = PythonOperator(
    task_id='train_xgb_flood',
    python_callable=call_xgb_training_api,
    op_kwargs={'target': 'flood_proxy'},
    dag=dag,
)

# Task 6: Train LSTM Heatwave Model (via Colab API)
train_lstm_heatwave = PythonOperator(
    task_id='train_lstm_heatwave',
    python_callable=call_lstm_training_api,
    op_kwargs={'target': 'heatwave'},
    dag=dag,
)

# Task 7: Train LSTM Flood Model (via Colab API)
train_lstm_flood = PythonOperator(
    task_id='train_lstm_flood',
    python_callable=call_lstm_training_api,
    op_kwargs={'target': 'flood_proxy'},
    dag=dag,
)


# ============================================
# TASK DEPENDENCIES
# ============================================
# Data pipeline -> Training (XGBoost and LSTM in parallel)
ingest_data >> feature_engineering >> labeling

# After labeling, train all models
labeling >> [train_xgb_heatwave, train_xgb_flood, train_lstm_heatwave, train_lstm_flood]
