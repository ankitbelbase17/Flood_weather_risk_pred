"""
Real-Time Weather Prediction Dashboard
Fetches weather from OpenWeatherMap → Streams via Kafka → ML Prediction → Display

Features:
- District selection (Bara, Dhanusa, Sarlahi, Parsa, Siraha)
- Auto-streaming every 30 seconds from OpenWeatherMap
- Real-time heatwave and flood predictions
- Cassandra storage for historical data

Usage:
    1. Start Kafka: docker-compose up -d
    2. Start Producer API: python kafka_producer_api.py
    3. Run this dashboard: python weather_dashboard.py
"""

import os
import sys
import json
import time
import threading
import requests
from datetime import datetime
from queue import Queue, Empty
from collections import deque
import warnings
warnings.filterwarnings('ignore')

# Fix Windows console encoding for Unicode characters
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import gradio as gr
import pandas as pd
import numpy as np

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️  python-dotenv not installed. Run: pip install python-dotenv")

# Try importing Kafka
try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("⚠️  kafka-python not installed. Run: pip install kafka-python")

# Try importing ML libraries
try:
    import joblib
    import xgboost as xgb
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    print("⚠️  ML libraries not available")

# Try importing Cassandra
try:
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
    import uuid
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    print("⚠️  cassandra-driver not installed. Run: pip install cassandra-driver")


# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(','),
    'topic': os.getenv('KAFKA_TOPIC', 'weather-data'),
    'group_id': 'weather-dashboard-group',
    'auto_offset_reset': 'latest'
}

# Producer API endpoint
PRODUCER_API_URL = os.getenv('PRODUCER_API_URL', 'http://localhost:8000')

# Districts to monitor
DISTRICTS = ["Bara", "Dhanusa", "Sarlahi", "Parsa", "Siraha"]

# Streaming interval in seconds
STREAM_INTERVAL = 30

MODEL_PATHS = {
    'heatwave': 'models/xgb_heatwave_model.joblib',
    'flood': 'models/xgb_flood_proxy_model.joblib'
}

# Cassandra Configuration
CASSANDRA_CONFIG = {
    'hosts': os.getenv('CASSANDRA_HOSTS', '127.0.0.1').split(','),
    'port': int(os.getenv('CASSANDRA_PORT', '9042')),
    'keyspace': os.getenv('CASSANDRA_KEYSPACE', 'weather_monitoring')
}

# Airflow Configuration
AIRFLOW_CONFIG = {
    'base_url': os.getenv('AIRFLOW_BASE_URL', 'http://localhost:8090'),
    'username': os.getenv('AIRFLOW_USERNAME', 'admin'),
    'password': os.getenv('AIRFLOW_PASSWORD', 'admin')
}

# Inference Results JSON Path (shared with Airflow)
INFERENCE_RESULTS_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'data',
    'inference_results.json'
)


# ============================================================================
# INFERENCE RESULTS MANAGER
# ============================================================================

class InferenceResultsManager:
    """
    Manages inference results from JSON file.
    Airflow writes inference results to this JSON file,
    and the Gradio dashboard reads and displays them.
    """
    
    def __init__(self, json_path: str):
        self.json_path = json_path
        self.last_update = None
        self.cached_results = None
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Ensure the JSON file exists with default structure."""
        if not os.path.exists(self.json_path):
            os.makedirs(os.path.dirname(self.json_path), exist_ok=True)
            default_data = {
                "last_updated": None,
                "inference_count": 0,
                "models": {
                    "xgboost": {"heatwave": {"status": "pending", "last_run": None, "predictions": {}},
                               "flood": {"status": "pending", "last_run": None, "predictions": {}}},
                    "lstm": {"heatwave": {"status": "pending", "last_run": None, "predictions": {}},
                            "flood": {"status": "pending", "last_run": None, "predictions": {}}}
                },
                "districts": {},
                "latest_predictions": {"by_district": {}, "summary": {"high_risk_heatwave": 0, "high_risk_flood": 0, "total_predictions": 0}}
            }
            with open(self.json_path, 'w') as f:
                json.dump(default_data, f, indent=2)
    
    def load_results(self) -> dict:
        """Load inference results from JSON file."""
        try:
            if os.path.exists(self.json_path):
                with open(self.json_path, 'r') as f:
                    self.cached_results = json.load(f)
                    self.last_update = self.cached_results.get('last_updated')
                    return self.cached_results
        except Exception as e:
            print(f"⚠️  Error loading inference results: {e}")
        
        return self._get_default_results()
    
    def _get_default_results(self) -> dict:
        """Return default results structure."""
        return {
            "last_updated": None,
            "inference_count": 0,
            "models": {},
            "districts": {},
            "latest_predictions": {"by_district": {}, "summary": {}}
        }
    
    def get_district_predictions(self, district: str) -> dict:
        """Get predictions for a specific district."""
        results = self.load_results()
        districts = results.get('districts', {})
        return districts.get(district, {})
    
    def get_all_predictions(self) -> dict:
        """Get all latest predictions."""
        results = self.load_results()
        return results.get('latest_predictions', {})
    
    def get_model_status(self, model_type: str, target: str) -> dict:
        """Get status of a specific model."""
        results = self.load_results()
        models = results.get('models', {})
        return models.get(model_type, {}).get(target, {})
    
    def get_summary(self) -> dict:
        """Get inference summary."""
        results = self.load_results()
        return {
            'last_updated': results.get('last_updated'),
            'inference_count': results.get('inference_count', 0),
            'summary': results.get('latest_predictions', {}).get('summary', {})
        }
    
    def has_new_results(self) -> bool:
        """Check if there are new results since last check."""
        try:
            if os.path.exists(self.json_path):
                with open(self.json_path, 'r') as f:
                    data = json.load(f)
                    new_update = data.get('last_updated')
                    if new_update != self.last_update:
                        self.last_update = new_update
                        return True
        except:
            pass
        return False
    
    def sync_from_airflow(self) -> bool:
        """
        Sync inference results JSON from Airflow container.
        This allows the Gradio dashboard to read the latest results.
        """
        try:
            import subprocess
            result = subprocess.run(
                ['docker', 'cp', 
                 'airflow-airflow-standalone-1:/opt/airflow/data/inference_results.json',
                 self.json_path],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                print(f"✅ Synced inference results from Airflow container")
                return True
            else:
                # File might not exist yet in container
                return False
        except Exception as e:
            print(f"⚠️  Could not sync from Airflow: {e}")
            return False


# Initialize inference results manager
inference_manager = InferenceResultsManager(INFERENCE_RESULTS_JSON)


# ============================================================================
# CASSANDRA STORAGE
# ============================================================================

class CassandraStorage:
    """Cassandra storage for weather data and predictions."""
    
    def __init__(self, hosts: list, port: int, keyspace: str):
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        self.connected = False
        self.stats = {
            'records_stored': 0,
            'predictions_stored': 0,
            'errors': 0
        }
    
    def connect(self) -> bool:
        """Connect to Cassandra cluster."""
        if not CASSANDRA_AVAILABLE:
            print("⚠️  Cassandra driver not available")
            return False
        
        try:
            self.cluster = Cluster(contact_points=self.hosts, port=self.port)
            self.session = self.cluster.connect()
            self.connected = True
            print(f"✅ Connected to Cassandra at {self.hosts}:{self.port}")
            self._setup_schema()
            return True
        except Exception as e:
            print(f"⚠️  Cassandra connection failed: {e}")
            print("   Data will not be persisted to database.")
            self.connected = False
            return False
    
    def _setup_schema(self):
        """Create keyspace and tables if they don't exist."""
        # Create keyspace
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        
        self.session.set_keyspace(self.keyspace)
        
        # Table for weather observations
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS weather_observations (
                id UUID,
                district TEXT,
                date TEXT,
                fetch_time TIMESTAMP,
                max_temp DOUBLE,
                min_temp DOUBLE,
                temp_range DOUBLE,
                precipitation DOUBLE,
                humidity DOUBLE,
                wind_speed DOUBLE,
                pressure DOUBLE,
                cloudiness INT,
                visibility DOUBLE,
                weather_desc TEXT,
                sunrise TEXT,
                sunset TEXT,
                source TEXT,
                PRIMARY KEY ((district), fetch_time, id)
            ) WITH CLUSTERING ORDER BY (fetch_time DESC)
        """)
        
        # Table for predictions
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS weather_predictions (
                id UUID,
                district TEXT,
                prediction_time TIMESTAMP,
                max_temp DOUBLE,
                precipitation DOUBLE,
                humidity DOUBLE,
                heatwave_probability DOUBLE,
                flood_probability DOUBLE,
                heatwave_risk TEXT,
                flood_risk TEXT,
                PRIMARY KEY ((district), prediction_time, id)
            ) WITH CLUSTERING ORDER BY (prediction_time DESC)
        """)
        
        # Table for daily aggregates
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS daily_weather_summary (
                district TEXT,
                date TEXT,
                record_count INT,
                avg_temp DOUBLE,
                max_temp DOUBLE,
                min_temp DOUBLE,
                total_precip DOUBLE,
                avg_humidity DOUBLE,
                heatwave_alerts INT,
                flood_alerts INT,
                PRIMARY KEY ((district), date)
            ) WITH CLUSTERING ORDER BY (date DESC)
        """)
        
        print("✅ Cassandra schema ready")
    
    def store_weather_data(self, data: dict) -> bool:
        """Store weather observation in Cassandra."""
        if not self.connected:
            return False
        
        try:
            record_id = uuid.uuid4()
            fetch_time = datetime.fromisoformat(data.get('fetched_at', datetime.now().isoformat()))
            
            self.session.execute("""
                INSERT INTO weather_observations 
                (id, district, date, fetch_time, max_temp, min_temp, temp_range,
                 precipitation, humidity, wind_speed, pressure, cloudiness,
                 visibility, weather_desc, sunrise, sunset, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record_id,
                data.get('District', ''),
                data.get('Date', ''),
                fetch_time,
                float(data.get('MaxTemp_2m', 0)),
                float(data.get('MinTemp_2m', 0)),
                float(data.get('TempRange_2m', 0)),
                float(data.get('Precip', 0)),
                float(data.get('RH_2m', 0)),
                float(data.get('WindSpeed_10m', 0)),
                float(data.get('Pressure', 0)),
                int(data.get('Cloudiness', 0)),
                float(data.get('Visibility', 0)),
                data.get('WeatherDesc', ''),
                data.get('Sunrise', ''),
                data.get('Sunset', ''),
                data.get('source', 'openweathermap')
            ))
            
            self.stats['records_stored'] += 1
            return True
            
        except Exception as e:
            print(f"❌ Error storing weather data: {e}")
            self.stats['errors'] += 1
            return False
    
    def store_prediction(self, data: dict) -> bool:
        """Store prediction in Cassandra."""
        if not self.connected:
            return False
        
        try:
            record_id = uuid.uuid4()
            prediction_time = datetime.now()
            
            self.session.execute("""
                INSERT INTO weather_predictions
                (id, district, prediction_time, max_temp, precipitation, humidity,
                 heatwave_probability, flood_probability, heatwave_risk, flood_risk)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record_id,
                data.get('District', ''),
                prediction_time,
                float(data.get('MaxTemp_2m', 0)),
                float(data.get('Precip', 0)),
                float(data.get('RH_2m', 0)),
                float(data.get('heatwave_probability', 0)),
                float(data.get('flood_probability', 0)),
                data.get('heatwave_risk', 'LOW'),
                data.get('flood_risk', 'LOW')
            ))
            
            self.stats['predictions_stored'] += 1
            return True
            
        except Exception as e:
            print(f"❌ Error storing prediction: {e}")
            self.stats['errors'] += 1
            return False
    
    def get_recent_observations(self, district: str, limit: int = 20) -> list:
        """Get recent observations for a district."""
        if not self.connected:
            return []
        
        try:
            rows = self.session.execute("""
                SELECT * FROM weather_observations
                WHERE district = %s
                LIMIT %s
            """, (district, limit))
            
            return [dict(row._asdict()) for row in rows]
        except Exception as e:
            print(f"❌ Error fetching observations: {e}")
            return []
    
    def get_recent_predictions(self, district: str, limit: int = 20) -> list:
        """Get recent predictions for a district."""
        if not self.connected:
            return []
        
        try:
            rows = self.session.execute("""
                SELECT * FROM weather_predictions
                WHERE district = %s
                LIMIT %s
            """, (district, limit))
            
            return [dict(row._asdict()) for row in rows]
        except Exception as e:
            print(f"❌ Error fetching predictions: {e}")
            return []
    
    def query_observations(self, district: str, limit: int = 50) -> list:
        """Query observations from Cassandra for display."""
        if not self.connected:
            return []
        
        try:
            rows = self.session.execute("""
                SELECT district, fetch_time, date, max_temp, min_temp, temp_range,
                       precipitation, humidity, wind_speed, pressure, cloudiness,
                       visibility, weather_desc, source
                FROM weather_observations
                WHERE district = %s
                ORDER BY fetch_time DESC
                LIMIT %s
            """, (district, limit))
            
            results = []
            for row in rows:
                results.append({
                    'district': row.district,
                    'fetch_time': row.fetch_time.strftime('%Y-%m-%d %H:%M:%S') if row.fetch_time else None,
                    'date': row.date,
                    'max_temp': row.max_temp,
                    'min_temp': row.min_temp,
                    'precipitation': row.precipitation,
                    'humidity': row.humidity,
                    'wind_speed': row.wind_speed,
                    'pressure': row.pressure,
                    'cloudiness': row.cloudiness,
                    'weather_desc': row.weather_desc,
                    'source': row.source
                })
            return results
        except Exception as e:
            print(f"❌ Error querying observations: {e}")
            return []
    
    def query_predictions(self, district: str, limit: int = 50) -> list:
        """Query predictions from Cassandra for display."""
        if not self.connected:
            return []
        
        try:
            rows = self.session.execute("""
                SELECT district, prediction_time, max_temp, precipitation, humidity,
                       heatwave_probability, flood_probability, heatwave_risk, flood_risk
                FROM weather_predictions
                WHERE district = %s
                ORDER BY prediction_time DESC
                LIMIT %s
            """, (district, limit))
            
            results = []
            for row in rows:
                results.append({
                    'district': row.district,
                    'prediction_time': row.prediction_time.strftime('%Y-%m-%d %H:%M:%S') if row.prediction_time else None,
                    'max_temp': row.max_temp,
                    'precipitation': row.precipitation,
                    'humidity': row.humidity,
                    'heatwave_prob': round(row.heatwave_probability * 100, 2) if row.heatwave_probability else 0,
                    'flood_prob': round(row.flood_probability * 100, 2) if row.flood_probability else 0,
                    'heatwave_risk': row.heatwave_risk,
                    'flood_risk': row.flood_risk
                })
            return results
        except Exception as e:
            print(f"❌ Error querying predictions: {e}")
            return []
    
    def get_all_stats(self) -> dict:
        """Get statistics for all districts."""
        if not self.connected:
            return {}
        
        stats = {
            'total_observations': 0,
            'total_predictions': 0,
            'by_district': {}
        }
        
        for district in ['Bara', 'Dhanusa', 'Sarlahi', 'Parsa', 'Siraha']:
            try:
                obs_row = self.session.execute(
                    "SELECT COUNT(*) as cnt FROM weather_observations WHERE district = %s",
                    [district]
                ).one()
                pred_row = self.session.execute(
                    "SELECT COUNT(*) as cnt FROM weather_predictions WHERE district = %s",
                    [district]
                ).one()
                
                obs_count = obs_row.cnt if obs_row else 0
                pred_count = pred_row.cnt if pred_row else 0
                
                stats['by_district'][district] = {
                    'observations': obs_count,
                    'predictions': pred_count
                }
                stats['total_observations'] += obs_count
                stats['total_predictions'] += pred_count
            except:
                stats['by_district'][district] = {'observations': 0, 'predictions': 0}
        
        return stats
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        return self.stats.copy()
    
    def close(self):
        """Close Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            self.connected = False


# ============================================================================
# AIRFLOW INTEGRATION
# ============================================================================

class AirflowMonitor:
    """Monitor Airflow DAGs and task statuses."""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.auth = (username, password)
        self.connected = False
        self.last_check = None
        self._check_connection()
    
    def _check_connection(self) -> bool:
        """Check if Airflow is accessible."""
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/health",
                auth=self.auth,
                timeout=5
            )
            self.connected = response.status_code == 200
            return self.connected
        except:
            self.connected = False
            return False
    
    def get_dags(self) -> list:
        """Get list of all DAGs."""
        if not self.connected:
            self._check_connection()
        
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/dags",
                auth=self.auth,
                timeout=10
            )
            if response.status_code == 200:
                return response.json().get('dags', [])
        except:
            pass
        return []
    
    def get_dag_runs(self, dag_id: str, limit: int = 5) -> list:
        """Get recent DAG runs."""
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
                auth=self.auth,
                params={'limit': limit, 'order_by': '-execution_date'},
                timeout=10
            )
            if response.status_code == 200:
                return response.json().get('dag_runs', [])
        except:
            pass
        return []
    
    def get_task_instances(self, dag_id: str, dag_run_id: str) -> list:
        """Get task instances for a DAG run."""
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
                auth=self.auth,
                timeout=10
            )
            if response.status_code == 200:
                return response.json().get('task_instances', [])
        except:
            pass
        return []
    
    def trigger_dag(self, dag_id: str) -> dict:
        """Trigger a DAG run."""
        try:
            response = requests.post(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
                auth=self.auth,
                json={'conf': {}},
                timeout=10
            )
            if response.status_code in [200, 201]:
                return {'success': True, 'data': response.json()}
            return {'success': False, 'error': response.text}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_pipeline_status(self) -> dict:
        """Get comprehensive pipeline status."""
        status = {
            'airflow_connected': self.connected,
            'dags': {},
            'training': {'status': 'unknown', 'last_run': None, 'tasks': []},
            'inference': {'status': 'unknown', 'last_run': None, 'tasks': []},
            'data_pipeline': {'status': 'unknown', 'last_run': None, 'tasks': []}
        }
        
        if not self.connected:
            self._check_connection()
            if not self.connected:
                return status
        
        # Define DAG mappings
        dag_mapping = {
            'flood_heatwave_training': 'training',
            'flood_heatwave_inference': 'inference',
            'flood_heatwave_data_pipeline': 'data_pipeline',
            'flood_heatwave_pipeline': 'data_pipeline'  # Combined pipeline
        }
        
        dags = self.get_dags()
        for dag in dags:
            dag_id = dag.get('dag_id', '')
            status['dags'][dag_id] = {
                'is_paused': dag.get('is_paused', True),
                'is_active': dag.get('is_active', False)
            }
            
            # Get recent runs for relevant DAGs
            if dag_id in dag_mapping:
                category = dag_mapping[dag_id]
                runs = self.get_dag_runs(dag_id, limit=3)
                
                if runs:
                    latest_run = runs[0]
                    status[category]['status'] = latest_run.get('state', 'unknown')
                    status[category]['last_run'] = latest_run.get('execution_date', '')
                    status[category]['dag_run_id'] = latest_run.get('dag_run_id', '')
                    
                    # Get task instances for latest run
                    tasks = self.get_task_instances(dag_id, latest_run.get('dag_run_id', ''))
                    status[category]['tasks'] = [
                        {
                            'task_id': t.get('task_id'),
                            'state': t.get('state'),
                            'duration': t.get('duration'),
                            'start_date': t.get('start_date'),
                            'end_date': t.get('end_date')
                        }
                        for t in tasks
                    ]
        
        self.last_check = datetime.now().isoformat()
        return status


# ============================================================================
# ML PREDICTOR
# ============================================================================

class WeatherPredictor:
    """ML model wrapper for weather predictions."""
    
    def __init__(self):
        self.heatwave_model = None
        self.flood_model = None
        self._load_models()
    
    def _load_models(self):
        """Load XGBoost models."""
        try:
            if os.path.exists(MODEL_PATHS['heatwave']):
                self.heatwave_model = joblib.load(MODEL_PATHS['heatwave'])
                print("✅ Loaded heatwave model")
            else:
                print(f"⚠️  Heatwave model not found at {MODEL_PATHS['heatwave']}")
                
            if os.path.exists(MODEL_PATHS['flood']):
                self.flood_model = joblib.load(MODEL_PATHS['flood'])
                print("✅ Loaded flood model")
            else:
                print(f"⚠️  Flood model not found at {MODEL_PATHS['flood']}")
        except Exception as e:
            print(f"❌ Error loading models: {e}")
    
    def predict(self, data: dict) -> dict:
        """Make predictions from weather data."""
        features = self._extract_features(data)
        
        heatwave_prob = 0.0
        flood_prob = 0.0
        
        # Heatwave prediction
        if self.heatwave_model is not None:
            try:
                if isinstance(self.heatwave_model, xgb.Booster):
                    dmatrix = xgb.DMatrix([list(features.values())], feature_names=list(features.keys()))
                    heatwave_prob = float(self.heatwave_model.predict(dmatrix)[0])
                else:
                    heatwave_prob = float(self.heatwave_model.predict_proba([list(features.values())])[0][1])
            except Exception as e:
                heatwave_prob = self._rule_based_heatwave(data)
        else:
            heatwave_prob = self._rule_based_heatwave(data)
        
        # Flood prediction
        if self.flood_model is not None:
            try:
                if isinstance(self.flood_model, xgb.Booster):
                    dmatrix = xgb.DMatrix([list(features.values())], feature_names=list(features.keys()))
                    flood_prob = float(self.flood_model.predict(dmatrix)[0])
                else:
                    flood_prob = float(self.flood_model.predict_proba([list(features.values())])[0][1])
            except Exception as e:
                flood_prob = self._rule_based_flood(data)
        else:
            flood_prob = self._rule_based_flood(data)
        
        return {
            'heatwave_probability': heatwave_prob,
            'flood_probability': flood_prob,
            'heatwave_risk': 'HIGH' if heatwave_prob > 0.5 else 'MEDIUM' if heatwave_prob > 0.3 else 'LOW',
            'flood_risk': 'HIGH' if flood_prob > 0.5 else 'MEDIUM' if flood_prob > 0.3 else 'LOW'
        }
    
    def _rule_based_heatwave(self, data: dict) -> float:
        """Rule-based heatwave prediction."""
        temp = data.get('MaxTemp_2m', data.get('temp_max', 30))
        humidity = data.get('RH_2m', data.get('humidity', 50))
        # Heat index consideration
        if temp > 40:
            return min(1.0, 0.7 + (temp - 40) / 20)
        elif temp > 35:
            return min(1.0, 0.3 + (temp - 35) / 10)
        else:
            return max(0.0, (temp - 30) / 20)
    
    def _rule_based_flood(self, data: dict) -> float:
        """Rule-based flood prediction."""
        precip = data.get('Precip', data.get('precipitation', 0))
        humidity = data.get('RH_2m', data.get('humidity', 50))
        
        flood_score = 0.0
        if precip > 100:
            flood_score = 0.8
        elif precip > 50:
            flood_score = 0.5
        elif precip > 20:
            flood_score = 0.3
        
        # Humidity factor
        if humidity > 90:
            flood_score += 0.2
        elif humidity > 80:
            flood_score += 0.1
        
        return min(1.0, flood_score)
    
    def _extract_features(self, data: dict) -> dict:
        """Extract model features from raw data."""
        temp = data.get('MaxTemp_2m', data.get('temp_max', 30))
        precip = data.get('Precip', data.get('precipitation', 0))
        humidity = data.get('RH_2m', data.get('humidity', 50))
        
        return {
            'Precip': precip,
            'precip_3d': precip * 3,
            'precip_7d': precip * 7,
            'precip_lag_1': precip,
            'precip_lag_3': precip,
            'precip_lag_7': precip,
            'MaxTemp_2m': temp,
            'maxT_3d_mean': temp,
            'maxT_lag_1': temp,
            'maxT_lag_3': temp,
            'anom_maxT': temp - 30,
            'RH_2m': humidity,
            'wetness_flag': 1 if humidity > 80 else 0,
            'API': precip * 0.9,
            'TempRange_2m': data.get('TempRange_2m', data.get('temp_range', 10)),
            'WindSpeed_10m': data.get('WindSpeed_10m', data.get('wind_speed', 5)),
            'WindSpeed_50m': data.get('WindSpeed_50m', 10),
            'doy_sin': np.sin(2 * np.pi * datetime.now().timetuple().tm_yday / 365),
            'doy_cos': np.cos(2 * np.pi * datetime.now().timetuple().tm_yday / 365),
            'month': datetime.now().month,
            'year': datetime.now().year
        }


# ============================================================================
# WEATHER STREAMING SERVICE
# ============================================================================

class WeatherStreamingService:
    """Service to fetch weather data and stream via Kafka."""
    
    def __init__(self, producer_api_url: str, cassandra_storage: CassandraStorage = None):
        self.producer_api_url = producer_api_url
        self.cassandra = cassandra_storage
        self.streaming = False
        self.current_district = None
        self.stream_thread = None
        self.data_history = deque(maxlen=50)
        self.predictor = WeatherPredictor()
        self.stats = {
            'api_calls': 0,
            'successful': 0,
            'errors': 0,
            'cassandra_stored': 0,
            'last_fetch_time': None
        }
        self.latest_data = None
        self.latest_prediction = None
    
    def fetch_weather(self, district: str) -> dict:
        """Fetch weather for a district from OpenWeatherMap via Producer API."""
        try:
            # Call the producer API which fetches from OpenWeatherMap
            url = f"{self.producer_api_url}/weather/city/{district}"
            response = requests.post(url, timeout=15)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                self.stats['successful'] += 1
                self.stats['last_fetch_time'] = datetime.now().isoformat()
                
                # Extract weather data
                weather = data.get('weather', {})
                
                # Transform to our format
                result = {
                    'District': district,
                    'Date': weather.get('date', datetime.now().strftime('%Y-%m-%d')),
                    'MaxTemp_2m': weather.get('temp_max', weather.get('temperature', 0)),
                    'MinTemp_2m': weather.get('temp_min', 0),
                    'TempRange_2m': weather.get('temp_range', 0),
                    'Precip': weather.get('precipitation', 0),
                    'RH_2m': weather.get('humidity', 0),
                    'WindSpeed_10m': weather.get('wind_speed', 0),
                    'Pressure': weather.get('pressure', 0),
                    'Cloudiness': weather.get('cloudiness', 0),
                    'Visibility': weather.get('visibility_km', 0),
                    'WeatherDesc': weather.get('weather_description', ''),
                    'Sunrise': weather.get('sunrise', ''),
                    'Sunset': weather.get('sunset', ''),
                    'fetched_at': datetime.now().isoformat(),
                    'kafka_published': data.get('kafka_published', False),
                    'source': 'openweathermap'
                }
                
                # Make prediction
                prediction = self.predictor.predict(result)
                result.update(prediction)
                
                self.latest_data = result
                self.latest_prediction = prediction
                self.data_history.append(result)
                
                # Store in Cassandra
                if self.cassandra and self.cassandra.connected:
                    if self.cassandra.store_weather_data(result):
                        self.stats['cassandra_stored'] += 1
                    self.cassandra.store_prediction(result)
                
                return result
            else:
                self.stats['errors'] += 1
                error_msg = response.json().get('detail', 'Unknown error')
                return {'error': error_msg, 'District': district}
                
        except requests.exceptions.ConnectionError:
            self.stats['errors'] += 1
            return {'error': 'Producer API not running. Start with: python kafka_producer_api.py', 'District': district}
        except Exception as e:
            self.stats['errors'] += 1
            return {'error': str(e), 'District': district}
    
    def start_streaming(self, district: str, interval: int = 30):
        """Start auto-streaming for a district."""
        self.current_district = district
        self.streaming = True
        
        def stream_loop():
            while self.streaming and self.current_district:
                self.fetch_weather(self.current_district)
                time.sleep(interval)
        
        self.stream_thread = threading.Thread(target=stream_loop, daemon=True)
        self.stream_thread.start()
        print(f"🚀 Started streaming for {district} every {interval}s")
    
    def stop_streaming(self):
        """Stop auto-streaming."""
        self.streaming = False
        self.current_district = None
        print("⏹️  Streaming stopped")
    
    def get_history(self) -> list:
        """Get data history."""
        return list(self.data_history)
    
    def get_stats(self) -> dict:
        """Get service statistics."""
        return self.stats.copy()


# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

# Initialize Cassandra storage
cassandra_storage = CassandraStorage(
    hosts=CASSANDRA_CONFIG['hosts'],
    port=CASSANDRA_CONFIG['port'],
    keyspace=CASSANDRA_CONFIG['keyspace']
)
cassandra_storage.connect()

# Initialize Airflow monitor
airflow_monitor = AirflowMonitor(
    base_url=AIRFLOW_CONFIG['base_url'],
    username=AIRFLOW_CONFIG['username'],
    password=AIRFLOW_CONFIG['password']
)

# Initialize streaming service with Cassandra
streaming_service = WeatherStreamingService(PRODUCER_API_URL, cassandra_storage)


# ============================================================================
# DASHBOARD FUNCTIONS
# ============================================================================

def create_weather_card(data: dict) -> str:
    """Create HTML card for current weather."""
    if not data or 'error' in data:
        error_msg = data.get('error', 'No data available') if data else 'Select a district to start'
        return f"""
        <div style="text-align: center; padding: 40px; color: #64748b; background: #1e293b; border-radius: 16px;">
            <div style="font-size: 48px;">🌤️</div>
            <div style="font-size: 18px; margin-top: 10px;">{error_msg}</div>
        </div>
        """
    
    temp = data.get('MaxTemp_2m', 0)
    precip = data.get('Precip', 0)
    humidity = data.get('RH_2m', 0)
    wind = data.get('WindSpeed_10m', 0)
    desc = data.get('WeatherDesc', 'N/A').title()
    
    temp_color = "#ef4444" if temp > 40 else "#f59e0b" if temp > 35 else "#22c55e"
    
    return f"""
    <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 25px; border-radius: 20px; border: 1px solid #475569;">
        <div style="text-align: center; margin-bottom: 20px;">
            <div style="font-size: 18px; color: #94a3b8;">📍 {data.get('District', 'Unknown')}</div>
            <div style="font-size: 14px; color: #64748b;">{data.get('Date', 'N/A')} | {desc}</div>
        </div>
        
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px;">
            <div style="text-align: center; background: rgba(0,0,0,0.2); padding: 20px; border-radius: 12px;">
                <div style="font-size: 48px; color: {temp_color}; font-weight: bold;">{temp:.1f}°C</div>
                <div style="color: #94a3b8; margin-top: 5px;">🌡️ Temperature</div>
            </div>
            <div style="text-align: center; background: rgba(0,0,0,0.2); padding: 20px; border-radius: 12px;">
                <div style="font-size: 48px; color: #3b82f6; font-weight: bold;">{humidity:.0f}%</div>
                <div style="color: #94a3b8; margin-top: 5px;">💧 Humidity</div>
            </div>
            <div style="text-align: center; background: rgba(0,0,0,0.2); padding: 20px; border-radius: 12px;">
                <div style="font-size: 48px; color: #06b6d4; font-weight: bold;">{precip:.1f}mm</div>
                <div style="color: #94a3b8; margin-top: 5px;">🌧️ Precipitation</div>
            </div>
            <div style="text-align: center; background: rgba(0,0,0,0.2); padding: 20px; border-radius: 12px;">
                <div style="font-size: 48px; color: #a855f7; font-weight: bold;">{wind:.1f}</div>
                <div style="color: #94a3b8; margin-top: 5px;">💨 Wind (m/s)</div>
            </div>
        </div>
        
        <div style="margin-top: 15px; text-align: center; color: #64748b; font-size: 12px;">
            ☀️ Sunrise: {data.get('Sunrise', 'N/A')} | 🌙 Sunset: {data.get('Sunset', 'N/A')}
        </div>
    </div>
    """


def create_prediction_card(data: dict) -> str:
    """Create HTML card for predictions."""
    if not data or 'error' in data:
        return """
        <div style="text-align: center; padding: 40px; color: #64748b; background: #1e293b; border-radius: 16px;">
            <div style="font-size: 48px;">🔮</div>
            <div style="font-size: 18px; margin-top: 10px;">Awaiting weather data for prediction...</div>
        </div>
        """
    
    heatwave_prob = data.get('heatwave_probability', 0) * 100
    flood_prob = data.get('flood_probability', 0) * 100
    heatwave_risk = data.get('heatwave_risk', 'LOW')
    flood_risk = data.get('flood_risk', 'LOW')
    
    hw_color = "#ef4444" if heatwave_risk == 'HIGH' else "#f59e0b" if heatwave_risk == 'MEDIUM' else "#22c55e"
    fl_color = "#ef4444" if flood_risk == 'HIGH' else "#f59e0b" if flood_risk == 'MEDIUM' else "#22c55e"
    
    hw_bg = "rgba(239,68,68,0.2)" if heatwave_risk == 'HIGH' else "rgba(245,158,11,0.2)" if heatwave_risk == 'MEDIUM' else "rgba(34,197,94,0.2)"
    fl_bg = "rgba(239,68,68,0.2)" if flood_risk == 'HIGH' else "rgba(245,158,11,0.2)" if flood_risk == 'MEDIUM' else "rgba(34,197,94,0.2)"
    
    return f"""
    <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 25px; border-radius: 20px; border: 1px solid #475569;">
        <div style="text-align: center; margin-bottom: 20px;">
            <div style="font-size: 18px; color: #94a3b8;">🔮 ML Predictions</div>
            <div style="font-size: 14px; color: #64748b;">Based on current weather conditions</div>
        </div>
        
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px;">
            <div style="text-align: center; background: {hw_bg}; padding: 25px; border-radius: 16px; border: 2px solid {hw_color};">
                <div style="font-size: 36px;">🔥</div>
                <div style="font-size: 14px; color: #94a3b8; margin: 10px 0;">Heatwave Risk</div>
                <div style="font-size: 42px; color: {hw_color}; font-weight: bold;">{heatwave_prob:.1f}%</div>
                <div style="font-size: 18px; color: {hw_color}; font-weight: 600; margin-top: 10px; 
                            padding: 5px 15px; background: rgba(0,0,0,0.3); border-radius: 20px; display: inline-block;">
                    {heatwave_risk}
                </div>
            </div>
            <div style="text-align: center; background: {fl_bg}; padding: 25px; border-radius: 16px; border: 2px solid {fl_color};">
                <div style="font-size: 36px;">🌊</div>
                <div style="font-size: 14px; color: #94a3b8; margin: 10px 0;">Flood Risk</div>
                <div style="font-size: 42px; color: {fl_color}; font-weight: bold;">{flood_prob:.1f}%</div>
                <div style="font-size: 18px; color: {fl_color}; font-weight: 600; margin-top: 10px;
                            padding: 5px 15px; background: rgba(0,0,0,0.3); border-radius: 20px; display: inline-block;">
                    {flood_risk}
                </div>
            </div>
        </div>
    </div>
    """


def create_status_html(streaming: bool, district: str, stats: dict, cassandra_stats: dict = None) -> str:
    """Create status bar HTML."""
    status_color = "#22c55e" if streaming else "#64748b"
    status_text = f"🔴 LIVE - Streaming {district}" if streaming else "⚫ Stopped"
    
    cassandra_stored = stats.get('cassandra_stored', 0)
    cassandra_status = "🟢" if cassandra_storage.connected else "🔴"
    
    return f"""
    <div style="display: flex; justify-content: space-between; align-items: center; 
                background: linear-gradient(135deg, #1e293b, #0f172a); padding: 15px 25px; 
                border-radius: 12px; border: 1px solid #334155;">
        <div style="display: flex; align-items: center; gap: 15px;">
            <div style="width: 12px; height: 12px; background: {status_color}; border-radius: 50%; 
                        animation: {'pulse 1.5s infinite' if streaming else 'none'};"></div>
            <span style="color: #f1f5f9; font-weight: 600;">{status_text}</span>
        </div>
        <div style="display: flex; gap: 20px; color: #94a3b8; font-size: 14px;">
            <span>📡 API: {stats.get('api_calls', 0)}</span>
            <span>✅ Success: {stats.get('successful', 0)}</span>
            <span>{cassandra_status} Cassandra: {cassandra_stored}</span>
            <span>❌ Errors: {stats.get('errors', 0)}</span>
        </div>
    </div>
    <style>
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
        }}
    </style>
    """


def create_history_table(history: list) -> pd.DataFrame:
    """Create history DataFrame."""
    if not history:
        return pd.DataFrame(columns=['Time', 'District', 'Temp (°C)', 'Humidity (%)', 'Precip (mm)', 'Heatwave %', 'Flood %'])
    
    rows = []
    for item in reversed(history[-15:]):
        if 'error' not in item:
            rows.append({
                'Time': item.get('fetched_at', '')[:19].replace('T', ' '),
                'District': item.get('District', ''),
                'Temp (°C)': round(item.get('MaxTemp_2m', 0), 1),
                'Humidity (%)': round(item.get('RH_2m', 0), 0),
                'Precip (mm)': round(item.get('Precip', 0), 1),
                'Heatwave %': round(item.get('heatwave_probability', 0) * 100, 1),
                'Flood %': round(item.get('flood_probability', 0) * 100, 1)
            })
    
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=['Time', 'District', 'Temp (°C)', 'Humidity (%)', 'Precip (mm)', 'Heatwave %', 'Flood %'])


# ============================================================================
# CASSANDRA QUERY FUNCTIONS
# ============================================================================

def query_cassandra_observations(district: str, limit: int) -> tuple:
    """Query observations from Cassandra."""
    if not cassandra_storage.connected:
        return (
            pd.DataFrame(),
            "❌ Cassandra not connected. Start Docker: docker-compose up -d",
            create_cassandra_stats_html({})
        )
    
    if not district:
        return (
            pd.DataFrame(),
            "⚠️ Please select a district",
            create_cassandra_stats_html(cassandra_storage.get_all_stats())
        )
    
    data = cassandra_storage.query_observations(district, int(limit))
    
    if not data:
        return (
            pd.DataFrame(),
            f"ℹ️ No observations found for {district}",
            create_cassandra_stats_html(cassandra_storage.get_all_stats())
        )
    
    df = pd.DataFrame(data)
    # Rename columns for better display
    df.columns = ['District', 'Timestamp', 'Date', 'Max Temp', 'Min Temp', 
                  'Precip', 'Humidity', 'Wind', 'Pressure', 'Clouds', 'Weather', 'Source']
    
    return (
        df,
        f"✅ Found {len(data)} observations for {district}",
        create_cassandra_stats_html(cassandra_storage.get_all_stats())
    )


def query_cassandra_predictions(district: str, limit: int) -> tuple:
    """Query predictions from Cassandra."""
    if not cassandra_storage.connected:
        return (
            pd.DataFrame(),
            "❌ Cassandra not connected. Start Docker: docker-compose up -d",
            create_cassandra_stats_html({})
        )
    
    if not district:
        return (
            pd.DataFrame(),
            "⚠️ Please select a district",
            create_cassandra_stats_html(cassandra_storage.get_all_stats())
        )
    
    data = cassandra_storage.query_predictions(district, int(limit))
    
    if not data:
        return (
            pd.DataFrame(),
            f"ℹ️ No predictions found for {district}",
            create_cassandra_stats_html(cassandra_storage.get_all_stats())
        )
    
    df = pd.DataFrame(data)
    df.columns = ['District', 'Timestamp', 'Max Temp', 'Precip', 'Humidity',
                  'Heatwave %', 'Flood %', 'Heatwave Risk', 'Flood Risk']
    
    return (
        df,
        f"✅ Found {len(data)} predictions for {district}",
        create_cassandra_stats_html(cassandra_storage.get_all_stats())
    )


# ============================================================================
# INFERENCE RESULTS DISPLAY FUNCTIONS
# ============================================================================

def create_inference_results_html(district: str = None) -> str:
    """Create HTML display for inference results from JSON file."""
    results = inference_manager.load_results()
    
    if not results or not results.get('last_updated'):
        return """
        <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 25px; border-radius: 16px; border: 1px solid #475569;">
            <div style="text-align: center; color: #f59e0b;">
                <div style="font-size: 48px;">⏳</div>
                <div style="font-size: 18px; margin-top: 10px; font-weight: 600;">Waiting for Inference Results</div>
                <div style="font-size: 13px; color: #64748b; margin-top: 8px;">
                    No inference results available yet.<br>
                    Airflow will run inference automatically or trigger it manually.
                </div>
            </div>
        </div>
        """
    
    last_updated = results.get('last_updated', 'N/A')
    if last_updated and last_updated != 'N/A':
        last_updated = last_updated[:19].replace('T', ' ')
    
    inference_count = results.get('inference_count', 0)
    summary = results.get('latest_predictions', {}).get('summary', {})
    high_risk_heatwave = summary.get('high_risk_heatwave', 0)
    high_risk_flood = summary.get('high_risk_flood', 0)
    
    # Get model statuses
    models = results.get('models', {})
    
    model_status_html = ""
    for model_name, targets in models.items():
        for target, info in targets.items():
            status = info.get('status', 'pending')
            last_run = info.get('last_run', 'N/A')
            if last_run and last_run != 'N/A':
                last_run = last_run[:19].replace('T', ' ')
            
            status_icon = '✅' if status == 'success' else '⚠️' if status == 'fallback' else '⏳'
            status_color = '#22c55e' if status == 'success' else '#f59e0b' if status == 'fallback' else '#64748b'
            
            model_status_html += f"""
            <div style="display: flex; align-items: center; gap: 8px; padding: 8px 12px; background: rgba(0,0,0,0.2); border-radius: 8px; margin: 4px 0;">
                <span>{status_icon}</span>
                <span style="color: #e2e8f0; font-weight: 500; flex: 1;">{model_name.upper()} - {target.title()}</span>
                <span style="color: {status_color}; font-size: 12px;">{status.upper()}</span>
            </div>
            """
    
    # District predictions
    districts_data = results.get('districts', {})
    district_cards_html = ""
    
    for dist_name, dist_predictions in districts_data.items():
        if district and dist_name != district:
            continue
        
        # Get best prediction for each target
        heatwave_pred = dist_predictions.get('heatwave', {})
        flood_pred = dist_predictions.get('flood', {})
        
        # Find the best model prediction
        hw_prob = 0
        hw_risk = 'LOW'
        hw_model = 'N/A'
        for model in ['xgboost', 'lstm', 'rule']:
            if model in heatwave_pred:
                hw_prob = heatwave_pred[model].get('probability', 0)
                hw_risk = heatwave_pred[model].get('risk', 'LOW')
                hw_model = model.upper()
                break
        
        fl_prob = 0
        fl_risk = 'LOW'
        fl_model = 'N/A'
        for model in ['xgboost', 'lstm', 'rule']:
            if model in flood_pred:
                fl_prob = flood_pred[model].get('probability', 0)
                fl_risk = flood_pred[model].get('risk', 'LOW')
                fl_model = model.upper()
                break
        
        hw_color = '#ef4444' if hw_risk == 'HIGH' else '#f59e0b' if hw_risk == 'MEDIUM' else '#22c55e'
        fl_color = '#ef4444' if fl_risk == 'HIGH' else '#f59e0b' if fl_risk == 'MEDIUM' else '#22c55e'
        
        district_cards_html += f"""
        <div style="background: rgba(0,0,0,0.3); padding: 15px; border-radius: 12px; margin: 8px 0;">
            <div style="color: #f1f5f9; font-weight: 600; font-size: 14px; margin-bottom: 10px;">📍 {dist_name}</div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                <div style="background: rgba(239,68,68,0.1); padding: 10px; border-radius: 8px; border-left: 3px solid {hw_color};">
                    <div style="color: #94a3b8; font-size: 11px;">🔥 Heatwave</div>
                    <div style="color: {hw_color}; font-size: 18px; font-weight: bold;">{hw_risk}</div>
                    <div style="color: #64748b; font-size: 11px;">{hw_prob*100:.1f}% ({hw_model})</div>
                </div>
                <div style="background: rgba(59,130,246,0.1); padding: 10px; border-radius: 8px; border-left: 3px solid {fl_color};">
                    <div style="color: #94a3b8; font-size: 11px;">🌊 Flood</div>
                    <div style="color: {fl_color}; font-size: 18px; font-weight: bold;">{fl_risk}</div>
                    <div style="color: #64748b; font-size: 11px;">{fl_prob*100:.1f}% ({fl_model})</div>
                </div>
            </div>
        </div>
        """
    
    if not district_cards_html:
        district_cards_html = """
        <div style="text-align: center; color: #64748b; padding: 20px;">
            No district predictions available yet.
        </div>
        """
    
    return f"""
    <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 20px; border-radius: 16px; border: 1px solid #475569;">
        <div style="text-align: center; margin-bottom: 20px;">
            <div style="font-size: 20px; color: #f1f5f9; font-weight: 600;">🤖 ML Inference Results</div>
            <div style="color: #22c55e; font-size: 13px; margin-top: 5px;">
                Last Updated: {last_updated} | Total Runs: {inference_count}
            </div>
        </div>
        
        <!-- Summary Cards -->
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin-bottom: 20px;">
            <div style="background: linear-gradient(135deg, rgba(239,68,68,0.2), rgba(239,68,68,0.1)); padding: 15px; border-radius: 12px; text-align: center;">
                <div style="font-size: 32px;">🔥</div>
                <div style="color: #ef4444; font-size: 28px; font-weight: bold;">{high_risk_heatwave}</div>
                <div style="color: #94a3b8; font-size: 12px;">High Risk Heatwave</div>
            </div>
            <div style="background: linear-gradient(135deg, rgba(59,130,246,0.2), rgba(59,130,246,0.1)); padding: 15px; border-radius: 12px; text-align: center;">
                <div style="font-size: 32px;">🌊</div>
                <div style="color: #3b82f6; font-size: 28px; font-weight: bold;">{high_risk_flood}</div>
                <div style="color: #94a3b8; font-size: 12px;">High Risk Flood</div>
            </div>
        </div>
        
        <!-- Model Status -->
        <div style="margin-bottom: 20px;">
            <div style="color: #94a3b8; font-size: 13px; margin-bottom: 8px; font-weight: 500;">📊 Model Status</div>
            {model_status_html}
        </div>
        
        <!-- District Predictions -->
        <div>
            <div style="color: #94a3b8; font-size: 13px; margin-bottom: 8px; font-weight: 500;">📍 District Predictions</div>
            <div style="max-height: 400px; overflow-y: auto;">
                {district_cards_html}
            </div>
        </div>
        
        <!-- Footer -->
        <div style="text-align: center; margin-top: 15px; padding-top: 15px; border-top: 1px solid rgba(255,255,255,0.1);">
            <div style="color: #64748b; font-size: 11px;">
                Results loaded from: {INFERENCE_RESULTS_JSON}
            </div>
        </div>
    </div>
    """


def refresh_inference_results() -> str:
    """Refresh inference results display by syncing from Airflow first."""
    # Try to sync from Airflow container
    inference_manager.sync_from_airflow()
    return create_inference_results_html()


def get_district_inference(district: str) -> str:
    """Get inference results for a specific district."""
    if not district:
        return create_inference_results_html()
    return create_inference_results_html(district)


def query_all_districts(data_type: str, limit: int) -> tuple:
    """Query data for all districts."""
    if not cassandra_storage.connected:
        return (
            pd.DataFrame(),
            "❌ Cassandra not connected",
            create_cassandra_stats_html({})
        )
    
    all_data = []
    for district in DISTRICTS:
        if data_type == "Observations":
            data = cassandra_storage.query_observations(district, int(limit) // 5)
        else:
            data = cassandra_storage.query_predictions(district, int(limit) // 5)
        all_data.extend(data)
    
    if not all_data:
        return (
            pd.DataFrame(),
            f"ℹ️ No {data_type.lower()} found",
            create_cassandra_stats_html(cassandra_storage.get_all_stats())
        )
    
    df = pd.DataFrame(all_data)
    
    # Sort by timestamp
    time_col = 'fetch_time' if data_type == "Observations" else 'prediction_time'
    if time_col in df.columns:
        df = df.sort_values(time_col, ascending=False)
    
    return (
        df,
        f"✅ Found {len(all_data)} {data_type.lower()} across all districts",
        create_cassandra_stats_html(cassandra_storage.get_all_stats())
    )


def refresh_cassandra_stats() -> str:
    """Refresh Cassandra statistics."""
    return create_cassandra_stats_html(cassandra_storage.get_all_stats())


def create_cassandra_stats_html(stats: dict) -> str:
    """Create HTML for Cassandra statistics."""
    if not stats or not cassandra_storage.connected:
        return """
        <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 20px; border-radius: 16px; border: 1px solid #475569;">
            <div style="text-align: center; color: #ef4444;">
                <div style="font-size: 36px;">🔴</div>
                <div style="font-size: 16px; margin-top: 10px;">Cassandra Not Connected</div>
                <div style="font-size: 12px; color: #64748b; margin-top: 5px;">Run: docker-compose up -d</div>
            </div>
        </div>
        """
    
    total_obs = stats.get('total_observations', 0)
    total_pred = stats.get('total_predictions', 0)
    by_district = stats.get('by_district', {})
    
    district_rows = ""
    for district, counts in by_district.items():
        obs = counts.get('observations', 0)
        pred = counts.get('predictions', 0)
        bar_width = min(obs * 2, 100)
        district_rows += f"""
        <div style="display: flex; align-items: center; margin: 8px 0; padding: 8px; background: rgba(0,0,0,0.2); border-radius: 8px;">
            <div style="width: 80px; color: #94a3b8; font-weight: 500;">{district}</div>
            <div style="flex: 1; margin: 0 10px;">
                <div style="background: #3b82f6; height: 8px; width: {bar_width}%; border-radius: 4px;"></div>
            </div>
            <div style="width: 60px; text-align: right; color: #22c55e;">{obs}</div>
            <div style="width: 60px; text-align: right; color: #f59e0b;">{pred}</div>
        </div>
        """
    
    return f"""
    <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 20px; border-radius: 16px; border: 1px solid #475569;">
        <div style="text-align: center; margin-bottom: 15px;">
            <div style="font-size: 18px; color: #f1f5f9; font-weight: 600;">🗄️ Cassandra Database Stats</div>
            <div style="color: #22c55e; font-size: 14px; margin-top: 5px;">🟢 Connected</div>
        </div>
        
        <div style="display: flex; justify-content: center; gap: 30px; margin: 20px 0; padding: 15px; background: rgba(0,0,0,0.2); border-radius: 12px;">
            <div style="text-align: center;">
                <div style="font-size: 28px; color: #3b82f6; font-weight: bold;">{total_obs}</div>
                <div style="color: #94a3b8; font-size: 12px;">Observations</div>
            </div>
            <div style="text-align: center;">
                <div style="font-size: 28px; color: #f59e0b; font-weight: bold;">{total_pred}</div>
                <div style="color: #94a3b8; font-size: 12px;">Predictions</div>
            </div>
        </div>
        
        <div style="font-size: 12px; color: #64748b; margin-bottom: 10px;">
            <div style="display: flex; justify-content: flex-end; gap: 10px;">
                <span style="color: #22c55e;">📊 Obs</span>
                <span style="color: #f59e0b;">🔮 Pred</span>
            </div>
        </div>
        
        {district_rows}
    </div>
    """


def create_airflow_status_html() -> str:
    """Create HTML for Airflow pipeline status."""
    status = airflow_monitor.get_pipeline_status()
    
    if not status['airflow_connected']:
        return """
        <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 20px; border-radius: 16px; border: 1px solid #475569;">
            <div style="text-align: center; color: #ef4444;">
                <div style="font-size: 36px;">🔴</div>
                <div style="font-size: 16px; margin-top: 10px;">Airflow Not Connected</div>
                <div style="font-size: 12px; color: #64748b; margin-top: 5px;">
                    Check if Airflow is running at http://localhost:8090
                </div>
            </div>
        </div>
        """
    
    def get_status_icon(state: str) -> str:
        state_lower = (state or '').lower()
        if state_lower == 'success':
            return '✅'
        elif state_lower == 'running':
            return '🔄'
        elif state_lower == 'failed':
            return '❌'
        elif state_lower == 'queued':
            return '⏳'
        return '⚪'
    
    def get_status_color(state: str) -> str:
        state_lower = (state or '').lower()
        if state_lower == 'success':
            return '#22c55e'
        elif state_lower == 'running':
            return '#3b82f6'
        elif state_lower == 'failed':
            return '#ef4444'
        elif state_lower == 'queued':
            return '#f59e0b'
        return '#64748b'
    
    # Training status
    training = status.get('training', {})
    training_status = training.get('status', 'unknown')
    training_icon = get_status_icon(training_status)
    training_color = get_status_color(training_status)
    training_last = training.get('last_run', 'N/A')
    if training_last and training_last != 'N/A':
        training_last = training_last[:19].replace('T', ' ')
    
    # Inference status
    inference = status.get('inference', {})
    inference_status = inference.get('status', 'unknown')
    inference_icon = get_status_icon(inference_status)
    inference_color = get_status_color(inference_status)
    inference_last = inference.get('last_run', 'N/A')
    if inference_last and inference_last != 'N/A':
        inference_last = inference_last[:19].replace('T', ' ')
    
    # Data pipeline status
    data_pipeline = status.get('data_pipeline', {})
    data_status = data_pipeline.get('status', 'unknown')
    data_icon = get_status_icon(data_status)
    data_color = get_status_color(data_status)
    data_last = data_pipeline.get('last_run', 'N/A')
    if data_last and data_last != 'N/A':
        data_last = data_last[:19].replace('T', ' ')
    
    # Training tasks
    training_tasks_html = ""
    for task in training.get('tasks', [])[:6]:
        task_icon = get_status_icon(task.get('state'))
        task_color = get_status_color(task.get('state'))
        training_tasks_html += f"""
        <div style="display: flex; align-items: center; gap: 8px; padding: 4px 8px; background: rgba(0,0,0,0.2); border-radius: 6px; margin: 4px 0;">
            <span>{task_icon}</span>
            <span style="color: #94a3b8; font-size: 11px; flex: 1;">{task.get('task_id', 'N/A')}</span>
            <span style="color: {task_color}; font-size: 10px;">{task.get('state', 'N/A')}</span>
        </div>
        """
    
    # Inference tasks
    inference_tasks_html = ""
    for task in inference.get('tasks', [])[:3]:
        task_icon = get_status_icon(task.get('state'))
        task_color = get_status_color(task.get('state'))
        inference_tasks_html += f"""
        <div style="display: flex; align-items: center; gap: 8px; padding: 4px 8px; background: rgba(0,0,0,0.2); border-radius: 6px; margin: 4px 0;">
            <span>{task_icon}</span>
            <span style="color: #94a3b8; font-size: 11px; flex: 1;">{task.get('task_id', 'N/A')}</span>
            <span style="color: {task_color}; font-size: 10px;">{task.get('state', 'N/A')}</span>
        </div>
        """
    
    return f"""
    <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 20px; border-radius: 16px; border: 1px solid #475569;">
        <div style="text-align: center; margin-bottom: 15px;">
            <div style="font-size: 18px; color: #f1f5f9; font-weight: 600;">⚙️ Airflow Pipeline Status</div>
            <div style="color: #22c55e; font-size: 14px; margin-top: 5px;">🟢 Connected</div>
        </div>
        
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-bottom: 15px;">
            <!-- Training Status -->
            <div style="background: rgba(0,0,0,0.3); padding: 15px; border-radius: 12px; border-left: 3px solid {training_color};">
                <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px;">
                    <span style="font-size: 20px;">🎯</span>
                    <span style="color: #f1f5f9; font-weight: 600;">Training</span>
                </div>
                <div style="display: flex; align-items: center; gap: 5px;">
                    <span>{training_icon}</span>
                    <span style="color: {training_color}; font-weight: 500;">{training_status.upper()}</span>
                </div>
                <div style="color: #64748b; font-size: 11px; margin-top: 5px;">Last: {training_last}</div>
            </div>
            
            <!-- Inference Status -->
            <div style="background: rgba(0,0,0,0.3); padding: 15px; border-radius: 12px; border-left: 3px solid {inference_color};">
                <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px;">
                    <span style="font-size: 20px;">🔮</span>
                    <span style="color: #f1f5f9; font-weight: 600;">Inference</span>
                </div>
                <div style="display: flex; align-items: center; gap: 5px;">
                    <span>{inference_icon}</span>
                    <span style="color: {inference_color}; font-weight: 500;">{inference_status.upper()}</span>
                </div>
                <div style="color: #64748b; font-size: 11px; margin-top: 5px;">Last: {inference_last}</div>
            </div>
            
            <!-- Data Pipeline Status -->
            <div style="background: rgba(0,0,0,0.3); padding: 15px; border-radius: 12px; border-left: 3px solid {data_color};">
                <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px;">
                    <span style="font-size: 20px;">📊</span>
                    <span style="color: #f1f5f9; font-weight: 600;">Data Pipeline</span>
                </div>
                <div style="display: flex; align-items: center; gap: 5px;">
                    <span>{data_icon}</span>
                    <span style="color: {data_color}; font-weight: 500;">{data_status.upper()}</span>
                </div>
                <div style="color: #64748b; font-size: 11px; margin-top: 5px;">Last: {data_last}</div>
            </div>
        </div>
        
        <!-- Task Details -->
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
            <div>
                <div style="color: #94a3b8; font-size: 12px; margin-bottom: 5px;">🎯 Training Tasks</div>
                {training_tasks_html if training_tasks_html else '<div style="color: #64748b; font-size: 11px;">No recent tasks</div>'}
            </div>
            <div>
                <div style="color: #94a3b8; font-size: 12px; margin-bottom: 5px;">🔮 Inference Tasks</div>
                {inference_tasks_html if inference_tasks_html else '<div style="color: #64748b; font-size: 11px;">No recent tasks</div>'}
            </div>
        </div>
        
        <div style="text-align: center; margin-top: 15px;">
            <a href="http://localhost:8090" target="_blank" style="color: #3b82f6; font-size: 12px; text-decoration: none;">
                🔗 Open Airflow UI →
            </a>
        </div>
    </div>
    """


def refresh_airflow_status() -> str:
    """Refresh Airflow status display."""
    return create_airflow_status_html()


def trigger_training_dag() -> str:
    """Trigger the training DAG."""
    result = airflow_monitor.trigger_dag('flood_heatwave_training')
    if result.get('success'):
        return "✅ Training DAG triggered successfully!"
    else:
        return f"❌ Failed to trigger: {result.get('error', 'Unknown error')}"


def trigger_inference_dag() -> str:
    """Trigger the inference DAG."""
    result = airflow_monitor.trigger_dag('flood_heatwave_inference')
    if result.get('success'):
        return "✅ Inference DAG triggered successfully!"
    else:
        return f"❌ Failed to trigger: {result.get('error', 'Unknown error')}"


def fetch_once(district: str):
    """Fetch weather once for selected district."""
    if not district:
        return (
            create_weather_card(None),
            create_prediction_card(None),
            create_status_html(False, None, streaming_service.get_stats()),
            create_history_table([]),
            f"⚠️ Please select a district"
        )
    
    data = streaming_service.fetch_weather(district)
    stats = streaming_service.get_stats()
    history = streaming_service.get_history()
    
    if 'error' in data:
        return (
            create_weather_card(data),
            create_prediction_card(None),
            create_status_html(False, district, stats),
            create_history_table(history),
            f"❌ Error: {data['error']}"
        )
    
    return (
        create_weather_card(data),
        create_prediction_card(data),
        create_status_html(streaming_service.streaming, district, stats),
        create_history_table(history),
        f"✅ Fetched weather for {district} - Temp: {data['MaxTemp_2m']:.1f}°C, Humidity: {data['RH_2m']:.0f}%"
    )


def start_streaming(district: str):
    """Start auto-streaming for district."""
    if not district:
        return f"⚠️ Please select a district first"
    
    # Fetch once immediately
    streaming_service.fetch_weather(district)
    
    # Start streaming
    streaming_service.start_streaming(district, interval=STREAM_INTERVAL)
    
    return f"🚀 Started auto-streaming for {district} every {STREAM_INTERVAL} seconds"


def stop_streaming():
    """Stop auto-streaming."""
    streaming_service.stop_streaming()
    return "⏹️ Streaming stopped"


def refresh_dashboard():
    """Refresh dashboard with latest data."""
    data = streaming_service.latest_data
    stats = streaming_service.get_stats()
    history = streaming_service.get_history()
    district = streaming_service.current_district
    
    return (
        create_weather_card(data),
        create_prediction_card(data),
        create_status_html(streaming_service.streaming, district, stats),
        create_history_table(history)
    )


# ============================================================================
# CUSTOM CSS
# ============================================================================

CUSTOM_CSS = """
.gradio-container {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%) !important;
    min-height: 100vh;
}
.main-title {
    text-align: center;
    font-size: 2.5em;
    font-weight: 700;
    background: linear-gradient(135deg, #3b82f6, #22c55e, #f59e0b);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    padding: 20px;
    margin-bottom: 10px;
}
.subtitle {
    text-align: center;
    color: #64748b;
    font-size: 1.1em;
    margin-bottom: 25px;
}
.section-header {
    color: #94a3b8;
    font-size: 1.2em;
    font-weight: 600;
    border-left: 4px solid #3b82f6;
    padding-left: 12px;
    margin: 20px 0 15px 0;
}
"""


# ============================================================================
# MAIN DASHBOARD
# ============================================================================

def create_dashboard():
    """Create the Gradio dashboard."""
    
    with gr.Blocks(
        title="🌡️ Weather Prediction Dashboard",
        theme=gr.themes.Soft(
            primary_hue="blue",
            secondary_hue="green",
            neutral_hue="slate"
        ),
        css=CUSTOM_CSS
    ) as app:
        
        # Header
        gr.HTML("""
        <div class="main-title">
            🌡️ Real-Time Weather & Disaster Prediction 🌊
        </div>
        <div class="subtitle">
            OpenWeatherMap → Kafka Streaming → ML Prediction → Cassandra Storage → Live Dashboard
        </div>
        """)
        
        # Main tabs
        with gr.Tabs():
            # =====================================================================
            # TAB 1: LIVE STREAMING
            # =====================================================================
            with gr.TabItem("📡 Live Streaming", id="live"):
                # Status bar
                status_html = gr.HTML(value=create_status_html(False, None, {}))
                
                # District Selection & Controls
                with gr.Row():
                    with gr.Column(scale=2):
                        district_dropdown = gr.Dropdown(
                            choices=DISTRICTS,
                            label="📍 Select District",
                            value=None,
                            info="Choose a district to monitor",
                            interactive=True
                        )
                    with gr.Column(scale=1):
                        fetch_btn = gr.Button("🔍 Fetch Once", variant="secondary", size="lg")
                    with gr.Column(scale=1):
                        start_btn = gr.Button("▶️ Start Streaming", variant="primary", size="lg")
                    with gr.Column(scale=1):
                        stop_btn = gr.Button("⏹️ Stop", variant="stop", size="lg")
                
                # Status message
                status_msg = gr.Textbox(
                    label="Status", 
                    value="👆 Select a district and click 'Fetch Once' or 'Start Streaming'",
                    interactive=False
                )
                
                # Main content
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.HTML('<div class="section-header">🌤️ Current Weather</div>')
                        weather_card = gr.HTML(value=create_weather_card(None))
                    
                    with gr.Column(scale=1):
                        gr.HTML('<div class="section-header">🔮 Risk Predictions</div>')
                        prediction_card = gr.HTML(value=create_prediction_card(None))
                
                # History table
                gr.HTML('<div class="section-header">📊 Recent Data History (Session)</div>')
                history_table = gr.Dataframe(
                    value=create_history_table([]),
                    interactive=False,
                    wrap=True
                )
                
                # Auto-refresh timer (every 5 seconds to update UI)
                timer = gr.Timer(value=5)
            
            # =====================================================================
            # TAB 2: CASSANDRA HISTORICAL DATA
            # =====================================================================
            with gr.TabItem("🗄️ Historical Data (Cassandra)", id="cassandra"):
                gr.HTML("""
                <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 15px; border-radius: 12px; margin-bottom: 20px;">
                    <div style="color: #f1f5f9; font-size: 16px;">
                        📊 <b>Query Historical Weather Data</b> - Browse weather observations and predictions stored in Cassandra
                    </div>
                    <div style="color: #94a3b8; font-size: 13px; margin-top: 5px;">
                        💡 Data is continuously collected by the background streamer from all districts
                    </div>
                </div>
                """)
                
                with gr.Row():
                    # Query controls column
                    with gr.Column(scale=1):
                        gr.HTML('<div class="section-header">🔍 Query Controls</div>')
                        
                        query_district = gr.Dropdown(
                            choices=["All Districts"] + DISTRICTS,
                            label="📍 Select District",
                            value="All Districts",
                            interactive=True
                        )
                        
                        query_type = gr.Radio(
                            choices=["Observations", "Predictions"],
                            label="📋 Data Type",
                            value="Observations",
                            interactive=True
                        )
                        
                        query_limit = gr.Slider(
                            minimum=10,
                            maximum=200,
                            value=50,
                            step=10,
                            label="📊 Number of Records",
                            interactive=True
                        )
                        
                        with gr.Row():
                            query_btn = gr.Button("🔍 Query Data", variant="primary", size="lg")
                            refresh_stats_btn = gr.Button("🔄 Refresh Stats", variant="secondary", size="lg")
                        
                        query_status = gr.Textbox(
                            label="Query Status",
                            value="👆 Select options and click 'Query Data'",
                            interactive=False
                        )
                        
                        # Cassandra stats panel
                        cassandra_stats_html = gr.HTML(
                            value=create_cassandra_stats_html(cassandra_storage.get_all_stats() if cassandra_storage.connected else {})
                        )
                    
                    # Results column
                    with gr.Column(scale=2):
                        gr.HTML('<div class="section-header">📊 Query Results</div>')
                        
                        query_results = gr.Dataframe(
                            value=pd.DataFrame(),
                            label="Results",
                            interactive=False,
                            wrap=True
                        )
            
            # =====================================================================
            # TAB 3: AIRFLOW PIPELINE STATUS
            # =====================================================================
            with gr.TabItem("⚙️ Pipeline Status", id="pipeline"):
                gr.HTML("""
                <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 15px; border-radius: 12px; margin-bottom: 20px;">
                    <div style="color: #f1f5f9; font-size: 16px;">
                        ⚙️ <b>Airflow Pipeline Monitoring</b> - View training and inference status
                    </div>
                    <div style="color: #94a3b8; font-size: 13px; margin-top: 5px;">
                        💡 Monitor DAG runs, training progress, and inference results in real-time
                    </div>
                </div>
                """)
                
                with gr.Row():
                    with gr.Column(scale=2):
                        # Airflow Status Panel
                        airflow_status_html = gr.HTML(value=create_airflow_status_html())
                    
                    with gr.Column(scale=1):
                        gr.HTML('<div class="section-header">🎮 Pipeline Controls</div>')
                        
                        # Control buttons
                        with gr.Column():
                            refresh_airflow_btn = gr.Button("🔄 Refresh Status", variant="secondary", size="lg")
                            trigger_training_btn = gr.Button("🎯 Trigger Training", variant="primary", size="lg")
                            trigger_inference_btn = gr.Button("🔮 Trigger Inference", variant="primary", size="lg")
                        
                        pipeline_status_msg = gr.Textbox(
                            label="Pipeline Status",
                            value="Click refresh to get latest status",
                            interactive=False
                        )
                        
                        gr.HTML("""
                        <div style="background: rgba(0,0,0,0.2); padding: 15px; border-radius: 12px; margin-top: 15px;">
                            <div style="color: #94a3b8; font-size: 13px; font-weight: 600; margin-bottom: 10px;">📋 DAG Schedules</div>
                            <div style="color: #64748b; font-size: 12px; line-height: 1.8;">
                                <div>🎯 <b>Training:</b> Every 1 minute</div>
                                <div>🔮 <b>Inference:</b> Every 1 minute (2s loop)</div>
                                <div>📊 <b>Data Pipeline:</b> Every 10 minutes</div>
                            </div>
                        </div>
                        """)
                
                # Pipeline Timer (refresh every 10 seconds)
                pipeline_timer = gr.Timer(value=10)
            
            # =====================================================================
            # TAB 4: ML INFERENCE RESULTS
            # =====================================================================
            with gr.TabItem("🤖 ML Inference Results", id="inference"):
                gr.HTML("""
                <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 15px; border-radius: 12px; margin-bottom: 20px;">
                    <div style="color: #f1f5f9; font-size: 16px;">
                        🤖 <b>ML Model Inference Results</b> - Real-time predictions from Airflow
                    </div>
                    <div style="color: #94a3b8; font-size: 13px; margin-top: 5px;">
                        💡 View heatwave and flood risk predictions from XGBoost and LSTM models
                    </div>
                </div>
                """)
                
                with gr.Row():
                    with gr.Column(scale=3):
                        # Main inference results panel
                        inference_results_html = gr.HTML(value=create_inference_results_html())
                    
                    with gr.Column(scale=1):
                        gr.HTML('<div class="section-header">🎮 Inference Controls</div>')
                        
                        # District filter
                        inference_district_dropdown = gr.Dropdown(
                            choices=["All Districts"] + DISTRICTS,
                            value="All Districts",
                            label="Filter by District"
                        )
                        
                        # Control buttons
                        refresh_inference_btn = gr.Button("🔄 Refresh Results", variant="secondary", size="lg")
                        trigger_inference_btn2 = gr.Button("🔮 Run New Inference", variant="primary", size="lg")
                        
                        inference_status_msg = gr.Textbox(
                            label="Status",
                            value="Inference results loaded from JSON",
                            interactive=False
                        )
                        
                        gr.HTML(f"""
                        <div style="background: rgba(0,0,0,0.2); padding: 15px; border-radius: 12px; margin-top: 15px;">
                            <div style="color: #94a3b8; font-size: 13px; font-weight: 600; margin-bottom: 10px;">📋 Data Source</div>
                            <div style="color: #64748b; font-size: 11px; line-height: 1.8; word-break: break-all;">
                                <div>📁 JSON File:</div>
                                <div style="color: #3b82f6; margin-top: 5px;">{INFERENCE_RESULTS_JSON}</div>
                                <div style="margin-top: 10px;">🔄 Updates: Every inference run</div>
                            </div>
                        </div>
                        """)
                
                # Inference Timer (refresh every 5 seconds)
                inference_timer = gr.Timer(value=5)
            
            # =====================================================================
            # TAB 5: INFO
            # =====================================================================
            with gr.TabItem("ℹ️ About", id="about"):
                gr.Markdown(f"""
                ## 🌡️ Weather Prediction Dashboard
                
                This dashboard monitors weather conditions for districts in Nepal's Terai region and predicts disaster risks.
                
                ### 📍 Monitored Districts
                - **Bara** - Southern plains district
                - **Dhanusa** - Terai district bordering India  
                - **Sarlahi** - Agricultural district
                - **Parsa** - Contains Parsa Wildlife Reserve
                - **Siraha** - Eastern Terai district
                
                ### 🔄 Data Flow Architecture
                ```
                ┌─────────────────────────────────────────────────────────────┐
                │              BACKGROUND STREAMER (Distributed)              │
                │  Continuously fetches data for all 5 districts every 30s    │
                └────────────────────────────┬────────────────────────────────┘
                                             │
                                             ▼
                        ┌─────────────────────────────────────┐
                        │     OpenWeatherMap API (5 calls)     │
                        └────────────────────────────┬────────┘
                                                     │
                                                     ▼
                ┌─────────────────┐         ┌─────────────────┐
                │   Kafka Topic   │◄────────│  Producer API   │
                │  (weather-data) │         │   (port 8000)   │
                └────────┬────────┘         └─────────────────┘
                         │
                         ▼
                ┌─────────────────┐         ┌─────────────────┐
                │   Cassandra DB  │◄────────│  ML Predictions │
                │  (time-series)  │         │  (XGBoost)      │
                └────────┬────────┘         └─────────────────┘
                         │
                         ▼
                ┌─────────────────────────────────────────────┐
                │            This Dashboard (Gradio)           │
                │  - Live streaming tab (real-time)            │
                │  - Historical data tab (query Cassandra)     │
                └─────────────────────────────────────────────┘
                ```
                
                ### 📡 Live Streaming Tab
                - Select a district and click **"Start Streaming"** to auto-fetch weather every **{STREAM_INTERVAL} seconds**
                - Click **"Fetch Once"** for a single data point
                - View real-time heatwave and flood risk predictions
                
                ### 🗄️ Historical Data Tab
                - Query observations and predictions stored in Cassandra
                - View data from all districts or filter by specific district
                - Data is continuously collected by the background streamer
                
                ### 🚀 Components to Run
                1. **Docker**: `docker-compose up -d` (Kafka, Cassandra, Zookeeper)
                2. **Background Streamer**: `python background_streamer.py` (collects data)
                3. **Producer API**: `python kafka_producer_api.py` (optional, for manual triggers)
                4. **This Dashboard**: `python weather_dashboard.py`
                
                ### 🔮 ML Predictions
                - **Heatwave Risk**: Based on temperature thresholds (>35°C moderate, >40°C high)
                - **Flood Risk**: Based on precipitation and humidity levels
                - Models: XGBoost classifiers trained on historical Nepal weather data
                """)
        
        # =====================================================================
        # EVENT HANDLERS
        # =====================================================================
        
        # Live streaming handlers
        fetch_btn.click(
            fn=fetch_once,
            inputs=[district_dropdown],
            outputs=[weather_card, prediction_card, status_html, history_table, status_msg]
        )
        
        start_btn.click(
            fn=start_streaming,
            inputs=[district_dropdown],
            outputs=[status_msg]
        )
        
        stop_btn.click(
            fn=stop_streaming,
            outputs=[status_msg]
        )
        
        timer.tick(
            fn=refresh_dashboard,
            outputs=[weather_card, prediction_card, status_html, history_table]
        )
        
        district_dropdown.change(
            fn=fetch_once,
            inputs=[district_dropdown],
            outputs=[weather_card, prediction_card, status_html, history_table, status_msg]
        )
        
        # Cassandra query handlers
        def handle_query(district, data_type, limit):
            if district == "All Districts":
                return query_all_districts(data_type, limit)
            else:
                if data_type == "Observations":
                    return query_cassandra_observations(district, limit)
                else:
                    return query_cassandra_predictions(district, limit)
        
        query_btn.click(
            fn=handle_query,
            inputs=[query_district, query_type, query_limit],
            outputs=[query_results, query_status, cassandra_stats_html]
        )
        
        refresh_stats_btn.click(
            fn=refresh_cassandra_stats,
            outputs=[cassandra_stats_html]
        )
        
        # Airflow pipeline handlers
        refresh_airflow_btn.click(
            fn=refresh_airflow_status,
            outputs=[airflow_status_html]
        )
        
        trigger_training_btn.click(
            fn=trigger_training_dag,
            outputs=[pipeline_status_msg]
        )
        
        trigger_inference_btn.click(
            fn=trigger_inference_dag,
            outputs=[pipeline_status_msg]
        )
        
        pipeline_timer.tick(
            fn=refresh_airflow_status,
            outputs=[airflow_status_html]
        )
        
        # Inference results handlers
        def handle_inference_filter(district):
            if district == "All Districts":
                return create_inference_results_html()
            return create_inference_results_html(district)
        
        refresh_inference_btn.click(
            fn=refresh_inference_results,
            outputs=[inference_results_html]
        )
        
        trigger_inference_btn2.click(
            fn=trigger_inference_dag,
            outputs=[inference_status_msg]
        )
        
        inference_district_dropdown.change(
            fn=handle_inference_filter,
            inputs=[inference_district_dropdown],
            outputs=[inference_results_html]
        )
        
        inference_timer.tick(
            fn=refresh_inference_results,
            outputs=[inference_results_html]
        )
    
    return app


# ============================================================================
# MAIN
# ============================================================================

def main():
    cassandra_status = "✅ Connected" if cassandra_storage.connected else "❌ Not connected"
    airflow_status = "✅ Connected" if airflow_monitor.connected else "❌ Not connected"
    inference_json_exists = "✅ Exists" if os.path.exists(INFERENCE_RESULTS_JSON) else "⚠️ Not found"
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   🌡️  REAL-TIME WEATHER & DISASTER PREDICTION DASHBOARD                      ║
║                                                                              ║
║   OpenWeatherMap → Kafka → ML Prediction → Cassandra → Live Display          ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

📍 Monitored Districts: Bara, Dhanusa, Sarlahi, Parsa, Siraha
⏱️  Streaming Interval: {interval} seconds
🔗 Producer API: {api_url}

🗄️  Cassandra Storage:
   Status: {cassandra_status}
   Hosts: {cassandra_hosts}
   Keyspace: {keyspace}
   Tables: weather_observations, weather_predictions, daily_weather_summary

⚙️  Airflow Pipeline:
   Status: {airflow_status}
   URL: {airflow_url}
   DAGs: flood_heatwave_training, flood_heatwave_inference, flood_heatwave_data_pipeline

🤖 ML Inference Results:
   JSON File: {inference_json}
   Status: {inference_status}
   Updates: Every inference run by Airflow

🔗 Dashboard: http://localhost:7860

💡 Prerequisites:
   1. Start Docker: docker-compose up -d
   2. Start Producer API: python kafka_producer_api.py
   3. Open this dashboard and select a district

📊 Data is automatically stored in Cassandra when streaming!
📈 Inference results are stored in JSON and displayed in the ML Inference Results tab!
""".format(
        interval=STREAM_INTERVAL, 
        api_url=PRODUCER_API_URL,
        cassandra_status=cassandra_status,
        cassandra_hosts=CASSANDRA_CONFIG['hosts'],
        keyspace=CASSANDRA_CONFIG['keyspace'],
        airflow_status=airflow_status,
        airflow_url=AIRFLOW_CONFIG['base_url'],
        inference_json=INFERENCE_RESULTS_JSON,
        inference_status=inference_json_exists
    ))
    
    app = create_dashboard()
    app.launch(
        server_name="127.0.0.1",
        server_port=7860,
        share=False
    )


if __name__ == "__main__":
    main()
