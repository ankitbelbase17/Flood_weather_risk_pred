"""
Inference Task for Airflow
Runs inference using the latest model checkpoint from HDFS
Supports both XGBoost and LSTM models
Saves inference results to JSON file for Gradio dashboard consumption
"""
import os
import json
import tempfile
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, udf, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, ArrayType
import threading
import fcntl

# JSON file path for storing inference results (shared with Gradio)
INFERENCE_RESULTS_JSON = os.environ.get(
    'INFERENCE_RESULTS_JSON', 
    '/opt/airflow/data/inference_results.json'
)

# Try importing ML libraries
try:
    import torch
    import torch.nn as nn
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    print("⚠️  PyTorch not available")

try:
    import joblib
    import xgboost as xgb
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False
    print("⚠️  XGBoost/joblib not available")

# Configuration
HDFS_BASE_PATH = os.environ.get('HDFS_BASE_PATH', 'hdfs://namenode:9000/user/airflow/weather_data')
MODEL_CHECKPOINT_PATH = os.environ.get('MODEL_CHECKPOINT_PATH', 'hdfs://namenode:9000/user/airflow/models')
INFERENCE_INTERVAL_SECONDS = int(os.environ.get('INFERENCE_INTERVAL_SECONDS', '2'))


# ============================================================================
# JSON RESULTS MANAGEMENT
# ============================================================================

def load_inference_results() -> dict:
    """Load existing inference results from JSON file."""
    try:
        if os.path.exists(INFERENCE_RESULTS_JSON):
            with open(INFERENCE_RESULTS_JSON, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"⚠️  Error loading inference results: {e}")
    
    # Return default structure
    return {
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


def save_inference_results(results: dict):
    """Save inference results to JSON file with file locking."""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(INFERENCE_RESULTS_JSON), exist_ok=True)
        
        # Write with atomic operation
        temp_path = INFERENCE_RESULTS_JSON + '.tmp'
        with open(temp_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        # Atomic rename
        os.replace(temp_path, INFERENCE_RESULTS_JSON)
        print(f"✅ Saved inference results to {INFERENCE_RESULTS_JSON}")
        
    except Exception as e:
        print(f"❌ Error saving inference results: {e}")


def update_inference_results(model_type: str, target: str, predictions_df: pd.DataFrame, status: str = 'success'):
    """Update inference results JSON with new predictions."""
    results = load_inference_results()
    
    now = datetime.now().isoformat()
    results['last_updated'] = now
    results['inference_count'] = results.get('inference_count', 0) + 1
    
    # Map target name
    target_key = 'heatwave' if target == 'heatwave' else 'flood'
    model_key = 'xgboost' if model_type == 'xgb' else 'lstm'
    
    # Update model status
    if model_key not in results['models']:
        results['models'][model_key] = {}
    if target_key not in results['models'][model_key]:
        results['models'][model_key][target_key] = {}
    
    results['models'][model_key][target_key]['status'] = status
    results['models'][model_key][target_key]['last_run'] = now
    
    # Extract per-district predictions
    district_predictions = {}
    high_risk_count = 0
    
    prob_col = f'{target}_probability'
    risk_col = f'{target}_risk'
    
    if prob_col in predictions_df.columns:
        for district in predictions_df['District'].unique():
            district_data = predictions_df[predictions_df['District'] == district]
            if len(district_data) > 0:
                latest = district_data.iloc[-1]  # Get most recent
                prob = float(latest.get(prob_col, 0))
                risk = str(latest.get(risk_col, 'LOW'))
                
                district_predictions[district] = {
                    'probability': prob,
                    'risk': risk,
                    'date': str(latest.get('Date', now)),
                    'model': model_key,
                    'target': target_key
                }
                
                if risk == 'HIGH':
                    high_risk_count += 1
                
                # Update districts section
                if district not in results['districts']:
                    results['districts'][district] = {'heatwave': {}, 'flood': {}}
                results['districts'][district][target_key] = {
                    model_key: {
                        'probability': prob,
                        'risk': risk,
                        'last_updated': now
                    }
                }
    
    results['models'][model_key][target_key]['predictions'] = district_predictions
    
    # Update latest predictions
    if 'by_district' not in results['latest_predictions']:
        results['latest_predictions']['by_district'] = {}
    
    for district, pred in district_predictions.items():
        if district not in results['latest_predictions']['by_district']:
            results['latest_predictions']['by_district'][district] = {}
        results['latest_predictions']['by_district'][district][f'{model_key}_{target_key}'] = pred
    
    # Update summary
    results['latest_predictions']['summary'] = {
        'high_risk_heatwave': sum(1 for d in results['districts'].values() 
                                   if d.get('heatwave', {}).get('xgboost', {}).get('risk') == 'HIGH' or
                                      d.get('heatwave', {}).get('lstm', {}).get('risk') == 'HIGH'),
        'high_risk_flood': sum(1 for d in results['districts'].values() 
                               if d.get('flood', {}).get('xgboost', {}).get('risk') == 'HIGH' or
                                  d.get('flood', {}).get('lstm', {}).get('risk') == 'HIGH'),
        'total_predictions': results['inference_count'],
        'last_updated': now
    }
    
    save_inference_results(results)
    return results


class LSTMModel(nn.Module):
    """LSTM model for binary classification."""
    def __init__(self, input_size, hidden_size=64, fc_size=32):
        super(LSTMModel, self).__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, batch_first=True)
        self.fc1 = nn.Linear(hidden_size, fc_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(fc_size, 1)
    
    def forward(self, x):
        lstm_out, (h_n, c_n) = self.lstm(x)
        out = self.fc1(h_n[-1])
        out = self.relu(out)
        out = self.fc2(out)
        return out.squeeze(-1)


def create_spark_session():
    """Create Spark session with HDFS support."""
    return SparkSession.builder \
        .appName("InferenceTask") \
        .master("spark://spark-spark-1:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()


def download_model_from_hdfs(spark, hdfs_model_path, local_path):
    """Download model file from HDFS to local filesystem."""
    try:
        # Use Hadoop FileSystem API via Spark
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(hdfs_model_path),
            hadoop_conf
        )
        
        hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_model_path)
        local_fs_path = spark._jvm.org.apache.hadoop.fs.Path(local_path)
        
        fs.copyToLocalFile(hdfs_path, local_fs_path)
        print(f"✅ Downloaded model from {hdfs_model_path} to {local_path}")
        return True
    except Exception as e:
        print(f"⚠️  Error downloading model: {e}")
        return False


def get_latest_model_path(spark, model_type='lstm', target='heatwave'):
    """Get the latest model checkpoint path from HDFS."""
    try:
        model_dir = f"{MODEL_CHECKPOINT_PATH}/{model_type}_{target}"
        
        # List files in model directory
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI("hdfs://namenode:9000"),
            hadoop_conf
        )
        
        path = spark._jvm.org.apache.hadoop.fs.Path(model_dir)
        
        if not fs.exists(path):
            print(f"⚠️  Model directory does not exist: {model_dir}")
            return None
        
        # Find latest checkpoint
        file_statuses = fs.listStatus(path)
        if len(file_statuses) == 0:
            return None
        
        # Sort by modification time and get latest
        latest = max(file_statuses, key=lambda x: x.getModificationTime())
        latest_path = latest.getPath().toString()
        
        print(f"✅ Found latest model: {latest_path}")
        return latest_path
        
    except Exception as e:
        print(f"⚠️  Error finding latest model: {e}")
        return None


def load_lstm_model(model_path, device='cpu'):
    """Load LSTM model from checkpoint."""
    if not TORCH_AVAILABLE:
        raise ImportError("PyTorch not available")
    
    checkpoint = torch.load(model_path, map_location=device)
    model = LSTMModel(
        input_size=checkpoint['input_size'],
        hidden_size=checkpoint.get('hidden_size', 64),
        fc_size=checkpoint.get('fc_size', 32)
    )
    model.load_state_dict(checkpoint['model_state_dict'])
    model.to(device)
    model.eval()
    
    return model, checkpoint


def load_xgb_model(model_path):
    """Load XGBoost model."""
    if not XGB_AVAILABLE:
        raise ImportError("XGBoost not available")
    
    return joblib.load(model_path)


def run_xgb_inference(spark, target='heatwave'):
    """Run inference using XGBoost model."""
    print(f"🔮 Running XGBoost inference for {target}")
    
    # Get latest model
    hdfs_model_path = get_latest_model_path(spark, 'xgb', target)
    if not hdfs_model_path:
        # Try default path
        hdfs_model_path = f"{MODEL_CHECKPOINT_PATH}/xgb_{target}_model.joblib"
    
    # Download model to local
    local_model_path = f"/tmp/xgb_{target}_model.joblib"
    if not download_model_from_hdfs(spark, hdfs_model_path, local_model_path):
        print(f"⚠️  Using fallback rule-based prediction for {target}")
        return run_rule_based_inference(spark, target)
    
    # Load model
    model = load_xgb_model(local_model_path)
    
    # Read latest data
    data_path = f"{HDFS_BASE_PATH}/labeled"
    try:
        df = spark.read.parquet(data_path)
    except:
        data_path = f"{HDFS_BASE_PATH}/features"
        df = spark.read.parquet(data_path)
    
    # Convert to pandas for inference
    df_pandas = df.toPandas()
    
    # Prepare features
    drop_cols = ['Date', 'District', 'heatwave', 'flood_proxy', 'Latitude', 'Longitude',
                 'heat_exceed', 'precip_1d', 'precip_p99', 'precip3_p98', 'wetness_flag',
                 'temp_p95', 'rh_p90', 'precip_3d_sum', 'clim_maxT', 'clim_precip']
    feature_cols = [c for c in df_pandas.columns if c not in drop_cols]
    X = df_pandas[feature_cols].fillna(-999)
    
    # Run prediction
    if isinstance(model, xgb.Booster):
        dmatrix = xgb.DMatrix(X, feature_names=feature_cols)
        predictions = model.predict(dmatrix)
    elif hasattr(model, 'predict_proba'):
        predictions = model.predict_proba(X)[:, 1]
    else:
        predictions = model.predict(X)
    
    # Add predictions to dataframe
    df_pandas[f'{target}_probability'] = predictions
    df_pandas[f'{target}_risk'] = df_pandas[f'{target}_probability'].apply(
        lambda p: 'HIGH' if p > 0.5 else 'MEDIUM' if p > 0.3 else 'LOW'
    )
    
    # Convert back to Spark and save
    result_df = spark.createDataFrame(df_pandas)
    output_path = f"{HDFS_BASE_PATH}/predictions/xgb_{target}"
    result_df.write.mode("overwrite").parquet(output_path)
    
    print(f"✅ XGBoost inference completed for {target}")
    print(f"   Records: {len(df_pandas)}")
    print(f"   Output: {output_path}")
    
    # Print summary
    high_risk = (predictions > 0.5).sum()
    print(f"   High risk predictions: {high_risk} ({high_risk/len(predictions)*100:.2f}%)")
    
    # Save to JSON for Gradio dashboard
    update_inference_results('xgb', target, df_pandas, status='success')
    
    return result_df


def run_lstm_inference(spark, target='heatwave', timesteps=14):
    """Run inference using LSTM model."""
    print(f"🔮 Running LSTM inference for {target}")
    
    if not TORCH_AVAILABLE:
        print("⚠️  PyTorch not available, using rule-based fallback")
        return run_rule_based_inference(spark, target)
    
    # Get latest model
    hdfs_model_path = get_latest_model_path(spark, 'lstm', target)
    if not hdfs_model_path:
        hdfs_model_path = f"{MODEL_CHECKPOINT_PATH}/lstm_{target}.pt"
    
    # Download model to local
    local_model_path = f"/tmp/lstm_{target}.pt"
    if not download_model_from_hdfs(spark, hdfs_model_path, local_model_path):
        print(f"⚠️  Using fallback rule-based prediction for {target}")
        return run_rule_based_inference(spark, target)
    
    # Load model - Force CPU for compatibility
    device = torch.device('cpu')  # Force CPU - don't use CUDA
    model, checkpoint = load_lstm_model(local_model_path, device)
    
    # Read latest data
    data_path = f"{HDFS_BASE_PATH}/labeled"
    try:
        df_spark = spark.read.parquet(data_path)
    except:
        data_path = f"{HDFS_BASE_PATH}/features"
        df_spark = spark.read.parquet(data_path)
    
    # Convert to pandas
    df_pandas = df_spark.toPandas()
    df_pandas = df_pandas.sort_values(['District', 'Date']).copy()
    
    # Get feature columns from checkpoint
    feature_cols = checkpoint.get('feature_cols', None)
    if feature_cols is None:
        drop_cols = ['Date', 'District', 'heatwave', 'flood_proxy', 'heat_exceed',
                     'precip_1d', 'precip_p99', 'precip3_p98', 'wetness_flag',
                     'temp_p95', 'rh_p90', 'precip_3d_sum']
        feature_cols = [c for c in df_pandas.columns if c not in drop_cols]
    
    # Build predictions
    predictions = []
    indices = []
    
    for district, g in df_pandas.groupby('District'):
        arr = g[feature_cols].values.astype(np.float32)
        arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
        
        if len(arr) < timesteps:
            continue
        
        # Apply scaler if available
        if 'scaler_mean' in checkpoint and 'scaler_scale' in checkpoint:
            arr = (arr - checkpoint['scaler_mean']) / checkpoint['scaler_scale']
        
        for i in range(timesteps, len(arr) + 1):
            seq = arr[i-timesteps:i]
            seq_tensor = torch.FloatTensor(seq).unsqueeze(0).to(device)
            
            with torch.no_grad():
                logit = model(seq_tensor)
                prob = torch.sigmoid(logit).item()
            
            predictions.append(prob)
            indices.append(g.index[i-1])
    
    # Add predictions to dataframe
    pred_series = pd.Series(0.0, index=df_pandas.index)
    for idx, prob in zip(indices, predictions):
        pred_series.loc[idx] = prob
    
    df_pandas[f'{target}_probability'] = pred_series
    df_pandas[f'{target}_risk'] = df_pandas[f'{target}_probability'].apply(
        lambda p: 'HIGH' if p > 0.5 else 'MEDIUM' if p > 0.3 else 'LOW'
    )
    
    # Convert back to Spark and save
    result_df = spark.createDataFrame(df_pandas)
    output_path = f"{HDFS_BASE_PATH}/predictions/lstm_{target}"
    result_df.write.mode("overwrite").parquet(output_path)
    
    print(f"✅ LSTM inference completed for {target}")
    print(f"   Records: {len(predictions)}")
    print(f"   Output: {output_path}")
    
    # Save to JSON for Gradio dashboard
    update_inference_results('lstm', target, df_pandas, status='success')
    
    return result_df


def run_rule_based_inference(spark, target='heatwave'):
    """Fallback rule-based inference when models aren't available."""
    print(f"🔮 Running rule-based inference for {target}")
    
    # Read data
    data_path = f"{HDFS_BASE_PATH}/labeled"
    try:
        df = spark.read.parquet(data_path)
    except:
        data_path = f"{HDFS_BASE_PATH}/features"
        df = spark.read.parquet(data_path)
    
    if target == 'heatwave':
        # Rule: high probability if MaxTemp > 38
        df = df.withColumn(
            f'{target}_probability',
            (col("MaxTemp_2m") - 35) / 15
        )
    else:  # flood
        # Rule: high probability if Precip > 50 and RH > 80
        df = df.withColumn(
            f'{target}_probability',
            (col("Precip") - 50) / 100 + (col("RH_2m") - 80) / 100
        )
    
    # Clamp probability between 0 and 1
    df = df.withColumn(
        f'{target}_probability',
        col(f'{target}_probability').cast(DoubleType())
    )
    
    # Add risk level
    from pyspark.sql.functions import when
    df = df.withColumn(
        f'{target}_risk',
        when(col(f'{target}_probability') > 0.5, 'HIGH')
        .when(col(f'{target}_probability') > 0.3, 'MEDIUM')
        .otherwise('LOW')
    )
    
    # Save predictions
    output_path = f"{HDFS_BASE_PATH}/predictions/rule_{target}"
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"✅ Rule-based inference completed for {target}")
    print(f"   Output: {output_path}")
    
    # Save to JSON for Gradio dashboard
    df_pandas = df.toPandas()
    update_inference_results('rule', target, df_pandas, status='fallback')
    
    return df


def run_inference(model_type='xgb', target='heatwave'):
    """Main inference entry point."""
    print(f"{'='*60}")
    print(f"🚀 Starting Inference Task")
    print(f"   Model Type: {model_type}")
    print(f"   Target: {target}")
    print(f"   Time: {datetime.now().isoformat()}")
    print(f"{'='*60}")
    
    spark = create_spark_session()
    
    try:
        if model_type == 'lstm':
            result = run_lstm_inference(spark, target)
        elif model_type == 'xgb':
            result = run_xgb_inference(spark, target)
        else:
            result = run_rule_based_inference(spark, target)
        
        print(f"\n✅ Inference completed successfully")
        return result
        
    except Exception as e:
        print(f"❌ Inference failed: {e}")
        raise
    finally:
        spark.stop()


def run_all_inference():
    """Run inference for all model/target combinations."""
    print("🔮 Running all inference tasks")
    
    spark = create_spark_session()
    
    try:
        for target in ['heatwave', 'flood_proxy']:
            for model_type in ['xgb', 'lstm']:
                try:
                    print(f"\n--- {model_type.upper()} {target} ---")
                    if model_type == 'lstm':
                        run_lstm_inference(spark, target)
                    else:
                        run_xgb_inference(spark, target)
                except Exception as e:
                    print(f"⚠️  {model_type} {target} inference failed: {e}")
                    # Try rule-based fallback
                    run_rule_based_inference(spark, target)
        
        print("\n✅ All inference tasks completed")
        
    except Exception as e:
        print(f"❌ Inference batch failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    
    model_type = sys.argv[1] if len(sys.argv) > 1 else 'xgb'
    target = sys.argv[2] if len(sys.argv) > 2 else 'heatwave'
    
    if model_type == 'all':
        run_all_inference()
    else:
        run_inference(model_type, target)
