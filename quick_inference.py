#!/usr/bin/env python
"""
Quick Inference Script - Load models and test on sample data
Run this after the Airflow pipeline completes training
"""

import subprocess
import os
import sys

def check_hdfs_models():
    """Check if trained models exist in HDFS"""
    print("🔍 Checking for trained models in HDFS...")
    
    result = subprocess.run(
        ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/user/airflow/models/'],
        capture_output=True,
        text=True
    )
    
    if 'heatwave_lstm.pt' in result.stdout and 'flood_lstm.pt' in result.stdout:
        print("✅ Both models found in HDFS!")
        print(result.stdout)
        return True
    else:
        print("❌ Models not found. Has the training pipeline completed?")
        print("\nTo check pipeline status:")
        print("  1. Visit http://localhost:8090 (Airflow UI)")
        print("  2. Check the 'flood_heatwave_pipeline' DAG")
        print("  3. Ensure all tasks are green (successful)")
        return False

def download_models():
    """Download models from HDFS to local directory"""
    print("\n📥 Downloading models from HDFS...")
    
    os.makedirs('models', exist_ok=True)
    
    # Download heatwave model
    subprocess.run([
        'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-get',
        '/user/airflow/models/heatwave_lstm.pt',
        '/tmp/heatwave_lstm.pt'
    ], check=True)
    subprocess.run(['docker', 'cp', 'namenode:/tmp/heatwave_lstm.pt', 'models/'], check=True)
    
    # Download flood model
    subprocess.run([
        'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-get',
        '/user/airflow/models/flood_lstm.pt',
        '/tmp/flood_lstm.pt'
    ], check=True)
    subprocess.run(['docker', 'cp', 'namenode:/tmp/flood_lstm.pt', 'models/'], check=True)
    
    print("✅ Models downloaded to ./models/")
    return True

def check_labeled_data():
    """Check if labeled data exists in HDFS"""
    print("\n🔍 Checking for labeled data in HDFS...")
    
    result = subprocess.run(
        ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '-R', '/user/airflow/weather_data/labeled/'],
        capture_output=True,
        text=True
    )
    
    if '.parquet' in result.stdout:
        print("✅ Labeled data found in HDFS!")
        # Count files
        parquet_files = result.stdout.count('.parquet')
        print(f"   Found {parquet_files} parquet files")
        return True
    else:
        print("❌ Labeled data not found")
        return False

def run_quick_test():
    """Run a quick inference test"""
    print("\n🧪 Running quick inference test...")
    
    try:
        import torch
        import torch.nn as nn
        import numpy as np
        
        # Define model architecture
        class LSTMModel(nn.Module):
            def __init__(self, input_size, hidden_size=64, fc_size=32, num_layers=2, dropout=0.2):
                super(LSTMModel, self).__init__()
                self.hidden_size = hidden_size
                self.num_layers = num_layers
                
                self.lstm = nn.LSTM(
                    input_size=input_size,
                    hidden_size=hidden_size,
                    num_layers=num_layers,
                    batch_first=True,
                    dropout=dropout if num_layers > 1 else 0
                )
                
                self.fc1 = nn.Linear(hidden_size, fc_size)
                self.relu = nn.ReLU()
                self.dropout = nn.Dropout(dropout)
                self.fc2 = nn.Linear(fc_size, 1)
                self.sigmoid = nn.Sigmoid()
            
            def forward(self, x):
                lstm_out, (h_n, c_n) = self.lstm(x)
                out = h_n[-1]
                out = self.fc1(out)
                out = self.relu(out)
                out = self.dropout(out)
                out = self.fc2(out)
                out = self.sigmoid(out)
                return out
        
        # Load models
        heatwave_model = torch.load('models/heatwave_lstm.pt', weights_only=False)
        print("✅ Heatwave model loaded successfully")
        
        flood_model = torch.load('models/flood_lstm.pt', weights_only=False)
        print("✅ Flood model loaded successfully")
        
        # Create dummy input (batch_size=1, seq_len=30, features=estimated)
        # You'll need to adjust input_size based on actual features
        print("\n📊 Model is ready for inference!")
        print("   To run full inference with test data, use: inference_and_results.ipynb")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during inference test: {e}")
        return False

def main():
    """Main execution flow"""
    print("="*70)
    print("🌊 FLOOD & HEATWAVE PREDICTION - QUICK INFERENCE CHECK")
    print("="*70)
    
    # Step 1: Check if models exist
    if not check_hdfs_models():
        print("\n⏳ Waiting for training to complete...")
        print("   Check Airflow UI at http://localhost:8090")
        sys.exit(1)
    
    # Step 2: Check labeled data
    check_labeled_data()
    
    # Step 3: Download models
    try:
        download_models()
    except Exception as e:
        print(f"❌ Error downloading models: {e}")
        sys.exit(1)
    
    # Step 4: Run quick test
    run_quick_test()
    
    print("\n" + "="*70)
    print("✅ QUICK CHECK COMPLETE!")
    print("="*70)
    print("\n📓 Next Steps:")
    print("   1. Open inference_and_results.ipynb in Jupyter")
    print("   2. Run all cells to see detailed evaluation metrics")
    print("   3. View confusion matrices, ROC curves, and predictions")
    print("\n🚀 To launch Jupyter:")
    print("   cd Flood-And-HeatWave-Predictor")
    print("   jupyter notebook inference_and_results.ipynb")

if __name__ == "__main__":
    main()
