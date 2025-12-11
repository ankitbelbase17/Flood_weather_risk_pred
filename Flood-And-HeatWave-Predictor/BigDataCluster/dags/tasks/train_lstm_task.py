"""
LSTM Training Task for Airflow
Trains LSTM model and saves to HDFS
"""
import argparse
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from pyspark.sql import SparkSession

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

def build_sequences(df_pandas, timesteps=14, target='heatwave'):
    """Build sequences from DataFrame"""
    df_pandas = df_pandas.sort_values(['District','Date']).copy()
    
    exclude_cols = ['Date', 'District', 'heatwave', 'flood_proxy', 'heat_exceed',
                    'precip_1d', 'precip_p99', 'precip3_p98', 'wetness_flag',
                    'temp_p95', 'rh_p90', 'precip_3d_sum']
    feature_cols = [c for c in df_pandas.columns if c not in exclude_cols]
    
    sequences = []
    targets = []
    
    for district, g in df_pandas.groupby('District'):
        arr = g[feature_cols].fillna(0).values
        lab = g[target].fillna(0).astype(int).values
        
        if len(arr) < timesteps + 1:
            continue
        
        for i in range(timesteps, len(arr)):
            seq = arr[i-timesteps:i]
            sequences.append(seq)
            targets.append(lab[i])
    
    X = np.array(sequences, dtype=np.float32)
    y = np.array(targets, dtype=np.float32)
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    
    return X, y, feature_cols

def train_model(target='heatwave'):
    """Train LSTM model"""
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"TrainLSTM_{target}") \
        .master("spark://spark-spark-1:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    # Read labeled data from HDFS
    hdfs_input = "hdfs://namenode:9000/user/airflow/weather_data/labeled"
    df_spark = spark.read.parquet(hdfs_input)
    
    # Convert to Pandas for sequence building
    df_pandas = df_spark.toPandas()
    
    print(f"Training LSTM for {target} prediction")
    print(f"Data shape: {df_pandas.shape}")
    
    # Build sequences
    X, y, feature_cols = build_sequences(df_pandas, timesteps=14, target=target)
    print(f"Sequences shape: {X.shape}, Labels shape: {y.shape}")
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=True, random_state=42
    )
    
    # Scale features
    nsamples, nt, nf = X_train.shape
    scaler = StandardScaler()
    X_train_flat = X_train.reshape(-1, nf)
    X_test_flat = X_test.reshape(-1, nf)
    scaler.fit(X_train_flat)
    X_train_scaled = scaler.transform(X_train_flat).reshape(X_train.shape)
    X_test_scaled = scaler.transform(X_test_flat).reshape(X_test.shape)
    
    # Create tensors and loaders
    train_dataset = TensorDataset(
        torch.FloatTensor(X_train_scaled),
        torch.FloatTensor(y_train)
    )
    test_dataset = TensorDataset(
        torch.FloatTensor(X_test_scaled),
        torch.FloatTensor(y_test)
    )
    
    train_loader = DataLoader(train_dataset, batch_size=128, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=128, shuffle=False)
    
    # Setup model
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = LSTMModel(input_size=nf, hidden_size=64, fc_size=32).to(device)
    criterion = nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(model.parameters())
    
    # Training loop
    epochs = 20
    for epoch in range(epochs):
        model.train()
        train_loss = 0.0
        for X_batch, y_batch in train_loader:
            X_batch, y_batch = X_batch.to(device), y_batch.to(device)
            optimizer.zero_grad()
            outputs = model(X_batch)
            loss = criterion(outputs, y_batch)
            loss.backward()
            optimizer.step()
            train_loss += loss.item() * X_batch.size(0)
        
        train_loss /= len(train_loader.dataset)
        
        # Validation
        model.eval()
        val_loss = 0.0
        all_preds = []
        all_targets = []
        with torch.no_grad():
            for X_batch, y_batch in test_loader:
                X_batch, y_batch = X_batch.to(device), y_batch.to(device)
                outputs = model(X_batch)
                loss = criterion(outputs, y_batch)
                val_loss += loss.item() * X_batch.size(0)
                probs = torch.sigmoid(outputs)
                all_preds.extend(probs.cpu().numpy())
                all_targets.extend(y_batch.cpu().numpy())
        
        val_loss /= len(test_loader.dataset)
        val_auc = roc_auc_score(all_targets, all_preds) if len(set(all_targets)) > 1 else 0.0
        
        print(f'Epoch {epoch+1}/{epochs} - Train Loss: {train_loss:.4f} - Val Loss: {val_loss:.4f} - Val AUC: {val_auc:.4f}')
    
    # Save model to local first, then to HDFS
    model_path = f'/tmp/lstm_{target}.pt'
    torch.save({
        'model_state_dict': model.state_dict(),
        'input_size': nf,
        'hidden_size': 64,
        'fc_size': 32,
        'timesteps': 14,
        'feature_cols': feature_cols,
        'scaler_mean': scaler.mean_,
        'scaler_scale': scaler.scale_
    }, model_path)
    
    print(f"✓ Model trained and saved to {model_path}")
    print(f"  Final Val AUC: {val_auc:.4f}")
    
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', default='heatwave', choices=['heatwave', 'flood_proxy'])
    args = parser.parse_args()
    train_model(target=args.target)
