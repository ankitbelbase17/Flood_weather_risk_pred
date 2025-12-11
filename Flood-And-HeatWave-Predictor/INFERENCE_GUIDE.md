# 📊 Viewing Inference Results - Complete Guide

## Overview
This guide shows you how to view inference results and test data predictions for the Flood & Heatwave prediction models.

## 🎯 Three Ways to View Results

### **Option 1: Using the Inference Notebook (RECOMMENDED)** ⭐

The most comprehensive way with visualizations and detailed metrics.

1. **Wait for pipeline to complete**
   - Open Airflow UI: http://localhost:8090
   - Login: `admin` / `admin`
   - Check `flood_heatwave_pipeline` DAG
   - Ensure all 5 tasks are green ✅

2. **Launch Jupyter Notebook**
   ```powershell
   cd Flood-And-HeatWave-Predictor
   jupyter notebook inference_and_results.ipynb
   ```

3. **Run all cells** (Kernel → Restart & Run All)

4. **You'll get:**
   - ✅ Classification reports (precision, recall, F1-score)
   - ✅ Confusion matrices
   - ✅ ROC curves with AUC scores
   - ✅ Prediction probability distributions
   - ✅ Sample predictions with actual vs predicted
   - ✅ False positive/negative analysis
   - ✅ Exported CSV files with all results

---

### **Option 2: Quick Command-Line Check**

For a fast status check without opening Jupyter.

```powershell
cd Flood-And-HeatWave-Predictor
python quick_inference.py
```

**What it does:**
- ✅ Checks if models exist in HDFS
- ✅ Downloads models to local `models/` folder
- ✅ Verifies labeled data exists
- ✅ Runs basic model loading test
- ✅ Shows next steps

---

### **Option 3: Manual HDFS Exploration**

Check raw outputs directly in HDFS.

#### **A. Check trained models**
```powershell
docker exec namenode hdfs dfs -ls /user/airflow/models/
```
You should see:
- `heatwave_lstm.pt` - Heatwave prediction model
- `flood_lstm.pt` - Flood prediction model

#### **B. Check labeled data (with predictions)**
```powershell
docker exec namenode hdfs dfs -ls /user/airflow/weather_data/labeled/
```

#### **C. Download labeled data for local analysis**
```powershell
# Download all labeled parquet files
docker exec namenode hdfs dfs -get /user/airflow/weather_data/labeled/ ./labeled_data/

# View in Python
python -c "import pandas as pd; df = pd.read_parquet('./labeled_data'); print(df.head()); print(df.info())"
```

---

## 📈 What Results You'll See

### **1. Classification Metrics**
```
HEATWAVE MODEL PERFORMANCE
═══════════════════════════════════════════════════════════
              precision    recall  f1-score   support

No Heatwave       0.95      0.98      0.96      4850
   Heatwave       0.87      0.75      0.81      1150

   accuracy                           0.93      6000
ROC AUC Score: 0.9234
```

### **2. Confusion Matrix**
Visual matrix showing:
- True Positives (correctly predicted events)
- True Negatives (correctly predicted non-events)
- False Positives (false alarms)
- False Negatives (missed events)

### **3. Sample Predictions**
```csv
Actual_Heatwave,Predicted_Heatwave,Heatwave_Probability,Actual_Flood,Predicted_Flood,Flood_Probability
0,0,0.12,0,0,0.08
1,1,0.89,0,0,0.15
0,0,0.34,1,1,0.76
```

### **4. Generated Files**
After running the inference notebook, you'll have:
- `inference_results.csv` - All predictions with probabilities
- `model_performance_summary.csv` - Summary metrics
- `confusion_matrices.png` - Visual confusion matrices
- `roc_curves.png` - ROC curve plots
- `probability_distributions.png` - Probability histograms

---

## 🚀 Step-by-Step Workflow

### **Step 1: Check Pipeline Status**
```powershell
# Option A: Via Airflow UI
Start http://localhost:8090

# Option B: Via command line
docker exec airflow-airflow-standalone-1 airflow dags list
```

### **Step 2: Verify Models are Trained**
```powershell
docker exec namenode hdfs dfs -ls /user/airflow/models/
```
✅ Should show: `heatwave_lstm.pt` and `flood_lstm.pt`

### **Step 3: Run Inference**
```powershell
# Quick check
python quick_inference.py

# Full analysis
jupyter notebook inference_and_results.ipynb
```

### **Step 4: View Results**
- Open generated PNG files for visualizations
- Open CSV files in Excel or pandas
- Review metrics in notebook output

---

## 🔧 Troubleshooting

### **Problem: Models not found**
```
❌ Models not found. Has the training pipeline completed?
```

**Solution:**
1. Check Airflow UI (http://localhost:8090)
2. Look for `flood_heatwave_pipeline` DAG
3. Verify all tasks completed successfully (green)
4. If tasks failed, check logs:
   ```powershell
   docker logs airflow-airflow-standalone-1 --tail 200
   ```

### **Problem: Jupyter not connecting to Spark**
```
Error: Cannot connect to spark://spark-spark-1:7077
```

**Solution:**
```powershell
# Check Spark is running
docker ps | Select-String spark

# Verify Spark connectivity
docker exec jupyter-spark pyspark --version
```

### **Problem: HDFS files not accessible**
```
FileNotFoundException: /user/airflow/weather_data/labeled
```

**Solution:**
```powershell
# Verify HDFS is running
docker exec namenode hdfs dfs -ls /

# Check if data was ingested
docker exec namenode hdfs dfs -ls /user/airflow/weather_data/raw/
```

---

## 📊 Understanding the Results

### **Key Metrics Explained**

1. **Accuracy**: Overall correctness (correct predictions / total predictions)
   - Good: > 0.85
   - Excellent: > 0.90

2. **Precision**: How many predicted positives are actually positive
   - Important for: Reducing false alarms
   - Formula: TP / (TP + FP)

3. **Recall**: How many actual positives we detected
   - Important for: Not missing events
   - Formula: TP / (TP + FN)

4. **F1-Score**: Harmonic mean of precision and recall
   - Balanced metric
   - Good: > 0.80

5. **ROC AUC**: Area under ROC curve
   - Measures classification quality
   - Perfect: 1.0
   - Random: 0.5
   - Good: > 0.85

### **Interpreting Predictions**

```python
# Probability ranges
0.0 - 0.3  → Very unlikely (confident negative)
0.3 - 0.5  → Unlikely (leaning negative)
0.5 - 0.7  → Likely (leaning positive)
0.7 - 1.0  → Very likely (confident positive)
```

---

## 🎯 Next Steps After Viewing Results

1. **Model is performing well (AUC > 0.85)**
   - Deploy for real-time predictions
   - Create dashboard for monitoring
   - Set up alerts for high-probability events

2. **Model needs improvement (AUC < 0.80)**
   - Add more features (weather variables)
   - Try different sequence lengths
   - Adjust model hyperparameters
   - Collect more training data

3. **High false positives**
   - Increase prediction threshold (0.5 → 0.6)
   - Add more strict feature engineering
   - Review labeling criteria

4. **High false negatives**
   - Decrease prediction threshold (0.5 → 0.4)
   - Add more sensitive features
   - Review if training data has enough positive examples

---

## 📁 File Structure After Inference

```
Flood-And-HeatWave-Predictor/
├── inference_and_results.ipynb       # Main inference notebook
├── quick_inference.py                # Quick check script
├── models/                            # Downloaded from HDFS
│   ├── heatwave_lstm.pt
│   └── flood_lstm.pt
├── inference_results.csv              # All predictions
├── model_performance_summary.csv      # Metrics summary
├── confusion_matrices.png             # Visualization
├── roc_curves.png                     # ROC curves
└── probability_distributions.png      # Probability plots
```

---

## 💡 Tips

1. **Always check Airflow first** to ensure pipeline completed
2. **Use the notebook** for comprehensive analysis
3. **Save all visualizations** for documentation/reports
4. **Compare with baseline** (random would be 0.5 AUC)
5. **Monitor false negatives carefully** - missing a flood is worse than a false alarm

---

## 🆘 Need Help?

1. **Pipeline not running?**
   - Check: `docker ps` - all containers running?
   - Check: http://localhost:8090 - Airflow accessible?

2. **Models not training?**
   - Check Airflow logs in UI
   - Verify data in HDFS: `docker exec namenode hdfs dfs -ls -R /user/airflow/`

3. **Inference errors?**
   - Ensure all packages installed: `pip install torch pandas numpy scikit-learn matplotlib seaborn pyspark`
   - Check Python version: `python --version` (should be 3.11.3)

---

**📧 For detailed logs:** Check Airflow UI → DAGs → flood_heatwave_pipeline → Graph View → Click on task → Logs
