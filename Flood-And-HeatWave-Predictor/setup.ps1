# Flood & Heatwave Prediction - Setup Script
# This script automates the complete setup process

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  Flood & Heatwave Prediction - Setup" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Check Docker
Write-Host "[1/6] Checking Docker..." -ForegroundColor Yellow
$dockerRunning = docker ps 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Docker is not running. Please start Docker Desktop." -ForegroundColor Red
    exit 1
}
Write-Host "✅ Docker is running" -ForegroundColor Green
Write-Host ""

# Step 2: Create network
Write-Host "[2/6] Setting up Docker network..." -ForegroundColor Yellow
$networkExists = docker network ls | Select-String "custom_network"
if (-not $networkExists) {
    docker network create -d bridge custom_network
    Write-Host "✅ Created custom_network" -ForegroundColor Green
} else {
    Write-Host "✅ custom_network already exists" -ForegroundColor Green
}
Write-Host ""

# Step 3: Start services
Write-Host "[3/6] Starting Big Data services..." -ForegroundColor Yellow
Write-Host "   This may take 2-3 minutes..." -ForegroundColor Gray
cd BigDataCluster
make start-all
Write-Host "✅ Services started" -ForegroundColor Green
Write-Host ""

# Step 4: Wait for services to be ready
Write-Host "[4/6] Waiting for services to initialize (30 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30
Write-Host "✅ Services should be ready" -ForegroundColor Green
Write-Host ""

# Step 5: Upload data
Write-Host "[5/6] Uploading data to HDFS..." -ForegroundColor Yellow
$dataFile = "..\data\terai_districts_weather.csv"
if (Test-Path $dataFile) {
    docker cp $dataFile namenode:/tmp/
    docker exec namenode hdfs dfs -mkdir -p /user/airflow/weather_data/raw 2>$null
    docker exec namenode hdfs dfs -put /tmp/terai_districts_weather.csv /user/airflow/weather_data/raw/ 2>$null
    
    # Create output directories
    docker exec namenode hdfs dfs -mkdir -p /user/airflow/weather_data/features 2>$null
    docker exec namenode hdfs dfs -mkdir -p /user/airflow/weather_data/labeled 2>$null
    docker exec namenode hdfs dfs -mkdir -p /user/airflow/models 2>$null
    
    Write-Host "✅ Data uploaded to HDFS" -ForegroundColor Green
} else {
    Write-Host "⚠️  Data file not found: $dataFile" -ForegroundColor Yellow
    Write-Host "   Please ensure terai_districts_weather.csv exists in data/ folder" -ForegroundColor Gray
}
Write-Host ""

# Step 6: Deploy DAGs
Write-Host "[6/6] Deploying Airflow DAGs..." -ForegroundColor Yellow
docker cp dags/flood_heatwave_pipeline.py airflow-airflow-standalone-1:/opt/airflow/dags/ 2>$null
docker cp dags/tasks airflow-airflow-standalone-1:/opt/airflow/dags/ 2>$null
docker exec airflow-airflow-standalone-1 airflow db upgrade 2>$null
Write-Host "✅ DAGs deployed" -ForegroundColor Green
Write-Host ""

# Summary
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  ✅ Setup Complete!" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "📊 Access Points:" -ForegroundColor Cyan
Write-Host "   Airflow:  http://localhost:8090 (admin/admin)" -ForegroundColor White
Write-Host "   HDFS:     http://localhost:9870" -ForegroundColor White
Write-Host "   Spark:    http://localhost:8081" -ForegroundColor White
Write-Host ""
Write-Host "🚀 Next Steps:" -ForegroundColor Cyan
Write-Host "   1. Open Airflow UI: http://localhost:8090" -ForegroundColor White
Write-Host "   2. Login with: admin / admin" -ForegroundColor White
Write-Host "   3. Find 'flood_heatwave_pipeline' DAG" -ForegroundColor White
Write-Host "   4. Click the ▶️ button to trigger the pipeline" -ForegroundColor White
Write-Host "   5. Monitor execution (takes ~10-20 minutes)" -ForegroundColor White
Write-Host ""
Write-Host "📈 After pipeline completes:" -ForegroundColor Cyan
Write-Host "   cd .." -ForegroundColor Gray
Write-Host "   python quick_inference.py" -ForegroundColor Gray
Write-Host "   # or" -ForegroundColor Gray
Write-Host "   jupyter notebook inference_and_results.ipynb" -ForegroundColor Gray
Write-Host ""
Write-Host "To stop services: cd BigDataCluster; make stop-all" -ForegroundColor Yellow
Write-Host ""
