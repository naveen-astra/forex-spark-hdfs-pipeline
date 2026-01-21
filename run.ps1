# Forex Data Preprocessing Runner Script (PowerShell)

Write-Host "=== Forex Data Preprocessing ===" -ForegroundColor Cyan
Write-Host ""

# Step 1: Upload data to HDFS
Write-Host "Step 1: Uploading data to HDFS..." -ForegroundColor Yellow
sbt "runMain HDFSDataLoader"

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to upload data to HDFS" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Step 2: Preprocessing data with Spark..." -ForegroundColor Yellow
sbt "runMain ForexDataPreprocessor"

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to preprocess data" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Processing Complete ===" -ForegroundColor Green
Write-Host "Check HDFS for processed data:"
Write-Host "  hdfs dfs -ls /user/forex/processed/"
