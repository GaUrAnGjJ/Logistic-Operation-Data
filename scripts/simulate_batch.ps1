# ==============================================================================
# simulate_batch.ps1 - Simulates the arrival and ingestion of a data batch (PowerShell)
# simulate_batch.ps1 - End-to-End Batch Simulation & Medallion Pipeline Execution
# Usage:
#   .\scripts\simulate_batch.ps1 -BatchInput 001
#   .\scripts\simulate_batch.ps1 -BatchInput batch_001
#   .\scripts\simulate_batch.ps1 -BatchInput batch_002
#   .\scripts\simulate_batch.ps1 -BatchInput batch_003
# ==============================================================================

param (
    [string]$BatchInput = "001"
    [string]$BatchInput = "002",
    [switch]$Force
)

# Normalize batch identifier
if ($BatchInput -match '^\d+$') {
    $BatchId = "batch_{0:d3}" -f [int]$BatchInput
} elseif ($BatchInput -match '^batch_\d+$') {
    $BatchId = $BatchInput
} else {
    Write-Error "Invalid batch format '$BatchInput'. Use '001' or 'batch_001'."
    Write-Error "Invalid batch format '$BatchInput'. Use '002' or 'batch_002'."
    exit 1
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Starting Batch Simulation: $BatchId" -ForegroundColor Cyan
Write-Host " Starting End-to-End Simulation Pipeline: $BatchId" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Check local batch files
if (-not (Test-Path "data\batches\$BatchId")) {
    Write-Host "[1/4] Batch folder data\batches\$BatchId not found. Running batch_splitter.py..." -ForegroundColor Yellow
    python src\ingestion\batch_splitter.py
} else {
    Write-Host "[1/4] Local batch files verified in data\batches\$BatchId" -ForegroundColor Green
$cmdArgs = @("src/orchestration/pipeline_orchestrator.py", "--batch_id", $BatchId)
if ($Force) {
    $cmdArgs += "--force"
}

# 2. Idempotency Check in BigQuery
Write-Host "[2/4] Checking batch status in BigQuery audit table..." -ForegroundColor Yellow
python src\ingestion\batch_controller.py --check $BatchId
if ($LASTEXITCODE -ne 0) {
    Write-Host "[IDEMPOTENCY] Batch $BatchId has ALREADY been COMPLETED in BigQuery." -ForegroundColor Yellow
    Write-Host "Skipping re-processing to prevent duplicate ingestion." -ForegroundColor Yellow
    exit 0
}
python @cmdArgs

# 3. Upload batch to GCS raw zone
Write-Host "[3/4] Uploading batch data to GCS raw/ and manifests/..." -ForegroundColor Yellow
python src\ingestion\gcs_uploader.py --batch_id $BatchId
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to upload $BatchId to GCS."
    Write-Error "Pipeline failed for $BatchId."
    exit 1
}

# 4. Register batch arrival in BigQuery audit table
Write-Host "[4/4] Registering batch arrival in logistics_audit.batch_control..." -ForegroundColor Yellow
python src\ingestion\batch_controller.py --register $BatchId

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Batch Simulation for $BatchId completed successfully!" -ForegroundColor Green
Write-Host " GCS Raw Zone and Manifests are ready for Bronze processing." -ForegroundColor Green
Write-Host " Simulation Pipeline for $BatchId Completed Successfully!" -ForegroundColor Green
Write-Host " Data is fully published in BigQuery logistics_gold." -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
