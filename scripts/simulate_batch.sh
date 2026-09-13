#!/usr/bin/env bash
# ==============================================================================
# simulate_batch.sh - Simulates the arrival and ingestion of a data batch
# Usage:
#   ./scripts/simulate_batch.sh 001
#   ./scripts/simulate_batch.sh batch_001
# ==============================================================================

set -e

BATCH_INPUT=${1:-"001"}

# Normalize batch identifier to format: batch_00X
if [[ "$BATCH_INPUT" =~ ^[0-9]+$ ]]; then
    BATCH_ID=$(printf "batch_%03d" "$BATCH_INPUT")
elif [[ "$BATCH_INPUT" =~ ^batch_[0-9]+$ ]]; then
    BATCH_ID="$BATCH_INPUT"
else
    echo "ERROR: Invalid batch format '$BATCH_INPUT'. Use '001' or 'batch_001'."
    exit 1
fi

echo "=========================================================="
echo " Starting Batch Simulation: $BATCH_ID"
echo "=========================================================="

# 1. Check if batches exist, if not generate them
if [ ! -d "data/batches/$BATCH_ID" ]; then
    echo "[1/4] Batch folder data/batches/$BATCH_ID not found. Running batch_splitter.py..."
    python src/ingestion/batch_splitter.py
else
    echo "[1/4] Local batch files verified in data/batches/$BATCH_ID"
fi

# 2. Idempotency check via BigQuery Batch Controller
echo "[2/4] Checking batch status in BigQuery audit table..."
set +e
python src/ingestion/batch_controller.py --check "$BATCH_ID"
CHECK_STATUS=$?
set -e

if [ $CHECK_STATUS -ne 0 ]; then
    echo "[IDEMPOTENCY] Batch $BATCH_ID has ALREADY been COMPLETED in BigQuery."
    echo "Skipping re-processing to prevent duplicate ingestion."
    exit 0
fi

# 3. Upload batch to Google Cloud Storage raw zone
echo "[3/4] Uploading batch data to GCS raw/ and manifests/..."
python src/ingestion/gcs_uploader.py --batch_id "$BATCH_ID"

# 4. Register batch arrival in BigQuery audit table
echo "[4/4] Registering batch arrival in logistics_audit.batch_control..."
python src/ingestion/batch_controller.py --register "$BATCH_ID"

echo "=========================================================="
echo " Batch Simulation for $BATCH_ID completed successfully!"
echo " GCS Raw Zone and Manifests are ready for Bronze processing."
echo "=========================================================="
