#!/usr/bin/env bash

set -eou pipefail

GCP_PROJECT_ID="second-academy-449820-u6"
BQ_DATASET="de_zoomcamp_hw3"
GCP_BUCKET_PATH="gs://de-zoomcamp-hw3-akshay/*.parquet"
EXTERNAL_TABLE_NAME="yellow_trip_taxi_records_external"
REGULAR_TABLE_NAME_UNPARTITIONED="yellow_trip_taxi_records_regular_unpartitioned"

export GOOGLE_APPLICATION_CREDENTIALS="./gcp.json"

gcloud config set project "$GCP_PROJECT_ID"
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"

# Create an external table
TEMP_BQ_DEF_FILE_NAME="$(mktemp)"

echo "Generating BigQuery definition file for external table..."
bq mkdef \
    --source_format=PARQUET \
    --autodetect=true \
    $GCP_BUCKET_PATH > "$TEMP_BQ_DEF_FILE_NAME"

echo "Creating BigQuery external table..."
bq mk --table \
  --external_table_definition="$TEMP_BQ_DEF_FILE_NAME" \
  "$BQ_DATASET.$EXTERNAL_TABLE_NAME"

echo "External table: [$BQ_DATASET.$EXTERNAL_TABLE_NAME] created successfully!"

bq query \
    --destination_table "${BQ_DATASET}.${REGULAR_TABLE_NAME_UNPARTITIONED}" \
    --replace \
    -n 0 \
    --use_legacy_sql=false \
    "SELECT * FROM ${BQ_DATASET}.${EXTERNAL_TABLE_NAME}"

# END