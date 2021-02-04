#!/usr/bin/env bash

set -e

if [[ $# -ne 6 ]]; then
    echo "Usage: ./8_upload_log_files.sh <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <run-id> <memory-profile-dir> <data-archive-dir>"
    echo "Uploads the pipeline's log files"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
RUN_ID=$4
MEMORY_PROFILE_DIR=$5
DATA_ARCHIVE_DIR=$6

cd ..
pipenv run python upload_log_files.py "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" \
    "$RUN_ID" "$MEMORY_PROFILE_DIR" "$DATA_ARCHIVE_DIR"
