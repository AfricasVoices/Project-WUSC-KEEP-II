#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./1_s03_kakuma_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

PROJECT_NAME="WUSC-KEEP-II"
DATASETS=(
    "kakuma_s03e01"
    "kakuma_s03e02"
    "kakuma_s03e03"
    "kakuma_s03e04"
    "kakuma_s03e05"
    "kakuma_expectations_from_educated_girls"
    "kakuma_prevention_of_sgbv_cases"
    "s03_kakuma_lessons_learnt"
    "s03_kakuma_impact_made"
    "kakuma_s03_close_out"

    "kakuma_location"
    "kakuma_gender"
    "kakuma_age"
    "kakuma_nationality"
    "kakuma_household_language"
    "kakuma_disabled"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout "e895887b3abceb63bab672a262d5c1dd73dcad92"  # (master which supports incremental get)

mkdir -p "$DATA_ROOT/Coded Coda Files"

for DATASET in ${DATASETS[@]}
do
FILE="$DATA_ROOT/Coded Coda Files/$DATASET.json"

    if [ -e "$FILE" ]; then
        echo "Getting messages data from ${PROJECT_NAME}_${DATASET} (incremental update)..."
        MESSAGES=$(pipenv run python get.py --previous-export-file-path "$FILE" "$AUTH" "${PROJECT_NAME}_${DATASET}" messages)
        echo "$MESSAGES" >"$FILE"
    else
        echo "Getting messages data from ${PROJECT_NAME}_${DATASET} (full download)..."
        pipenv run python get.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages >"$FILE"
    fi
done
