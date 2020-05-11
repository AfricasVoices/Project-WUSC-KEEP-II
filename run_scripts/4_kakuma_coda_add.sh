#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./4_coda_add.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Uploads coded messages datasets from '<data-root>/Outputs/Coda Files' to Coda"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

PROJECT_NAME="WUSC-KEEP-II"

DATASETS=(
    "kakuma_s01e01"
    "kakuma_s01e02"
    "kakuma_s01e03"
    "kakuma_s01e04"
    "kakuma_s01e05"
    "kakuma_s01e06"
    "kakuma_s01e07"
    "kakuma_s01_intro"

    "kakuma_location"
    "kakuma_gender"
    "kakuma_age"
    "kakuma_nationality"
    "kakuma_household_language"

    "kakuma_girls_education_champions"
    "kakuma_encouragement_for_boys"
    "kakuma_unmarried_fathers_community_view"
    "kakuma_lessons_learnt"
    "kakuma_show_suggestions"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout "9a9a8e708e3f20f37848a6b02f79bcee43e5be3b"  # (master which supports segmenting)

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${PROJECT_NAME}_${DATASET}..."

    pipenv run python add.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done
