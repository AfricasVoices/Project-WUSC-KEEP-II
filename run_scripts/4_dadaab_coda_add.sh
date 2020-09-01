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
    "dadaab_s01e01"
    "dadaab_s01e02"
    "dadaab_s01e03"
    "dadaab_s01e04"
    "dadaab_s01e05"
    "dadaab_s01e06"
    "dadaab_s01e07"
    "dadaab_s01_intro"

    "dadaab_location"
    "dadaab_gender"
    "dadaab_age"
    "dadaab_nationality"
    "dadaab_household_language"

    "dadaab_girls_education_champions"
    "dadaab_encouragement_for_boys"
    "dadaab_unmarried_fathers_community_view"
    "dadaab_lessons_learnt"
    "dadaab_show_suggestions"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout "9a9a8e708e3f20f37848a6b02f79bcee43e5be3b"  # (master which supports segmenting)

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${PROJECT_NAME}_${DATASET}..."

    pipenv run python add.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done