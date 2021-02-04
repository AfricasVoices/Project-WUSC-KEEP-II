#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./1_kakuma_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
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

    "kakuma_s02e01"
    "kakuma_s02e02"
    "kakuma_s02e03"
    "kakuma_s02e04"
    "kakuma_s02e05"
    "kakuma_s02e06"
    "kakuma_s02e07"
    "kakuma_s02e08"

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

    "kakuma_community_views_on_girls_education"
    "kakuma_community_views_on_girls_education_final"
    "kakuma_responses_to_sexual_violence"
    "kakuma_adolescent_mothers_challenges"

    "s02_kakuma_lessons_learnt"
    "s02_kakuma_impact_made"
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
