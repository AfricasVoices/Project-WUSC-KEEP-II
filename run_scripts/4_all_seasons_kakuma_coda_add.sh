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

    "kakuma_s02e01"
    "kakuma_s02e02"
    "kakuma_s02e03"
    "kakuma_s02e04"
    "kakuma_s02e05"
    "kakuma_s02e06"
    "kakuma_s02e07"
    "kakuma_s02e08"

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

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${PROJECT_NAME}_${DATASET}..."

    pipenv run python add.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done
