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

    "dadaab_s02e01"
    "dadaab_s02e02"
    "dadaab_s02e03"
    "dadaab_s02e04"
    "dadaab_s02e05"
    "dadaab_s02e06"
    "dadaab_s02e07"
    "dadaab_s02e08"

    "dadaab_s03e01"
    "dadaab_s03e02"
    "dadaab_s03e03"
    "dadaab_s03e04"
    "dadaab_s03e05"
    "dadaab_expectations_from_educated_girls"
    "dadaab_prevention_of_sgbv_cases"
    "s03_dadaab_lessons_learnt"
    "s03_dadaab_impact_made"
    "dadaab_s03_close_out"

    "dadaab_location"
    "dadaab_gender"
    "dadaab_age"
    "dadaab_nationality"
    "dadaab_household_language"
    "dadaab_disabled"

    "dadaab_girls_education_champions"
    "dadaab_encouragement_for_boys"
    "dadaab_unmarried_fathers_community_view"
    "dadaab_lessons_learnt"
    "dadaab_show_suggestions"

    "dadaab_community_views_on_girls_education"
    "dadaab_community_views_on_girls_education_final"
    "dadaab_responses_to_sexual_violence"
    "dadaab_adolescent_mothers_challenges"

    "s02_dadaab_lessons_learnt"
    "s02_dadaab_impact_made"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout "e895887b3abceb63bab672a262d5c1dd73dcad92"  # (master which supports incremental get)

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${PROJECT_NAME}_${DATASET}..."

    pipenv run python add.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done
