import json

from core_data_modules.data_models import CodeScheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return CodeScheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    DADAAB_S01E01_REASONS = _open_scheme("dadaab_s01e01_reasons.json")
    DADAAB_S01E02_REASONS = _open_scheme("dadaab_s01e02_reasons.json")
    DADAAB_S01E03_REASONS = _open_scheme("dadaab_s01e03_reasons.json")
    DADAAB_S01E04_REASONS = _open_scheme("dadaab_s01e04_reasons.json")
    DADAAB_S01E05_REASONS = _open_scheme("dadaab_s01e05_reasons.json")
    DADAAB_S01E06_REASONS = _open_scheme("dadaab_s01e06_reasons.json")
    DADAAB_S01E07_REASONS = _open_scheme("dadaab_s01e07_reasons.json")
    DADAAB_S01_INTRO_REASONS = _open_scheme("dadaab_s01_intro_reasons.json")

    DADAAB_S02E01_REASONS = _open_scheme("dadaab_s02e01_reasons.json")
    DADAAB_S02E02_REASONS = _open_scheme("dadaab_s02e02_reasons.json")
    DADAAB_S02E03_REASONS = _open_scheme("dadaab_s02e03_reasons.json")
    DADAAB_S02E04_REASONS = _open_scheme("dadaab_s02e04_reasons.json")
    DADAAB_S02E05_REASONS = _open_scheme("dadaab_s02e05_reasons.json")
    DADAAB_S02E06_REASONS = _open_scheme("dadaab_s02e06_reasons.json")
    DADAAB_S02E07_REASONS = _open_scheme("dadaab_s02e07_reasons.json")
    DADAAB_S02E08_REASONS = _open_scheme("dadaab_s02e08_reasons.json")

    KAKUMA_S01E01_REASONS = _open_scheme("kakuma_s01e01_reasons.json")
    KAKUMA_S01E02_REASONS = _open_scheme("kakuma_s01e02_reasons.json")
    KAKUMA_S01E03_REASONS = _open_scheme("kakuma_s01e03_reasons.json")
    KAKUMA_S01E04_REASONS = _open_scheme("kakuma_s01e04_reasons.json")
    KAKUMA_S01E05_REASONS = _open_scheme("kakuma_s01e05_reasons.json")
    KAKUMA_S01E06_REASONS = _open_scheme("kakuma_s01e06_reasons.json")
    KAKUMA_S01E07_REASONS = _open_scheme("kakuma_s01e07_reasons.json")
    KAKUMA_S01E08_REASONS = _open_scheme("kakuma_s01e08_reasons.json")
    KAKUMA_S01_INTRO_REASONS = _open_scheme("kakuma_s01_intro_reasons.json")

    KAKUMA_S02E01_REASONS = _open_scheme("kakuma_s02e01_reasons.json")
    KAKUMA_S02E02_REASONS = _open_scheme("kakuma_s02e02_reasons.json")
    KAKUMA_S02E03_REASONS = _open_scheme("kakuma_s02e03_reasons.json")
    KAKUMA_S02E04_REASONS = _open_scheme("kakuma_s02e04_reasons.json")
    KAKUMA_S02E05_REASONS = _open_scheme("kakuma_s02e05_reasons.json")
    KAKUMA_S02E06_REASONS = _open_scheme("kakuma_s02e06_reasons.json")
    KAKUMA_S02E07_REASONS = _open_scheme("kakuma_s02e07_reasons.json")
    KAKUMA_S02E08_REASONS = _open_scheme("kakuma_s02e08_reasons.json")

    GENDER = _open_scheme("gender.json")
    NATIONALITY = _open_scheme("nationality.json")
    AGE = _open_scheme("age.json")
    AGE_CATEGORY = _open_scheme("age_category.json")
    DADAAB_HOUSEHOLD_LANGUAGE = _open_scheme("dadaab_household_language.json") # We have different language & location schemes for the two camps
    DADAAB_LOCATION = _open_scheme("dadaab_location.json")
    KAKUMA_HOUSEHOLD_LANGUAGE = _open_scheme("kakuma_household_language.json")
    KAKUMA_LOCATION = _open_scheme("kakuma_location.json")

    DADAAB_GIRLS_EDUCATION_CHAMPIONS = _open_scheme("dadaab_girls_education_champions.json")
    DADAAB_ENCOURAGEMENT_FOR_BOYS_CHAMPIONS = _open_scheme("dadaab_encouragement_for_boys.json")
    DADAAB_UNMARRIED_FATHERS_COMMUNITY_VIEW = _open_scheme("dadaab_unmarried_fathers_community_view.json")
    DADAAB_LESSONS_LEARNT = _open_scheme("dadaab_lessons_learnt.json")
    DADAAB_SHOW_SUGGESTIONS = _open_scheme("dadaab_show_suggestions.json")

    DADAAB_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION = _open_scheme("dadaab_community_views_on_girls_education.json")
    DADAAB_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION_FINAL = _open_scheme("dadaab_community_views_on_girls_education_final.json")
    DADAAB_RESPONSES_TO_SEXUAL_VIOLENCE = _open_scheme("dadaab_responses_to_sexual_violence.json")
    DADAAB_ADOLESCENT_MOTHERS_CHALLENGES = _open_scheme("dadaab_adolescent_mothers_challenges.json")

    KAKUMA_GIRLS_EDUCATION_CHAMPIONS = _open_scheme("kakuma_girls_education_champions.json")
    KAKUMA_ENCOURAGEMENT_FOR_BOYS_CHAMPIONS = _open_scheme("kakuma_encouragement_for_boys.json")
    KAKUMA_UNMARRIED_FATHERS_COMMUNITY_VIEW = _open_scheme("kakuma_unmarried_fathers_community_view.json")
    KAKUMA_LESSONS_LEARNT = _open_scheme("kakuma_lessons_learnt.json")
    KAKUMA_SHOW_SUGGESTIONS = _open_scheme("kakuma_show_suggestions.json")

    KAKUMA_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION = _open_scheme("kakuma_community_views_on_girls_education.json")
    KAKUMA_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION_FINAL = _open_scheme("kakuma_community_views_on_girls_education_final.json")
    KAKUMA_RESPONSES_TO_SEXUAL_VIOLENCE = _open_scheme("kakuma_responses_to_sexual_violence.json")
    KAKUMA_ADOLESCENT_MOTHERS_CHALLENGES = _open_scheme("kakuma_adolescent_mothers_challenges.json")

    KAKUMA_WS_CORRECT_DATASET = _open_scheme("kakuma_ws_correct_dataset.json")
    DADAAB_WS_CORRECT_DATASET = _open_scheme("dadaab_ws_correct_dataset.json")
