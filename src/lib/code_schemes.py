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

    KAKUMA_S01E01_REASONS = _open_scheme("kakuma_s01e01_reasons.json")
    KAKUMA_S01E02_REASONS = _open_scheme("kakuma_s01e02_reasons.json")
    KAKUMA_S01E03_REASONS = _open_scheme("kakuma_s01e03_reasons.json")
    KAKUMA_S01E04_REASONS = _open_scheme("kakuma_s01e04_reasons.json")
    KAKUMA_S01E05_REASONS = _open_scheme("kakuma_s01e05_reasons.json")
    KAKUMA_S01E06_REASONS = _open_scheme("kakuma_s01e06_reasons.json")
    KAKUMA_S01E07_REASONS = _open_scheme("kakuma_s01e07_reasons.json")
    KAKUMA_S01_INTRO_REASONS = _open_scheme("kakuma_s01_intro_reasons.json")

    GENDER = _open_scheme("gender.json")
    NATIONALITY = _open_scheme("nationality.json")
    AGE = _open_scheme("age.json")
    DADAAB_HOUSEHOLD_LANGUAGE = _open_scheme("dadaab_household_language.json") # We have different language & location schemes for the two camps
    DADAAB_LOCATION = _open_scheme("dadaab_location.json")
    KAKUMA_HOUSEHOLD_LANGUAGE = _open_scheme("kakuma_household_language.json")
    KAKUMA_LOCATION = _open_scheme("kakuma_location.json")

    WS_CORRECT_DATASET = None
    KAKUMA_WS_CORRECT_DATASET = _open_scheme("kakuma_ws_correct_dataset.json")
    DADAAB_WS_CORRECT_DATASET = _open_scheme("dadaab_ws_correct_dataset.json")
