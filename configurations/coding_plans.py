from core_data_modules.cleaners import somali, swahili, Codes
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies

from configurations import code_imputation_functions
from configurations.code_schemes import CodeSchemes
from src.lib.configuration_objects import CodingConfiguration, CodingModes, CodingPlan


def clean_age_with_range_filter(text):
    """
    Cleans age from the given `text`, setting to NC if the cleaned age is not in the range 10 <= age < 100.
    """
    age = swahili.DemographicCleaner.clean_age(text)
    if type(age) == int and 10 <= age < 100:
        return str(age)
        # TODO: Once the cleaners are updated to not return Codes.NOT_CODED, this should be updated to still return
        #       NC in the case where age is an int but is out of range
    else:
        return Codes.NOT_CODED


S01_DADAAB_RQA_CODING_PLANS  = [
            CodingPlan(raw_field="rqa_s01e01_raw",
                       dataset_name="dadaab_s01e01",
                       time_field="sent_on",
                       run_id_field="rqa_s01e01_run_id",
                       coda_filename="dadaab_s01e01.json",
                       icr_filename="dadaab_s01e01.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S01E01_REASONS,
                               coded_field="rqa_s01e01_coded",
                               analysis_file_key="rqa_s01e01_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S01E01_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s01e01"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e02_raw",
                       dataset_name="dadaab_s01e02",
                       time_field="sent_on",
                       run_id_field="rqa_s01e02_run_id",
                       coda_filename="dadaab_s01e02.json",
                       icr_filename="dadaab_s01e02.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S01E02_REASONS,
                               coded_field="rqa_s01e02_coded",
                               analysis_file_key="rqa_s01e02_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S01E02_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s01e02"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e03_raw",
                       dataset_name="dadaab_s01e03",
                       time_field="sent_on",
                       run_id_field="rqa_s01e03_run_id",
                       coda_filename="dadaab_s01e03.json",
                       icr_filename="dadaab_s01e03.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S01E03_REASONS,
                               coded_field="rqa_s01e03_coded",
                               analysis_file_key="rqa_s01e03_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S01E03_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s01e03"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e04_raw",
                       dataset_name="dadaab_s01e04",
                       time_field="sent_on",
                       run_id_field="rqa_s01e04_run_id",
                       coda_filename="dadaab_s01e04.json",
                       icr_filename="dadaab_s01e04.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S01E04_REASONS,
                               coded_field="rqa_s01e04_coded",
                               analysis_file_key="rqa_s01e04_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S01E04_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s01e04"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e05_raw",
                       dataset_name="dadaab_s01e05",
                       time_field="sent_on",
                       run_id_field="rqa_s01e05_run_id",
                       coda_filename="dadaab_s01e05.json",
                       icr_filename="dadaab_s01e05.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S01E05_REASONS,
                               coded_field="rqa_s01e05_coded",
                               analysis_file_key="rqa_s01e05_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S01E05_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s01e05"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e06_raw",
                       dataset_name="dadaab_s01e06",
                       time_field="sent_on",
                       run_id_field="rqa_s01e06_run_id",
                       coda_filename="dadaab_s01e06.json",
                       icr_filename="dadaab_s01e06.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S01E06_REASONS,
                               coded_field="rqa_s01e06_coded",
                               analysis_file_key="rqa_s01e06_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S01E06_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s01e06"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e07_raw",
                       dataset_name="dadaab_s01e07",
                       time_field="sent_on",
                       run_id_field="rqa_s01e07_run_id",
                       coda_filename="dadaab_s01e07.json",
                       icr_filename="dadaab_s01e07.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S01E07_REASONS,
                               coded_field="rqa_s01e07_coded",
                               analysis_file_key="rqa_s01e07_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S01E07_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s01e07"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01_intro_raw",
                       dataset_name="dadaab_s01_intro",
                       time_field="sent_on",
                       run_id_field="rqa_s01_intro_run_id",
                       coda_filename="dadaab_s01_intro.json",
                       icr_filename="dadaab_s01_intro.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S01_INTRO_REASONS,
                               coded_field="rqa_s01_intro_coded",
                               analysis_file_key="rqa_s01_intro_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S01_INTRO_REASONS,
                                   x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s01 intro"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),
        ]

S02_DADAAB_RQA_CODING_PLANS  = [
            CodingPlan(raw_field="community_views_on_girls_education_raw",
                       dataset_name="dadaab_community_views_on_girls_education",
                       time_field="sent_on",
                       coda_filename="dadaab_community_views_on_girls_education.json",
                       run_id_field="community_views_on_girls_education_run_id",
                       icr_filename="dadaab_community_views_on_girls_education.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION,
                               coded_field="community_views_on_girls_education_coded",
                               analysis_file_key="community_views_on_girls_education_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                           "dadaab community views on girls education"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="community_views_on_girls_education_final_raw",
                                   dataset_name="dadaab_community_views_on_girls_education_final",
                                   time_field="sent_on",
                                   coda_filename="dadaab_community_views_on_girls_education_final.json",
                                   run_id_field="community_views_on_girls_education_final_run_id",
                                   icr_filename="dadaab_community_views_on_girls_education_final.csv",
                                   coding_configurations=[
                                       CodingConfiguration(
                                           coding_mode=CodingModes.MULTIPLE,
                                           code_scheme=CodeSchemes.DADAAB_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION_FINAL,
                                           coded_field="community_views_on_girls_education_final_coded",
                                           analysis_file_key="community_views_on_girls_education_final_",
                                           fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                               CodeSchemes.DADAAB_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION_FINAL, x, y)
                                       )
                                   ],
                                   ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                                       "dadaab community views on girls education final"),
                                   raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e01_raw",
                       dataset_name="dadaab_s02e01",
                       time_field="sent_on",
                       run_id_field="rqa_s02e01_run_id",
                       coda_filename="dadaab_s02e01.json",
                       icr_filename="dadaab_s02e01.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S02E01_REASONS,
                               coded_field="rqa_s02e01_coded",
                               analysis_file_key="rqa_s02e01_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S02E01_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s02e01"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e02_raw",
                       dataset_name="dadaab_s02e02",
                       time_field="sent_on",
                       run_id_field="rqa_s02e02_run_id",
                       coda_filename="dadaab_s02e02.json",
                       icr_filename="dadaab_s02e02.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S02E02_REASONS,
                               coded_field="rqa_s02e02_coded",
                               analysis_file_key="rqa_s02e02_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S02E02_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s02e02"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e03_raw",
                       dataset_name="dadaab_s02e03",
                       time_field="sent_on",
                       run_id_field="rqa_s02e03_run_id",
                       coda_filename="dadaab_s02e03.json",
                       icr_filename="dadaab_s02e03.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S02E03_REASONS,
                               coded_field="rqa_s02e03_coded",
                               analysis_file_key="rqa_s02e03_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S02E03_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s02e03"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e04_raw",
                       dataset_name="dadaab_s02e04",
                       time_field="sent_on",
                       run_id_field="rqa_s02e04_run_id",
                       coda_filename="dadaab_s02e04.json",
                       icr_filename="dadaab_s02e04.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S02E04_REASONS,
                               coded_field="rqa_s02e04_coded",
                               analysis_file_key="rqa_s02e04_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S02E04_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s02e04"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e05_raw",
                       dataset_name="dadaab_s02e05",
                       time_field="sent_on",
                       run_id_field="rqa_s02e05_run_id",
                       coda_filename="dadaab_s02e05.json",
                       icr_filename="dadaab_s02e05.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S02E05_REASONS,
                               coded_field="rqa_s02e05_coded",
                               analysis_file_key="rqa_s02e05_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S02E05_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s02e05"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e06_raw",
                       dataset_name="dadaab_s02e06",
                       time_field="sent_on",
                       run_id_field="rqa_s02e06_run_id",
                       coda_filename="dadaab_s02e06.json",
                       icr_filename="dadaab_s02e06.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S02E06_REASONS,
                               coded_field="rqa_s02e06_coded",
                               analysis_file_key="rqa_s02e06_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S02E06_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s02e06"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e07_raw",
                       dataset_name="dadaab_s02e07",
                       time_field="sent_on",
                       run_id_field="rqa_s02e07_run_id",
                       coda_filename="dadaab_s02e07.json",
                       icr_filename="dadaab_s02e07.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S02E07_REASONS,
                               coded_field="rqa_s02e07_coded",
                               analysis_file_key="rqa_s02e07_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S02E07_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s02e07"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e08_raw",
                                   dataset_name="dadaab_s02e08",
                                   time_field="sent_on",
                                   run_id_field="rqa_s02e08_run_id",
                                   coda_filename="dadaab_s02e08.json",
                                   icr_filename="dadaab_s02e08.csv",
                                   coding_configurations=[
                                       CodingConfiguration(
                                           coding_mode=CodingModes.MULTIPLE,
                                           code_scheme=CodeSchemes.DADAAB_S02E08_REASONS,
                                           coded_field="rqa_s02e08_coded",
                                           analysis_file_key="rqa_s02e08_",
                                           fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                               CodeSchemes.DADAAB_S02E08_REASONS, x, y)
                                       )
                                   ],
                                   ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s02e08"),
                                   raw_field_fold_strategy=FoldStrategies.concatenate),
        ]

S03_DADAAB_RQA_CODING_PLANS = [
    CodingPlan(raw_field="rqa_s03e01_raw",
               dataset_name="dadaab_s03e01",
               time_field="sent_on",
               run_id_field="rqa_s03e01_run_id",
               coda_filename="dadaab_s03e01.json",
               icr_filename="dadaab_s03e01.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.DADAAB_S03E01_REASONS,
                       coded_field="rqa_s03e01_coded",
                       analysis_file_key="rqa_s03e01_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.DADAAB_S03E01_REASONS, x, y)
                   )
               ],
               ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s03e01"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s03e02_raw",
               dataset_name="dadaab_s03e02",
               time_field="sent_on",
               run_id_field="rqa_s03e02_run_id",
               coda_filename="dadaab_s03e02.json",
               icr_filename="dadaab_s03e02.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.DADAAB_S03E02_REASONS,
                       coded_field="rqa_s03e02_coded",
                       analysis_file_key="rqa_s03e02_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.DADAAB_S03E02_REASONS, x, y)
                   )
               ],
               ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s03e02"),
               raw_field_fold_strategy=FoldStrategies.concatenate),


    CodingPlan(raw_field="rqa_s03e03_raw",
               dataset_name="dadaab_s03e03",
               time_field="sent_on",
               run_id_field="rqa_s03e03_run_id",
               coda_filename="dadaab_s03e03.json",
               icr_filename="dadaab_s03e03.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.DADAAB_S03E03_REASONS,
                       coded_field="rqa_s03e03_coded",
                       analysis_file_key="rqa_s03e03_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.DADAAB_S03E03_REASONS, x, y)
                   )
               ],
               ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s03e03"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s03e04_raw",
               dataset_name="dadaab_s03e04",
               time_field="sent_on",
               run_id_field="rqa_s03e04_run_id",
               coda_filename="dadaab_s03e04.json",
               icr_filename="dadaab_s03e04.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.DADAAB_S03E04_REASONS,
                       coded_field="rqa_s03e04_coded",
                       analysis_file_key="rqa_s03e04_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.DADAAB_S03E04_REASONS, x, y)
                   )
               ],
               ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s03e04"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s03e05_raw",
               dataset_name="dadaab_s03e05",
               time_field="sent_on",
               run_id_field="rqa_s03e05_run_id",
               coda_filename="dadaab_s03e05.json",
               icr_filename="dadaab_s03e05.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.DADAAB_S03E05_REASONS,
                       coded_field="rqa_s03e05_coded",
                       analysis_file_key="rqa_s03e05_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.DADAAB_S03E05_REASONS, x, y)
                   )
               ],
               ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab s03e05"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="expectations_from_educated_girls_raw",
                   dataset_name="dadaab_expectations_from_educated_girls",
                   listening_group_filename="dadaab_expectations_from_educated_girls.csv",
                   time_field="sent_on",
                   run_id_field="expectations_from_educated_girls_run_id",
                   coda_filename="dadaab_expectations_from_educated_girls.json",
                   icr_filename="dadaab_expectations_from_educated_girls.csv",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.DADAAB_EXPECTATIONS_FROM_EDUCATED_GIRLS,
                           coded_field="expectations_from_educated_girls_coded",
                           analysis_file_key="expectations_from_educated_girls_",
                           fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                               CodeSchemes.DADAAB_EXPECTATIONS_FROM_EDUCATED_GIRLS, x, y)
                       )
                   ],
                   ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab expectations_from_educated_girls"),
                   raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="prevention_of_sgbv_cases_raw",
                               dataset_name="dadaab_prevention_of_sgbv_cases",
                               listening_group_filename="dadaab_prevention_of_sgbv_cases.csv",
                               time_field="sent_on",
                               run_id_field="prevention_of_sgbv_cases_run_id",
                               coda_filename="dadaab_prevention_of_sgbv_cases.json",
                               icr_filename="dadaab_prevention_of_sgbv_cases.csv",
                               coding_configurations=[
                                   CodingConfiguration(
                                       coding_mode=CodingModes.MULTIPLE,
                                       code_scheme=CodeSchemes.DADAAB_PREVENTION_OF_SGBV_CASES,
                                       coded_field="prevention_of_sgbv_cases_coded",
                                       analysis_file_key="prevention_of_sgbv_cases_",
                                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                           CodeSchemes.DADAAB_PREVENTION_OF_SGBV_CASES, x, y)
                                   )
                               ],
                               ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab prevention_of_sgbv_cases"),
                               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="s03_impact_made_raw",
               dataset_name="s03_dadaab_impact_made",
               time_field="sent_on",
               run_id_field="s03_impact_made_run_id",
               icr_filename="s03_impact_made.csv",
               coda_filename="s03_dadaab_impact_made.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.S03_DADAAB_IMPACT_MADE,
                       coded_field="s03_impact_made",
                       analysis_file_key="s03_impact_made_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.S03_DADAAB_IMPACT_MADE, x, y)
                   )
               ],
               ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                   "s03 dadaab impact made"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="s03_lessons_learnt_raw",
               dataset_name="s03_dadaab_lessons_learnt",
               time_field="sent_on",
               icr_filename="s03_lessons_learnt.csv",
               run_id_field="s03_lessons_learnt_run_id",
               coda_filename="s03_dadaab_lessons_learnt.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.S03_DADAAB_LESSONS_LEARNT,
                       coded_field="s03_lessons_learnt",
                       analysis_file_key="s03_lessons_learnt_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.S03_DADAAB_LESSONS_LEARNT, x, y)
                   )
               ],
               ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                   "s03 dadaab lessons learnt"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="s03_close_out_raw",
                       dataset_name="s03_close_out",
                       time_field="sent_on",
                       run_id_field="s03_close_out_run_id",
                       icr_filename="s03_close_out.csv",
                       coda_filename="dadaab_s03_close_out.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_S03_CLOSE_OUTS,
                               coded_field="s03_close_out",
                               analysis_file_key="s03_close_out_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_S03_CLOSE_OUTS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                           "dadaab s03 close out"),
                       raw_field_fold_strategy=FoldStrategies.concatenate)
]

S01_KAKUMA_RQA_CODING_PLANS = [
            CodingPlan(raw_field="rqa_s01e01_raw",
                       dataset_name="kakuma_s01e01",
                       listening_group_filename="kakuma_s01e01_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e01_run_id",
                       coda_filename="kakuma_s01e01.json",
                       icr_filename="kakuma_s01e01.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S01E01_REASONS,
                               coded_field="rqa_s01e01_coded",
                               analysis_file_key="rqa_s01e01_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S01E01_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s01e01"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e02_raw",
                       dataset_name="kakuma_s01e02",
                       listening_group_filename="kakuma_s01e02_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e02_run_id",
                       coda_filename="kakuma_s01e02.json",
                       icr_filename="kakuma_s01e02.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S01E02_REASONS,
                               coded_field="rqa_s01e02_coded",
                               analysis_file_key="rqa_s01e02_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S01E02_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s01e02"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e03_raw",
                       dataset_name="kakuma_s01e03",
                       listening_group_filename="kakuma_s01e03_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e03_run_id",
                       coda_filename="kakuma_s01e03.json",
                       icr_filename="kakuma_s01e03.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S01E03_REASONS,
                               coded_field="rqa_s01e03_coded",
                               analysis_file_key="rqa_s01e03_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S01E03_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s01e03"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e04_raw",
                       dataset_name="kakuma_s01e04",
                       listening_group_filename="kakuma_s01e04_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e04_run_id",
                       coda_filename="kakuma_s01e04.json",
                       icr_filename="kakuma_s01e04.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S01E04_REASONS,
                               coded_field="rqa_s01e04_coded",
                               analysis_file_key="rqa_s01e04_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S01E04_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s01e04"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e05_raw",
                       dataset_name="kakuma_s01e05",
                       listening_group_filename="kakuma_s01e05_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e05_run_id",
                       coda_filename="kakuma_s01e05.json",
                       icr_filename="kakuma_s01e05.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S01E05_REASONS,
                               coded_field="rqa_s01e05_coded",
                               analysis_file_key="rqa_s01e05_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S01E05_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s01e05"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e06_raw",
                       dataset_name="kakuma_s01e06",
                       listening_group_filename="kakuma_s01e06_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e06_run_id",
                       coda_filename="kakuma_s01e06.json",
                       icr_filename="kakuma_s01e06.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S01E06_REASONS,
                               coded_field="rqa_s01e06_coded",
                               analysis_file_key="rqa_s01e06_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S01E06_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s01e06"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01e07_raw",
                       dataset_name="kakuma_s01e07",
                       listening_group_filename="kakuma_s01e07_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s01e07_run_id",
                       coda_filename="kakuma_s01e07.json",
                       icr_filename="kakuma_s01e07.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S01E07_REASONS,
                               coded_field="rqa_s01e07_coded",
                               analysis_file_key="rqa_s01e07_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S01E07_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s01e07"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s01_intro_raw",
                       dataset_name="kakuma_s01_intro",
                       time_field="sent_on",
                       run_id_field="rqa_s01_intro_run_id",
                       coda_filename="kakuma_s01_intro.json",
                       icr_filename="kakuma_s01_intro.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S01_INTRO_REASONS,
                               coded_field="rqa_s01_intro_coded",
                               analysis_file_key="rqa_s01_intro_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S01_INTRO_REASONS,
                                   x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s01 intro"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),
    ]

S02_KAKUMA_RQA_CODING_PLANS  = [

            CodingPlan(raw_field="community_views_on_girls_education_raw",
                       dataset_name="kakuma_community_views_on_girls_education",
                       time_field="sent_on",
                       coda_filename="kakuma_community_views_on_girls_education.json",
                       run_id_field="community_views_on_girls_education_run_id",
                       icr_filename="kakuma_community_views_on_girls_education.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION,
                               coded_field="community_views_on_girls_education_coded",
                               analysis_file_key="community_views_on_girls_education_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                           "kakuma community views on girls education"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="community_views_on_girls_education_final_raw",
                                   dataset_name="kakuma_community_views_on_girls_education_final",
                                   time_field="sent_on",
                                   coda_filename="kakuma_community_views_on_girls_education_final.json",
                                   run_id_field="community_views_on_girls_education_final_run_id",
                                   icr_filename="kakuma_community_views_on_girls_education_final.csv",
                                   coding_configurations=[
                                       CodingConfiguration(
                                           coding_mode=CodingModes.MULTIPLE,
                                           code_scheme=CodeSchemes.KAKUMA_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION_FINAL,
                                           coded_field="community_views_on_girls_education_final_coded",
                                           analysis_file_key="community_views_on_girls_education_final_",
                                           fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                               CodeSchemes.KAKUMA_COMMUNITY_VIEWS_ON_GIRLS_EDUCATION_FINAL, x, y)
                                       )
                                   ],
                                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                                       "kakuma community views on girls education final"),
                                   raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e01_raw",
                       dataset_name="kakuma_s02e01",
                       listening_group_filename="kakuma_s02e01_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s02e01_run_id",
                       coda_filename="kakuma_s02e01.json",
                       icr_filename="kakuma_s02e01.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S02E01_REASONS,
                               coded_field="rqa_s02e01_coded",
                               analysis_file_key="rqa_s02e01_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S02E01_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s02e01"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e02_raw",
                       dataset_name="kakuma_s02e02",
                       listening_group_filename="kakuma_s02e02_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s02e02_run_id",
                       coda_filename="kakuma_s02e02.json",
                       icr_filename="kakuma_s02e02.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S02E02_REASONS,
                               coded_field="rqa_s02e02_coded",
                               analysis_file_key="rqa_s02e02_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S02E02_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s02e02"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e03_raw",
                       dataset_name="kakuma_s02e03",
                       listening_group_filename="kakuma_s02e03_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s02e03_run_id",
                       coda_filename="kakuma_s02e03.json",
                       icr_filename="kakuma_s02e03.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S02E03_REASONS,
                               coded_field="rqa_s02e03_coded",
                               analysis_file_key="rqa_s02e03_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S02E03_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s02e03"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e04_raw",
                       dataset_name="kakuma_s02e04",
                       listening_group_filename="kakuma_s02e04_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s02e04_run_id",
                       coda_filename="kakuma_s02e04.json",
                       icr_filename="kakuma_s02e04.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S02E04_REASONS,
                               coded_field="rqa_s02e04_coded",
                               analysis_file_key="rqa_s02e04_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S02E04_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s02e04"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e05_raw",
                       dataset_name="kakuma_s02e05",
                       listening_group_filename="kakuma_s02e05_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s02e05_run_id",
                       coda_filename="kakuma_s02e05.json",
                       icr_filename="kakuma_s02e05.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S02E05_REASONS,
                               coded_field="rqa_s02e05_coded",
                               analysis_file_key="rqa_s02e05_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S02E05_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s02e05"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e06_raw",
                       dataset_name="kakuma_s02e06",
                       listening_group_filename="kakuma_s02e06_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s02e06_run_id",
                       coda_filename="kakuma_s02e06.json",
                       icr_filename="kakuma_s02e06.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S02E06_REASONS,
                               coded_field="rqa_s02e06_coded",
                               analysis_file_key="rqa_s02e06_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S02E06_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s02e06"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e07_raw",
                       dataset_name="kakuma_s02e07",
                       listening_group_filename="kakuma_s02e07_listening_group.csv",
                       time_field="sent_on",
                       run_id_field="rqa_s02e07_run_id",
                       coda_filename="kakuma_s02e07.json",
                       icr_filename="kakuma_s02e07.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_S02E07_REASONS,
                               coded_field="rqa_s02e07_coded",
                               analysis_file_key="rqa_s02e07_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_S02E07_REASONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s02e07"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s02e08_raw",
                                   dataset_name="kakuma_s02e08",
                                   listening_group_filename="kakuma_s02e08_listening_group.csv",
                                   time_field="sent_on",
                                   run_id_field="rqa_s02e08_run_id",
                                   coda_filename="kakuma_s02e08.json",
                                   icr_filename="kakuma_s02e08.csv",
                                   coding_configurations=[
                                       CodingConfiguration(
                                           coding_mode=CodingModes.MULTIPLE,
                                           code_scheme=CodeSchemes.KAKUMA_S02E08_REASONS,
                                           coded_field="rqa_s02e08_coded",
                                           analysis_file_key="rqa_s02e08_",
                                           fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                               CodeSchemes.KAKUMA_S02E08_REASONS, x, y)
                                       )
                                   ],
                                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s02e08"),
                                   raw_field_fold_strategy=FoldStrategies.concatenate),
        ]

S03_KAKUMA_RQA_CODING_PLANS = [
    CodingPlan(raw_field="rqa_s03e01_raw",
               dataset_name="kakuma_s03e01",
               listening_group_filename="kakuma_s03e01_listening_group.csv",
               time_field="sent_on",
               run_id_field="rqa_s03e01_run_id",
               coda_filename="kakuma_s03e01.json",
               icr_filename="kakuma_s03e01.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KAKUMA_S03E01_REASONS,
                       coded_field="rqa_s03e01_coded",
                       analysis_file_key="rqa_s03e01_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.KAKUMA_S03E01_REASONS, x, y)
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s03e01"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s03e02_raw",
               dataset_name="kakuma_s03e02",
               listening_group_filename="kakuma_s03e02_listening_group.csv",
               time_field="sent_on",
               run_id_field="rqa_s03e02_run_id",
               coda_filename="kakuma_s03e02.json",
               icr_filename="kakuma_s03e02.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KAKUMA_S03E02_REASONS,
                       coded_field="rqa_s03e02_coded",
                       analysis_file_key="rqa_s03e02_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.KAKUMA_S03E02_REASONS, x, y)
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s03e02"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s03e03_raw",
               dataset_name="kakuma_s03e03",
               listening_group_filename="kakuma_s03e03_listening_group.csv",
               time_field="sent_on",
               run_id_field="rqa_s03e03_run_id",
               coda_filename="kakuma_s03e03.json",
               icr_filename="kakuma_s03e03.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KAKUMA_S03E03_REASONS,
                       coded_field="rqa_s03e03_coded",
                       analysis_file_key="rqa_s03e03_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.KAKUMA_S03E03_REASONS, x, y)
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s03e03"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s03e04_raw",
               dataset_name="kakuma_s03e04",
               listening_group_filename="kakuma_s03e04_listening_group.csv",
               time_field="sent_on",
               run_id_field="rqa_s03e04_run_id",
               coda_filename="kakuma_s03e04.json",
               icr_filename="kakuma_s03e04.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KAKUMA_S03E04_REASONS,
                       coded_field="rqa_s03e04_coded",
                       analysis_file_key="rqa_s03e04_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.KAKUMA_S03E04_REASONS, x, y)
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s03e04"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="rqa_s03e05_raw",
               dataset_name="kakuma_s03e05",
               listening_group_filename="kakuma_s03e05_listening_group.csv",
               time_field="sent_on",
               run_id_field="rqa_s03e05_run_id",
               coda_filename="kakuma_s03e05.json",
               icr_filename="kakuma_s03e05.csv",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KAKUMA_S03E05_REASONS,
                       coded_field="rqa_s03e05_coded",
                       analysis_file_key="rqa_s03e05_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.KAKUMA_S03E05_REASONS, x, y)
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma s03e05"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="expectations_from_educated_girls_raw",
                   dataset_name="kakuma_expectations_from_educated_girls",
                   listening_group_filename="kakuma_expectations_from_educated_girls.csv",
                   time_field="sent_on",
                   run_id_field="expectations_from_educated_girls_run_id",
                   coda_filename="kakuma_expectations_from_educated_girls.json",
                   icr_filename="kakuma_expectations_from_educated_girls.csv",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.KAKUMA_EXPECTATIONS_FROM_EDUCATED_GIRLS,
                           coded_field="expectations_from_educated_girls_coded",
                           analysis_file_key="expectations_from_educated_girls_",
                           fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                               CodeSchemes.KAKUMA_EXPECTATIONS_FROM_EDUCATED_GIRLS, x, y)
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma expectations_from_educated_girls"),
                   raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="prevention_of_sgbv_cases_raw",
                       dataset_name="kakuma_prevention_of_sgbv_cases",
                       listening_group_filename="kakuma_prevention_of_sgbv_cases.csv",
                       time_field="sent_on",
                       run_id_field="prevention_of_sgbv_cases_run_id",
                       coda_filename="kakuma_prevention_of_sgbv_cases.json",
                       icr_filename="kakuma_prevention_of_sgbv_cases.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_PREVENTION_OF_SGBV_CASES,
                               coded_field="prevention_of_sgbv_cases_coded",
                               analysis_file_key="prevention_of_sgbv_cases_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_PREVENTION_OF_SGBV_CASES, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma prevention_of_sgbv_cases"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="s03_impact_made_raw",
               dataset_name="s03_kakuma_impact_made",
               time_field="sent_on",
               run_id_field="s03_impact_made_run_id",
               icr_filename="s03_impact_made.csv",
               coda_filename="s03_kakuma_impact_made.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.S03_KAKUMA_IMPACT_MADE,
                       coded_field="s03_impact_made",
                       analysis_file_key="s03_impact_made_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.S03_KAKUMA_IMPACT_MADE, x, y)
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "s03 kakuma impact made"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="s03_lessons_learnt_raw",
               dataset_name="s03_kakuma_lessons_learnt",
               time_field="sent_on",
               run_id_field="s03_lessons_learnt_run_id",
               icr_filename="s03_lessons_learnt.csv",
               coda_filename="s03_kakuma_lessons_learnt.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.S03_KAKUMA_LESSONS_LEARNT,
                       coded_field="s03_lessons_learnt",
                       analysis_file_key="s03_lessons_learnt_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.S03_KAKUMA_LESSONS_LEARNT, x, y)
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "s03 kakuma lessons learnt"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="s03_close_out_raw",
                   dataset_name="s03_close_out",
                   time_field="sent_on",
                   run_id_field="s03_close_out_run_id",
                   icr_filename="s03_close_out.csv",
                   coda_filename="kakuma_s03_close_out.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.KAKUMA_S03_CLOSE_OUTS,
                           coded_field="s03_close_out",
                           analysis_file_key="s03_close_out_",
                           fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                               CodeSchemes.KAKUMA_S03_CLOSE_OUTS, x, y)
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                       "kakuma s03 close out"),
                   raw_field_fold_strategy=FoldStrategies.concatenate)
]

def get_rqa_coding_plans(pipeline_name):
    if pipeline_name == "dadaab_s01_pipeline":
        return S01_DADAAB_RQA_CODING_PLANS

    elif pipeline_name == "dadaab_s02_pipeline":
        return S02_DADAAB_RQA_CODING_PLANS

    elif pipeline_name == "dadaab_s03_pipeline":
        return S03_DADAAB_RQA_CODING_PLANS

    elif pipeline_name == "dadaab_all_seasons_pipeline":
        return S01_DADAAB_RQA_CODING_PLANS + S02_DADAAB_RQA_CODING_PLANS + S03_DADAAB_RQA_CODING_PLANS

    elif pipeline_name == "kakuma_s01_pipeline":
        return S01_KAKUMA_RQA_CODING_PLANS

    elif pipeline_name == "kakuma_s02_pipeline":
        return S02_KAKUMA_RQA_CODING_PLANS

    elif pipeline_name == "kakuma_s03_pipeline":
        return S03_KAKUMA_RQA_CODING_PLANS

    else:
        assert pipeline_name == "kakuma_all_seasons_pipeline", "PipelineName must be either a " \
                                "'seasonal pipeline' or 'all seasons pipeline'"
        return S01_KAKUMA_RQA_CODING_PLANS + S02_KAKUMA_RQA_CODING_PLANS + S03_KAKUMA_RQA_CODING_PLANS

DADAAB_DEMOGS_CODING_PLAN = [
        CodingPlan(raw_field="location_raw",
                   dataset_name="dadaab_location",
                   time_field="location_time",
                   coda_filename="dadaab_location.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.DADAAB_LOCATION,
                           coded_field="location_coded",
                           analysis_file_key="location",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       ),
                   ],
                   ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab location"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="gender_raw",
                   dataset_name="dadaab_gender",
                   time_field="gender_time",
                   coda_filename="dadaab_gender.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.GENDER,
                           cleaner=somali.DemographicCleaner.clean_gender,
                           coded_field="gender_coded",
                           analysis_file_key="gender",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       )
                   ],
                   ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab gender"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="age_raw",
                   dataset_name="dadaab_age",
                   time_field="age_time",
                   coda_filename="dadaab_age.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.AGE,
                           cleaner=lambda text: clean_age_with_range_filter(text),
                           coded_field="age_coded",
                           analysis_file_key="age",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.AGE_CATEGORY,
                           coded_field="age_category_coded",
                           analysis_file_key="age_category",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       )
                   ],
                   code_imputation_function=code_imputation_functions.impute_age_category,
                   ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab age"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="household_language_raw",
                   dataset_name="dadaab_household_language",
                   time_field="household_language_time",
                   coda_filename="dadaab_household_language.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.DADAAB_HOUSEHOLD_LANGUAGE,
                           coded_field="household_language_coded",
                           analysis_file_key="household_language",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       )
                   ],
                   ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab household language"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="nationality_raw",
                   dataset_name="dadaab_nationality",
                   time_field="nationality_time",
                   coda_filename="dadaab_nationality.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.NATIONALITY,
                           coded_field="nationality_coded",
                           analysis_file_key="nationality",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       )
                   ],
                   ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab nationality"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="disabled_raw",
                           dataset_name="dadaab_disabled",
                           time_field="disabled_time",
                           coda_filename="dadaab_disabled.json",
                           coding_configurations=[
                               CodingConfiguration(
                                   coding_mode=CodingModes.SINGLE,
                                   code_scheme=CodeSchemes.DISABLED,
                                   coded_field="disabled_coded",
                                   analysis_file_key="disabled",
                                   fold_strategy=FoldStrategies.assert_label_ids_equal,
                               )
                           ],
                           ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value("dadaab disabled"),
                           raw_field_fold_strategy=FoldStrategies.assert_equal)
    ]

KAKUMA_DEMOG_CODING_PLANS = [
        CodingPlan(raw_field="location_raw",
                   dataset_name="kakuma_location",
                   time_field="location_time",
                   coda_filename="kakuma_location.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.KAKUMA_LOCATION,
                           coded_field="location_coded",
                           analysis_file_key="location",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       ),
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma location"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="gender_raw",
                   dataset_name="kakuma_gender",
                   time_field="gender_time",
                   coda_filename="kakuma_gender.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.GENDER,
                           cleaner=somali.DemographicCleaner.clean_gender,
                           coded_field="gender_coded",
                           analysis_file_key="gender",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma gender"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="age_raw",
                   dataset_name="kakuma_age",
                   time_field="age_time",
                   coda_filename="kakuma_age.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.AGE,
                           cleaner=lambda text: clean_age_with_range_filter(text),
                           coded_field="age_coded",
                           analysis_file_key="age",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                           include_in_theme_distribution=False
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.AGE_CATEGORY,
                           coded_field="age_category_coded",
                           analysis_file_key="age_category",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       )
                   ],
                   code_imputation_function=code_imputation_functions.impute_age_category,
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma age"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="household_language_raw",
                   dataset_name="kakuma_household_language",
                   time_field="household_language_time",
                   coda_filename="kakuma_household_language.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE,
                           coded_field="household_language_coded",
                           analysis_file_key="household_language",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma household language"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="nationality_raw",
                   dataset_name="kakuma_nationality",
                   time_field="nationality_time",
                   coda_filename="kakuma_nationality.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.NATIONALITY,
                           coded_field="nationality_coded",
                           analysis_file_key="nationality",
                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma nationality"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(raw_field="disabled_raw",
                                   dataset_name="kakuma_disabled",
                                   time_field="disabled_time",
                                   coda_filename="kakuma_disabled.json",
                                   coding_configurations=[
                                       CodingConfiguration(
                                           coding_mode=CodingModes.SINGLE,
                                           code_scheme=CodeSchemes.DISABLED,
                                           coded_field="disabled_coded",
                                           analysis_file_key="disabled",
                                           fold_strategy=FoldStrategies.assert_label_ids_equal,
                                       )
                                   ],
                                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value("kakuma disabled"),
                                   raw_field_fold_strategy=FoldStrategies.assert_equal),
        ]


def get_demog_coding_plans(pipeline_name):

    if pipeline_name in ["dadaab_s01_pipeline", "dadaab_s02_pipeline", "dadaab_s03_pipeline", "dadaab_all_seasons_pipeline"]:
        return DADAAB_DEMOGS_CODING_PLAN
    else:
        assert pipeline_name in ["kakuma_s01_pipeline", "kakuma_s02_pipeline", "kakuma_s03_pipeline", "kakuma_all_seasons_pipeline"],\
            "PipelineName must be either a 'seasonal pipeline' or 'all seasons pipeline'"
        return KAKUMA_DEMOG_CODING_PLANS

S01_DADAAB_FOLLOW_UP_CODING_PLANS = [
            CodingPlan(raw_field="girls_education_champions_raw",
                       dataset_name="dadaab_girls_education_champions",
                       time_field="girls_education_champions_time",
                       coda_filename="dadaab_girls_education_champions.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_GIRLS_EDUCATION_CHAMPIONS,
                               coded_field="girls_education_champions_coded",
                               analysis_file_key="girls_education_champions_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_GIRLS_EDUCATION_CHAMPIONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                           "dadaab girls education champions"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="encouragement_for_boys_raw",
                       dataset_name="dadaab_encouragement_for_boys",
                       time_field="encouragement_for_boys_time",
                       coda_filename="dadaab_encouragement_for_boys.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_ENCOURAGEMENT_FOR_BOYS_CHAMPIONS,
                               coded_field="encouragement_for_boys",
                               analysis_file_key="encouragement_for_boys_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_ENCOURAGEMENT_FOR_BOYS_CHAMPIONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                           "dadaab encouragement for boys"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="unmarried_fathers_community_view_raw",
                       dataset_name="dadaab_unmarried_fathers_community_view",
                       time_field="unmarried_fathers_community_view_time",
                       coda_filename="dadaab_unmarried_fathers_community_view.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_UNMARRIED_FATHERS_COMMUNITY_VIEW,
                               coded_field="girls_unmarried_fathers_community_view",
                               analysis_file_key="girls_unmarried_fathers_community_view_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_UNMARRIED_FATHERS_COMMUNITY_VIEW, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                           "dadaab unmarried fathers community view"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="lessons_learnt_raw",
                       dataset_name="dadaab_lessons_learnt",
                       time_field="lessons_learnt_time",
                       coda_filename="dadaab_lessons_learnt.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_LESSONS_LEARNT,
                               coded_field="lessons_learnt",
                               analysis_file_key="lessons_learnt_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_LESSONS_LEARNT,
                                   x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                           "dadaab lessons learnt"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="show_suggestions_raw",
                       dataset_name="dadaab_show_suggestions",
                       time_field="show_suggestions_time",
                       coda_filename="dadaab_show_suggestions.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.DADAAB_SHOW_SUGGESTIONS,
                               coded_field="show_suggestions",
                               analysis_file_key="show_suggestions_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.DADAAB_SHOW_SUGGESTIONS,
                                   x, y)
                           )
                       ],
                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                           "dadaab show suggestions"),
                       raw_field_fold_strategy=FoldStrategies.concatenate)
        ]

S02_DADAAB_FOLLOW_UP_CODING_PLANS = [
            CodingPlan(raw_field="responses_to_sexual_violence_raw",
                           dataset_name="dadaab_responses_to_sexual_violence",
                           time_field="responses_to_sexual_violence_time",
                           coda_filename="dadaab_responses_to_sexual_violence.json",
                           coding_configurations=[
                               CodingConfiguration(
                                   coding_mode=CodingModes.MULTIPLE,
                                   code_scheme=CodeSchemes.DADAAB_RESPONSES_TO_SEXUAL_VIOLENCE,
                                   coded_field="girls_responses_to_sexual_violence",
                                   analysis_file_key="girls_responses_to_sexual_violence_",
                                   fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                       CodeSchemes.DADAAB_RESPONSES_TO_SEXUAL_VIOLENCE, x, y)
                               )
                           ],
                           ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                               "dadaab responses to sexual violence"),
                           raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="adolescent_mothers_challenges_raw",
                           dataset_name="dadaab_adolescent_mothers_challenges",
                           time_field="adolescent_mothers_challenges_time",
                           coda_filename="dadaab_adolescent_mothers_challenges.json",
                           coding_configurations=[
                               CodingConfiguration(
                                   coding_mode=CodingModes.MULTIPLE,
                                   code_scheme=CodeSchemes.DADAAB_ADOLESCENT_MOTHERS_CHALLENGES,
                                   coded_field="adolescent_mothers_challenges",
                                   analysis_file_key="adolescent_mothers_challenges_",
                                   fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                       CodeSchemes.DADAAB_ADOLESCENT_MOTHERS_CHALLENGES, x, y)
                               )
                           ],
                           ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                               "dadaab adolescent mothers challenges"),
                           raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="s02_impact_made_raw",
                           dataset_name="s02_dadaab_impact_made",
                           time_field="s02_impact_made_time",
                           coda_filename="s02_dadaab_impact_made.json",
                           coding_configurations=[
                               CodingConfiguration(
                                   coding_mode=CodingModes.MULTIPLE,
                                   code_scheme=CodeSchemes.S02_DADAAB_IMPACT_MADE,
                                   coded_field="s02_impact_made",
                                   analysis_file_key="s02_impact_made_",
                                   fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                       CodeSchemes.S02_DADAAB_IMPACT_MADE, x, y)
                               )
                           ],
                           ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                               "s02 dadaab impact made"),
                           raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="s02_lessons_learnt_raw",
                                       dataset_name="s02_dadaab_lessons_learnt",
                                       time_field="s02_lessons_learnt_time",
                                       coda_filename="s02_dadaab_lessons_learnt.json",
                                       coding_configurations=[
                                           CodingConfiguration(
                                               coding_mode=CodingModes.MULTIPLE,
                                               code_scheme=CodeSchemes.S02_DADAAB_LESSONS_LEARNT,
                                               coded_field="s02_lessons_learnt",
                                               analysis_file_key="s02_lessons_learnt_",
                                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                                   CodeSchemes.S02_DADAAB_LESSONS_LEARNT, x, y)
                                           )
                                       ],
                                       ws_code=CodeSchemes.DADAAB_WS_CORRECT_DATASET.get_code_with_match_value(
                                           "s02 dadaab lessons learnt"),
                           raw_field_fold_strategy=FoldStrategies.concatenate),
        ]

S01_KAKUMA_FOLLOW_UP_CODING_PLANS = [
            CodingPlan(raw_field="girls_education_champions_raw",
                       dataset_name="kakuma_girls_education_champions",
                       time_field="girls_education_champions_time",
                       coda_filename="kakuma_girls_education_champions.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_GIRLS_EDUCATION_CHAMPIONS,
                               coded_field="girls_education_champions_coded",
                               analysis_file_key="girls_education_champions_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_GIRLS_EDUCATION_CHAMPIONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                           "kakuma girls education champions"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="encouragement_for_boys_raw",
                       dataset_name="kakuma_encouragement_for_boys",
                       time_field="encouragement_for_boys_time",
                       coda_filename="kakuma_encouragement_for_boys.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_ENCOURAGEMENT_FOR_BOYS_CHAMPIONS,
                               coded_field="encouragement_for_boys",
                               analysis_file_key="encouragement_for_boys_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_ENCOURAGEMENT_FOR_BOYS_CHAMPIONS, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                           "kakuma encouragement for boys"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="unmarried_fathers_community_view_raw",
                       dataset_name="kakuma_unmarried_fathers_community_view",
                       time_field="unmarried_fathers_community_view_time",
                       coda_filename="kakuma_unmarried_fathers_community_view.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_UNMARRIED_FATHERS_COMMUNITY_VIEW,
                               coded_field="unmarried_fathers_community_view",
                               analysis_file_key="unmarried_fathers_community_view_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_UNMARRIED_FATHERS_COMMUNITY_VIEW, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                           "kakuma unmarried fathers community view"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="lessons_learnt_raw",
                       dataset_name="kakuma_lessons_learnt",
                       time_field="lessons_learnt_time",
                       coda_filename="kakuma_lessons_learnt.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_LESSONS_LEARNT,
                               coded_field="lessons_learnt",
                               analysis_file_key="lessons_learnt_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_LESSONS_LEARNT,
                                   x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                           "kakuma lessons learnt"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="show_suggestions_raw",
                       dataset_name="kakuma_show_suggestions",
                       time_field="show_suggestions_time",
                       coda_filename="kakuma_show_suggestions.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.KAKUMA_SHOW_SUGGESTIONS,
                               coded_field="show_suggestions",
                               analysis_file_key="show_suggestions_",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                   CodeSchemes.KAKUMA_SHOW_SUGGESTIONS,
                                   x, y)
                           )
                       ],
                       ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                           "kakuma show suggestions"),
                       raw_field_fold_strategy=FoldStrategies.concatenate)
        ]
S03_DADAAB_FOLLOW_UP_CODING_PLANS = [

]
S02_KAKUMA_FOLLOW_UP_CODING_PLANS = [
    CodingPlan(raw_field="responses_to_sexual_violence_raw",
               dataset_name="kakuma_responses_to_sexual_violence",
               time_field="responses_to_sexual_violence_time",
               coda_filename="kakuma_responses_to_sexual_violence.json",
               coding_configurations=[
                   CodingConfiguration(
                       coding_mode=CodingModes.MULTIPLE,
                       code_scheme=CodeSchemes.KAKUMA_RESPONSES_TO_SEXUAL_VIOLENCE,
                       coded_field="girls_responses_to_sexual_violence",
                       analysis_file_key="girls_responses_to_sexual_violence_",
                       fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                           CodeSchemes.KAKUMA_RESPONSES_TO_SEXUAL_VIOLENCE, x, y)
                   )
               ],
               ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                   "kakuma responses to sexual violence"),
               raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="adolescent_mothers_challenges_raw",
                   dataset_name="kakuma_adolescent_mothers_challenges",
                   time_field="adolescent_mothers_challenges_time",
                   coda_filename="kakuma_adolescent_mothers_challenges.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.KAKUMA_ADOLESCENT_MOTHERS_CHALLENGES,
                           coded_field="adolescent_mothers_challenges",
                           analysis_file_key="adolescent_mothers_challenges_",
                           fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                               CodeSchemes.KAKUMA_ADOLESCENT_MOTHERS_CHALLENGES, x, y)
                       )
                   ],
                   ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                       "kakuma adolescent mothers challenges"),
                   raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="s02_impact_made_raw",
                                           dataset_name="s02_kakuma_impact_made",
                                           time_field="s02_impact_made_time",
                                           coda_filename="s02_kakuma_impact_made.json",
                                           coding_configurations=[
                                               CodingConfiguration(
                                                   coding_mode=CodingModes.MULTIPLE,
                                                   code_scheme=CodeSchemes.S02_KAKUMA_IMPACT_MADE,
                                                   coded_field="s02_impact_made",
                                                   analysis_file_key="s02_impact_made_",
                                                   fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                                       CodeSchemes.S02_KAKUMA_IMPACT_MADE, x, y)
                                               )
                                           ],
                                           ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                                               "s02 kakuma impact made"),
                                           raw_field_fold_strategy=FoldStrategies.concatenate),

    CodingPlan(raw_field="s02_lessons_learnt_raw",
                                           dataset_name="s02_kakuma_lessons_learnt",
                                           time_field="s02_lessons_learnt_time",
                                           coda_filename="s02_kakuma_lessons_learnt.json",
                                           coding_configurations=[
                                               CodingConfiguration(
                                                   coding_mode=CodingModes.MULTIPLE,
                                                   code_scheme=CodeSchemes.S02_KAKUMA_LESSONS_LEARNT,
                                                   coded_field="s02_lessons_learnt",
                                                   analysis_file_key="s02_lessons_learnt_",
                                                   fold_strategy=lambda x, y: FoldStrategies.list_of_labels(
                                                       CodeSchemes.S02_KAKUMA_LESSONS_LEARNT, x, y)
                                               )
                                           ],
                                           ws_code=CodeSchemes.KAKUMA_WS_CORRECT_DATASET.get_code_with_match_value(
                                               "s02 kakuma lessons learnt"),
                               raw_field_fold_strategy=FoldStrategies.concatenate),
]

S03_KAKUMA_FOLLOW_UP_CODING_PLANS = [

]


def get_follow_up_coding_plans(pipeline_name):
    if pipeline_name == "dadaab_s01_pipeline":
        return S01_DADAAB_FOLLOW_UP_CODING_PLANS

    elif pipeline_name == "dadaab_s02_pipeline":
        return S02_DADAAB_FOLLOW_UP_CODING_PLANS

    elif pipeline_name == "dadaab_s03_pipeline":
        return S03_DADAAB_FOLLOW_UP_CODING_PLANS

    elif pipeline_name == "dadaab_all_seasons_pipeline":
        return S01_DADAAB_FOLLOW_UP_CODING_PLANS + S02_DADAAB_FOLLOW_UP_CODING_PLANS + S03_DADAAB_FOLLOW_UP_CODING_PLANS

    elif pipeline_name == "kakuma_s01_pipeline":
        return S01_KAKUMA_FOLLOW_UP_CODING_PLANS

    elif pipeline_name == "kakuma_s02_pipeline":
        return S02_KAKUMA_FOLLOW_UP_CODING_PLANS

    elif pipeline_name == "kakuma_s03_pipeline":
        return S03_KAKUMA_FOLLOW_UP_CODING_PLANS

    else:
        assert pipeline_name == "kakuma_all_seasons_pipeline", "PipelineName must be either a 'seasonal pipeline' or 'all seasons pipeline'"
        return S01_KAKUMA_FOLLOW_UP_CODING_PLANS + S02_KAKUMA_FOLLOW_UP_CODING_PLANS + S03_KAKUMA_FOLLOW_UP_CODING_PLANS


def get_ws_correct_dataset_scheme(pipeline_name):
    if pipeline_name in ["dadaab_s01_pipeline", "dadaab_s02_pipeline", "dadaab_s03_pipeline", "dadaab_all_seasons_pipeline"]:
        return CodeSchemes.DADAAB_WS_CORRECT_DATASET
    else:
        assert pipeline_name in ["kakuma_s01_pipeline", "kakuma_s02_pipeline", "kakuma_s03_pipeline", "kakuma_all_seasons_pipeline"], \
            "PipelineName must be either a 'seasonal pipeline' or 'all seasons pipeline'"
        return CodeSchemes.KAKUMA_WS_CORRECT_DATASET
