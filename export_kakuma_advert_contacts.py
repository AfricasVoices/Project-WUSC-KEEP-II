import argparse
import csv
import json

from core_data_modules.logging import Logger
from core_data_modules.cleaners import Codes
from core_data_modules.traced_data.io import TracedDataJsonIO
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration
from src.lib.code_schemes import CodeSchemes

Logger.set_project_name("WUSC-KEEP-II")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates lists of phone numbers of WUSC-KEEP-II-KAKUMA participants"
                                                 "based on their manually labeled demographic language response")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to WUSC-KEEP-II-KAKUMA pipeline configuration json file")
    parser.add_argument("individuals_json_input_path", metavar="individuals_json_input_path",
                        help="Path to the WUSC-KEEP-II-KAKUMA individuals traced data JSONL file to extract phone"
                             "numbers from")
    parser.add_argument("contacts_csv_dir", metavar="output-path",
                        help="Dir to write CSVs with per-language contacts to")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    individuals_json_input_path = args.individuals_json_input_path
    contacts_csv_dir = args.contacts_csv_dir

    # Read the settings from the configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
        assert pipeline_configuration.pipeline_name == "kakuma_pipeline", "PipelineName must kakuma_pipeline"

    log.info("Downloading Firestore UUID Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        pipeline_configuration.phone_number_uuid_table.firebase_credentials_file_url
    ))

    phone_number_uuid_table = FirestoreUuidTable(
        pipeline_configuration.phone_number_uuid_table.table_name,
        firestore_uuid_table_credentials,
        "avf-phone-uuid-"
    )
    log.info("Initialised the Firestore UUID table")

    # Read the individuals dataset
    log.info(f"Loading the individuals dataset from {individuals_json_input_path}...")
    with open(individuals_json_input_path) as f:
        individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(individuals)} participants")
        
    # Search the TracedData for uuids for kakuma participants based on their manually labelled household language
    # i.e Oromo/Sudanese-Juba arabic/Somali/Swahili speakers.
    oromo_uuids = set()
    sudanese_juba_arabic_uuids = set()
    turkana_uuids = set()
    somali_uuids = set()
    english_uuids = set()
    swahili_uuids = set()

    log.info(f'Searching for {len(individuals)} participants uuids vis-a-vis` their manually labelled '
             f'demographic language response')
    for ind in individuals:
        if ind["consent_withdrawn"] == Codes.TRUE:
            continue

        if CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                ind["household_language_coded"]["CodeID"]).string_value == "oromo":
            oromo_uuids.add(ind['uid'])
        elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                    ind["household_language_coded"]["CodeID"]).string_value == "sudanese":
                sudanese_juba_arabic_uuids.add(ind['uid'])
        elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                    ind["household_language_coded"]["CodeID"]).string_value == "turkana":
                turkana_uuids.add(ind['uid'])
        elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                    ind["household_language_coded"]["CodeID"]).string_value == "somali":
                somali_uuids.add(ind['uid'])
        elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                    ind["household_language_coded"]["CodeID"]).string_value == "english":
                english_uuids.add(ind['uid'])
        else:
            swahili_uuids.add(ind['uid'])

    # Convert the uuids to phone numbers
    log.info("Converting the uuids to phone numbers...")
    uuids_to_phone_numbers = phone_number_uuid_table.uuid_to_data_batch(
        list(oromo_uuids) + list(sudanese_juba_arabic_uuids) +list(turkana_uuids) + list(somali_uuids) +
        list(english_uuids) + list(swahili_uuids))

    oromo_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in oromo_uuids]
    sudanese_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in sudanese_juba_arabic_uuids]
    turkana_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in turkana_uuids]
    somali_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in somali_uuids]
    english_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in english_uuids]
    swahili_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in swahili_uuids]

    # Export the phone numbers to their respective keep_ii_kakuma_{language} CSVs
    # TODO upload this to keep_ii_kakuma textit instance
    language_based_contacts = [oromo_phone_numbers, sudanese_phone_numbers, turkana_phone_numbers, somali_phone_numbers,
                       english_phone_numbers, swahili_phone_numbers  ]

    def export_numbers_to_csv(contacts_list, contacts_csv_dir, csv_name):
        contacts_list_csv = f'{contacts_csv_dir}/{csv_name}.csv'
        with open(contacts_list_csv, "w") as f:
            writer = csv.DictWriter(f, fieldnames=["URN:Tel", "Name"], lineterminator="\n")
            writer.writeheader()

            for n in contacts_list:
                writer.writerow({
                    "URN:Tel": n
                })
            log.info(f"Wrote {len(contacts_list)} contacts to {contacts_list_csv}")

    log.info(f"Exporting Oromo contacts...")
    export_numbers_to_csv(oromo_phone_numbers, contacts_csv_dir, 'keep_ii_kakuma_oromo')

    log.info(f"Exporting Sudanese/Juba arabic contacts...")
    export_numbers_to_csv(sudanese_phone_numbers, contacts_csv_dir, 'keep_ii_kakuma_sudanese')

    log.info(f"Exporting Turkana contacts...")
    export_numbers_to_csv(turkana_phone_numbers, contacts_csv_dir, 'keep_ii_kakuma_turkana')

    log.info(f"Exporting Somali contacts...")
    export_numbers_to_csv(somali_phone_numbers, contacts_csv_dir, 'keep_ii_kakuma_somali')

    log.info(f"Exporting English contacts...")
    export_numbers_to_csv(english_phone_numbers, contacts_csv_dir, 'keep_ii_kakuma_english')

    log.info(f"Exporting Swahili contacts...")
    export_numbers_to_csv(swahili_phone_numbers, contacts_csv_dir, 'keep_ii_kakuma_swahili')
