import argparse
import csv
import json
from collections import OrderedDict

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
    parser = argparse.ArgumentParser(description="Generates a list of phone numbers and textit language of "
                                                 "WUSC-KEEP-II-KAKUMA participants"
                                                 "based on their manually labeled demographic language response")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to WUSC-KEEP-II-KAKUMA pipeline configuration json file")
    parser.add_argument("messages_json_input_path", metavar="messages_json_input_path",
                        help="Path to the WUSC-KEEP-II-KAKUMA messages traced data JSONL file to extract phone"
                             "numbers from")
    parser.add_argument("contacts_csv_path", metavar="contacts-csv-path",
                        help="CSV file path to write per-language contacts data to")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    messages_json_input_path = args.messages_json_input_path
    contacts_csv_path = args.contacts_csv_path

    # Read the settings from the configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
        assert pipeline_configuration.pipeline_name == "kakuma_pipeline", "PipelineName must be kakuma_pipeline"

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
    log.info(f'Loading the messages dataset from {messages_json_input_path}...')
    with open(messages_json_input_path) as f:
        messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f'Loaded {len(messages)} messages')
        
    # Search the TracedData for uuids for kakuma participants based on their manually labelled household language
    # i.e Oromo/Sudanese-Juba arabic/Somali/Swahili speakers.
    oromo_uuids = set()
    sudanese_juba_arabic_uuids = set()
    turkana_uuids = set()
    somali_uuids = set()
    english_uuids = set()
    swahili_uuids = set()
    all_uids = set()

    log.info(f'Searching for the participants uuids vis-a-vis` their manually labelled '
             f'demographic language response')
    for msg in messages:
        if msg['uid'] in all_uids or msg["consent_withdrawn"] == Codes.TRUE:
            continue

        if CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                msg["household_language_coded"]["CodeID"]).string_value == "oromo":
            oromo_uuids.add(msg['uid'])
            all_uids.add(msg['uid'])
        elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                    msg["household_language_coded"]["CodeID"]).string_value == "sudanese":
                sudanese_juba_arabic_uuids.add(msg['uid'])
                all_uids.add(msg['uid'])
        elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                    msg["household_language_coded"]["CodeID"]).string_value == "turkana":
                turkana_uuids.add(msg['uid'])
                all_uids.add(msg['uid'])
        elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                    msg["household_language_coded"]["CodeID"]).string_value == "somali":
                somali_uuids.add(msg['uid'])
                all_uids.add(msg['uid'])
        elif CodeSchemes.KAKUMA_HOUSEHOLD_LANGUAGE.get_code_with_code_id(
                    msg["household_language_coded"]["CodeID"]).string_value == "english":
                english_uuids.add(msg['uid'])
                all_uids.add(msg['uid'])
        else:
            swahili_uuids.add(msg['uid'])
            all_uids.add(msg['uid'])

    # Convert the uuids to phone numbers
    log.info("Converting the uuids to phone numbers...")
    uuids_to_phone_numbers = phone_number_uuid_table.uuid_to_data_batch(list(all_uids))

    oromo_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in oromo_uuids]
    sudanese_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in sudanese_juba_arabic_uuids]
    turkana_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in turkana_uuids]
    somali_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in somali_uuids]
    english_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in english_uuids]
    swahili_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in swahili_uuids]

    # Export the phone numbers to the advert CSV
    # TODO upload this to keep_ii_kakuma textit instance through API?
    advert_contacts = OrderedDict()
    for contact_list in [oromo_phone_numbers, sudanese_phone_numbers, turkana_phone_numbers, somali_phone_numbers,
                       english_phone_numbers, swahili_phone_numbers]:

        if contact_list == oromo_phone_numbers:
            for phone_number in contact_list:
                advert_contacts[phone_number] = {
                    "URN:Tel": phone_number,
                    "Name": None,
                    "Language": 'orm'
                }
        elif contact_list == sudanese_phone_numbers:
            for phone_number in contact_list:
                advert_contacts[phone_number] = {
                    "URN:Tel": phone_number,
                    "Name": None,
                    "Language": 'apd'
                }
        elif contact_list == turkana_phone_numbers:
            for phone_number in contact_list:
                advert_contacts[phone_number] = {
                    "URN:Tel": phone_number,
                    "Name": None,
                    "Language": 'tuv'
                }
        elif contact_list == somali_phone_numbers:
            for phone_number in contact_list:
                advert_contacts[phone_number] = {
                    "URN:Tel": phone_number,
                    "Name": None,
                    "Language": 'som'
                }
        elif contact_list == english_phone_numbers:
            for phone_number in contact_list:
                advert_contacts[phone_number] = {
                    "URN:Tel": phone_number,
                    "Name": None,
                    "Language": 'eng'
                }
        elif contact_list == swahili_phone_numbers:
            for phone_number in contact_list:
                advert_contacts[phone_number] = {
                    "URN:Tel": phone_number,
                    "Name": None,
                    "Language": 'swh'
                }
    log.warning(f"Exporting {len(advert_contacts)} contacts to {contacts_csv_path}")
    with open(contacts_csv_path, "w") as f:
        headers = ["URN:Tel", "Name", "Language"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        for phone_number in advert_contacts.values():
            writer.writerow(phone_number)

        log.info(f"Wrote {len(advert_contacts)} contacts to {contacts_csv_path}")
