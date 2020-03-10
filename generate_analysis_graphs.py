import argparse
import glob
import json
from collections import OrderedDict
import sys
import csv

import altair
from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

from src.lib import PipelineConfiguration, CodeSchemes
from src.lib.pipeline_configuration import CodingModes

Logger.set_project_name("WUSC-KEEP-II")
log = Logger(__name__)

IMG_SCALE_FACTOR = 10  # Increase this to increase the resolution of the outputted PNGs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates graphs for analysis")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")

    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("individuals_json_input_path", metavar="individuals-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("output_dir", metavar="output-dir",
                        help="Directory to write the output graphs to")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path

    messages_json_input_path = args.messages_json_input_path
    individuals_json_input_path = args.individuals_json_input_path
    output_dir = args.output_dir

    IOUtils.ensure_dirs_exist(output_dir)

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    if pipeline_configuration.drive_upload is not None:
        log.info(f"Downloading Google Drive service account credentials...")
        credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, pipeline_configuration.drive_upload.drive_credentials_file_url))
        drive_client_wrapper.init_client_from_info(credentials_info)
        # Infer which RQA and Demog coding plans to use from the pipeline name.
    if pipeline_configuration.pipeline_name == "dadaab_pipeline":
        log.info("Extracting Dadaab pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.DADAAB_RQA_CODING_PLANS
        PipelineConfiguration.SURVEY_CODING_PLANS = PipelineConfiguration.DADAAB_SURVEY_CODING_PLANS
        CodeSchemes.WS_CORRECT_DATASET = CodeSchemes.DADAAB_WS_CORRECT_DATASET
    else:
        assert pipeline_configuration.pipeline_name == "kakuma_pipeline", "PipelineName must be either " \
                                                                          "'dadaab_pipeline or kakuma_pipeline"
        log.info("Extracting Kakuma pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.KAKUMA_RQA_CODING_PLANS
        PipelineConfiguration.SURVEY_CODING_PLANS = PipelineConfiguration.KAKUMA_SURVEY_CODING_PLANS
        CodeSchemes.WS_CORRECT_DATASET = CodeSchemes.KAKUMA_WS_CORRECT_DATASET

    # Read the messages dataset
    log.info(f"Loading the messages dataset from {messages_json_input_path}...")
    with open(messages_json_input_path) as f:
        messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(messages)} messages")

    # Read the individuals dataset
    log.info(f"Loading the individuals dataset from {individuals_json_input_path}...")
    with open(individuals_json_input_path) as f:
        individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(individuals)} individuals")

    log.info(f'Computing repeat and new participation per show ...')
    # Computes the number of new and repeat consented individuals who participated in each radio show.
    # Repeat participants are consented individuals who participated in previous shows prior to the target show.
    # New participants are consented individuals who participated in target show but din't participate in previous shows.
    repeat_new_participation_map = OrderedDict()  # of rqa_raw_field to participation metrics.

    rqa_raw_fields =  [plan.raw_field for plan in PipelineConfiguration.RQA_CODING_PLANS]

    for rqa_raw_field in rqa_raw_fields:
        target_radio_show = rqa_raw_field  # radio show in which we are calculating repeat and new participation metrics for.

        target_radio_show_participants = set()  # contains uids of individuals who participated in target radio show.
        for ind in individuals:
            if ind["consent_withdrawn"] == Codes.TRUE:
                continue

            if target_radio_show in ind:
                target_radio_show_participants.add(ind['uid'])
        log.debug(f'No. of uids in {target_radio_show} = {len(target_radio_show_participants)}')

        previous_radio_shows = []  # rqa_raw_fields of shows that aired before the target radio show.
        for rqa_raw_field in rqa_raw_fields:
            if rqa_raw_field == target_radio_show:
                break

            previous_radio_shows.append(rqa_raw_field)

        previous_radio_shows_participants = set()  # uids of individuals who participated in previous radio shows.
        for rqa_raw_field in previous_radio_shows:
            for ind in individuals:
                if ind["consent_withdrawn"] == Codes.TRUE:
                    continue

                if rqa_raw_field in ind:
                    previous_radio_shows_participants.add(ind['uid'])
        log.debug(f'No. of uids in {len(previous_radio_shows)} previous_radio_shows = {len(previous_radio_shows_participants)} ')

        repeat_participants = set()  # uids of individuals who participated in target and previous shows.
        new_participants = set()  # uids of individuals who participated in target show but din't participate in previous shows.
        for uid in target_radio_show_participants:
            if uid in previous_radio_shows_participants:
                repeat_participants.add(uid)
            else:
                new_participants.add(uid)
        log.debug(f'No. of repeat uids in {target_radio_show} = {len(repeat_participants)} ')
        log.debug(f'No. of new uids in {target_radio_show} = {len(new_participants)} ')

        repeat_new_participation_map[target_radio_show] = {
            "Radio Show": target_radio_show,  # Todo switch to dataset name
            "No. of opt-in participants": len(target_radio_show_participants),
            "No. of opt-in participants that are new": len(new_participants),
            "No. of opt-in participants that are repeats": len(repeat_participants),
            "% of opt-in participants that are new": None,
            "% of opt-in participants that are repeats": None
        }

        # Compute:
        #  -% of opt-in participants that are new, by computing No. of opt-in participants that are new / No. of opt-in participants
        #  * 100, to 1 decimal place.
        #  - % of opt-in participants that are repeats, by computing No. of opt-in participants that are repeats / No. of opt-in participants
        #  * 100, to 1 decimal place.
        if len(new_participants) > 0:
            repeat_new_participation_map[target_radio_show]["% of opt-in participants that are new"] = \
                round(len(new_participants) / len(target_radio_show_participants) * 100, 1)
            repeat_new_participation_map[target_radio_show]["% of opt-in participants that are repeats"] = \
                round(len(repeat_participants) / len(target_radio_show_participants) * 100, 1)

    log.info(f'Writing per show repeat and new participation metrics per show csv ...')
    with open(f"{output_dir}/per_show_repeat_and_new_participation.csv", "w") as f:
        headers = ["Radio Show", "No. of opt-in participants", "No. of opt-in participants that are new",
                   "No. of opt-in participants that are repeats", "% of opt-in participants that are new",
                   "% of opt-in participants that are repeats"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in repeat_new_participation_map.values():
            writer.writerow(row)

    sys.setrecursionlimit(15000)
    # Compute the number of messages in each show and graph
    log.info(f"Graphing the number of messages received in response to each show...")
    messages_per_show = OrderedDict()  # Of radio show index to messages count
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        messages_per_show[plan.raw_field] = 0

    for msg in messages:
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if msg.get(plan.raw_field, "") != "" and msg["consent_withdrawn"] == "false":
                messages_per_show[plan.raw_field] += 1

    chart = altair.Chart(
        altair.Data(values=[{"show": k, "count": v} for k, v in messages_per_show.items()])
    ).mark_bar().encode(
        x=altair.X("show:N", title="Show", sort=list(messages_per_show.keys())),
        y=altair.Y("count:Q", title="Number of Messages")
    ).properties(
        title="Messages per Show"
    )
    chart.save(f"{output_dir}/messages_per_show.html")
    chart.save(f"{output_dir}/messages_per_show.png", scale_factor=IMG_SCALE_FACTOR)

    # Compute the number of individuals in each show and graph
    log.info(f"Graphing the number of individuals who responded to each show...")
    individuals_per_show = OrderedDict()  # Of radio show index to individuals count
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        individuals_per_show[plan.raw_field] = 0

    for ind in individuals:
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if ind.get(plan.raw_field, "") != "" and ind["consent_withdrawn"] == "false":
                individuals_per_show[plan.raw_field] += 1

    chart = altair.Chart(
        altair.Data(values=[{"show": k, "count": v} for k, v in individuals_per_show.items()])
    ).mark_bar().encode(
        x=altair.X("show:N", title="Show", sort=list(individuals_per_show.keys())),
        y=altair.Y("count:Q", title="Number of Individuals")
    ).properties(
        title="Individuals per Show"
    )
    chart.save(f"{output_dir}/individuals_per_show.html")
    chart.save(f"{output_dir}/individuals_per_show.png", scale_factor=IMG_SCALE_FACTOR)

    # Plot the per-season distribution of responses for each survey question, per individual
    for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            log.info(f"Graphing the distribution of codes for {cc.analysis_file_key}...")
            label_counts = OrderedDict()
            for code in cc.code_scheme.codes:
                label_counts[code.string_value] = 0

            if cc.coding_mode == CodingModes.SINGLE:
                for ind in individuals:
                    label_counts[ind[cc.analysis_file_key]] += 1
            else:
                assert cc.coding_mode == CodingModes.MULTIPLE
                for ind in individuals:
                    for code in cc.code_scheme.codes:
                        if ind[f"{cc.analysis_file_key}{code.string_value}"] == Codes.MATRIX_1:
                            label_counts[code.string_value] += 1

            chart = altair.Chart(
                altair.Data(values=[{"label": k, "count": v} for k, v in label_counts.items()])
            ).mark_bar().encode(
                x=altair.X("label:N", title="Label", sort=list(label_counts.keys())),
                y=altair.Y("count:Q", title="Number of Individuals")
            ).properties(
                title=f"Season Distribution: {cc.analysis_file_key}"
            )
            chart.save(f"{output_dir}/season_distribution_{cc.analysis_file_key}.html")
            chart.save(f"{output_dir}/season_distribution_{cc.analysis_file_key}.png", scale_factor=IMG_SCALE_FACTOR)

    if pipeline_configuration.drive_upload is not None:
        log.info("Uploading graphs to Drive...")
        paths_to_upload = glob.glob(f"{output_dir}/*.png")
        for i, path in enumerate(paths_to_upload):
            log.info(f"Uploading graph {i + 1}/{len(paths_to_upload)}: {path}...")
            drive_client_wrapper.update_or_create(path, pipeline_configuration.drive_upload.analysis_graphs_dir,
                                                  target_folder_is_shared_with_me=True)
    else:
        log.info("Skipping uploading to Google Drive (because the pipeline configuration json does not contain the key "
                 "'DriveUploadPaths')")
