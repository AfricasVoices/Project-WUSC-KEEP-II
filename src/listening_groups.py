import csv
import os.path

from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
import time

from src.lib import PipelineConfiguration

log = Logger(__name__)

class ListeningGroup(object):

    @classmethod
    def tag_listening_groups_participants(cls, user, data, listening_group_dir):
        '''
        This tags uids who participated in repeat listening groups and/or weekly listening
        group sessions.
        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to tag listening group participation to.
        :type data: iterable of TracedData
        :param listening_group_dir: Directory containing de-identified listening groups contacts CSVs.
        :type user: str
        '''

        repeat_listening_group_participants = [] # Contains uids of listening group participants who will participate
                                                 # in all listening group sessions.
        listening_group_participants = dict()   # Contains lists of weekly listening group participants. The participants
                                                # will change each week.

        with open(f'{listening_group_dir}/repeat_listening_group.csv', "r", encoding='utf-8-sig') as f:
            repeat_listening_group_data = list(csv.DictReader(f))
            for row in repeat_listening_group_data:
                repeat_listening_group_participants.append(row['Mobile No'])
            log.info(f'Loaded {len(repeat_listening_group_participants)} repeat listening group participants')

        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            listening_group_participants[f'{plan.dataset_name}'] = []
            if os.path.isfile(f'{listening_group_dir}/{plan.dataset_name}_listening_group.csv'):
                with open(f'{listening_group_dir}/{plan.dataset_name}_listening_group.csv', "r",
                          encoding='utf-8-sig') as f:
                    plan_listening_group_data = list(csv.DictReader(f))
                    for row in plan_listening_group_data:
                        listening_group_participants[f'{plan.dataset_name}'].append(row['Mobile No'])
                    log.info(f'Loaded {len(listening_group_participants[f"{plan.dataset_name}"])} '
                             f'{plan.dataset_name} listening group participants')

        for td in data:
            listening_group_participation = dict() # of uid repeat and weekly listening group participation data
            listening_group_participation['repeat_listening_group_participant'] = False

            if td['uid'] in repeat_listening_group_participants:
                listening_group_participation['repeat_listening_group_participant'] = True

            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                listening_group_participation[f'{plan.dataset_name}_listening_group_participant'] = False
                if td['uid'] in listening_group_participants[f'{plan.dataset_name}']:
                    listening_group_participation[f'{plan.dataset_name}_listening_group_participant'] = True

            td.append_data(listening_group_participation, Metadata(user, Metadata.get_call_location(), time.time()))
