"""Main function for execute upload biometry"""
from typing import List

import aiofiles.os as aios
import argparse
import json
import os
import logging
import logging.config
import sys

from const import M7_PEOPLE_NAME, M7_PEOPLE_LAST_NAME, M7_PEOPLE_PATRONYMIC

global config_data

logger = logging.getLogger('biometry_utility')

def _get_person_name_info(file_name: str):
    person_name_list = file_name.split('_')

    person_dict = {
        M7_PEOPLE_NAME: None,
        M7_PEOPLE_LAST_NAME: None,
        M7_PEOPLE_PATRONYMIC: None
    }

async def _create_people_dict(files: List[str]):
    for file_name in files:





async def execute_upload_biometry():
    global config_data

    try:
        print('execute_upload_biometry - into')
        config_data = get_config()
        source_biometry_folder = config_data['source_biometry_folder']
        print('global ', config_data)
        _init_log()
        sorted_files = await _get_files(source_biometry_folder)

        pass
    except Exception as ex:
        print('Error execute_upload_biometry: ', ex)

def run_utility():
    global config_data
    print(config_data)
    pass

def _file_sort(value: str):
    return value.split('_')[0]

async def _get_files(folder_path: str) -> List[str]:
    files = sorted(await aios.listdir(folder_path), key=_file_sort)
    print('files ', files)
    return files


def _init_log():
    global config_data

    conf_logging = config_data['logging']
    log_path = conf_logging['handlers']['file']['filename']

    log_dir = os.path.dirname(log_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    #os.mknod(log_path)

    logging.config.dictConfig(conf_logging)
    logger.debug('Successfully log init')

def get_config():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--m7config',
            help='M7 configuration file name',
            type=str,
            default='biometry_utility_conf.json')
        args, _ = parser.parse_known_args()

        with open(args.m7config) as f:
            config = json.load(f)
        return config
    except Exception as ex:
        print('config ', ex)