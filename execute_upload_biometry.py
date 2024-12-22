"""Main function for execute upload biometry"""
from typing import List

import argparse
import json
import os
import logging
import logging.config
import sys

from aiohttp import ClientSession
from multidict import MultiDict

from const import M7_PEOPLE_NAME, M7_PEOPLE_LAST_NAME, M7_PEOPLE_PATRONYMIC, SERVICES_URL

global config_data

logger = logging.getLogger('biometry_utility')


class BiometryUploadBiometry:

    def __init__(self):
        self.token = None
        self.config = None


    def _update_urls():
        global config_data
        protocol = config_data['protocol']
        domain = config_data['domain']

        for service_name, url in SERVICES_URL.items():
            SERVICES_URL[service_name] = url.format(protocol, domain)

        print('SERVICES_URL ', SERVICES_URL)


    async def _get_token():
        try:
            headers = MultiDict({})
            headers.setdefault("Content-Type", "application/json")

            m7_accounts_url = SERVICES_URL['m7_accounts']
            logger.debug('_get_token from url: %s: enter', m7_accounts_url)
            async with ClientSession() as client:
                response = await client.post(
                    url=m7_accounts_url,
                    headers=headers,
                    json={
                        "method": "login",
                        "jsonrpc": "2.0",
                        "params": {
                            "login": "algont",
                            "password": "12345678"
                        },
                        "id": 0
                    }
                )
                resp_json_bytes = await response.content.read()
                resp_json = json.loads(resp_json_bytes.decode())
                token = resp_json['result']['access_token']
                logger.debug('Got access_token: %s', token)
                return token
        except Exception as ex:
            logger.debug('Error _get_token: %s', ex)



    def _get_person_name_info(file_name: str) -> dict :
        person_dict = {
            M7_PEOPLE_LAST_NAME: None,
            M7_PEOPLE_NAME: None,
            M7_PEOPLE_PATRONYMIC: None
        }

        person_full_name_list = file_name.split('_')
        last_elem = person_full_name_list[-1].split('.')[:1]  # del '.jpeg', '. jpg'
        person_full_name_list[-1] = last_elem[0]

        for index, name_info in enumerate(person_full_name_list):
            if index == 0:
                person_dict[M7_PEOPLE_LAST_NAME] = name_info
            if index == 1:
                person_dict[M7_PEOPLE_NAME] = name_info
            if index == 2:
                person_dict[M7_PEOPLE_PATRONYMIC] = name_info

        return person_dict


    async def _create_people_dict(files: List[str]):
        people_data = []

        for file_name in files:
            people_data.append(_get_person_name_info(file_name))
        print('people_data ', people_data)


    async def _create_m7_people(token: str, )


    async def execute_upload_biometry(self):

        try:
            print('execute_upload_biometry - into')
            self.config_data = self.get_config()
            source_biometry_folder = self.config_data['source_biometry_folder']
            print('config_data ',  self.config_data)
            self._init_log()
            self._update_urls()
            sorted_files = await self._get_files(source_biometry_folder)
            await self._create_people_dict(sorted_files)
            token = await self._get_token()

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
        files = sorted(os.listdir(folder_path), key=_file_sort)
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

    def get_config(self):
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