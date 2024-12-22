"""Main function for execute upload biometry"""
from typing import List

import argparse
import json
import os
import logging
import logging.config

from aiohttp import ClientSession
from multidict import MultiDict

from const import M7_PEOPLE_NAME, M7_PEOPLE_LAST_NAME, M7_PEOPLE_PATRONYMIC, SERVICES_URL

global config_data

logger = logging.getLogger('biometry_utility')


class BiometryUploadBiometry:

    def __init__(self):
        self.token = None
        self.config = None


    def _update_biometry_url(self):
        protocol = self.config['m7']['protocol']
        domain = self.config['m7']['root_domain']

        for service_name, url in SERVICES_URL.items():
            SERVICES_URL[service_name] = url.format(protocol, domain)
        print('SERVICES_URL ', SERVICES_URL)

    async def _get_token(self):
        try:
            headers = MultiDict({})
            headers.setdefault("Content-Type", "application/json")

            m7_accounts_url = self.config['m7']['endpoints']['auth_v2']
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


    @staticmethod
    def _create_initial_data_for_m7_people(file_name: str) -> dict :
        person_dict = {
            M7_PEOPLE_LAST_NAME: None,
            M7_PEOPLE_NAME: None,
            M7_PEOPLE_PATRONYMIC: None
        }

        person_full_name_list = file_name.split('_')
        last_elem = person_full_name_list[-1].split('.')[:1]  # del '.jpeg', '. jpg'
        person_full_name_list[-1] = last_elem[0]

        full_name = None
        for index, name_info in enumerate(person_full_name_list):
            if index == 0:
                person_dict[M7_PEOPLE_LAST_NAME] = name_info
                full_name = name_info
            if index == 1 and not name_info.isascii():
                person_dict[M7_PEOPLE_NAME] = name_info
                full_name = '{}_{}'.format(full_name, name_info)
            if index == 2 and not name_info.isascii():
                person_dict[M7_PEOPLE_PATRONYMIC] = name_info
                full_name = '{}_{}'.format(full_name, name_info)


        return {
            'full_name': full_name,
            'm7_people': {'m7_people': person_dict}
        }


    async def _create_people_dict(self, files: List[str]) -> List[dict]:
        people_data = {}

        for file_name in files:
            person_data = self._create_initial_data_for_m7_people(file_name)
            full_name = person_data['full_name']
            m7_people = person_data['m7_people']
            if people_data.get(full_name):
                people_data[full_name]['files'].append(file_name)
            else:
                people_data[full_name] = m7_people
                people_data[full_name]['files'] = [file_name]

        print('people_data ', people_data)


    async def _create_m7_people(self):
        pass



    async def execute_upload_biometry(self):

        try:
            print('execute_upload_biometry - into')
            self.config = self.get_config()
            source_biometry_folder = self.config['source_biometry_folder']
            print('config ',  self.config)
            self._init_log()
            self._update_biometry_url()
            sorted_files = await self._get_files(source_biometry_folder)
            await self._create_people_dict(sorted_files)
            self.token = await self._get_token()

            pass
        except Exception as ex:
            print('Error execute_upload_biometry: ', ex)


    @staticmethod
    def _file_sort(value: str):
        return value.split('_')[0]

    async def _get_files(self, folder_path: str) -> List[str]:
        files = sorted(os.listdir(folder_path), key=self._file_sort)
        print('files ', files)
        return files


    def _init_log(self):

        conf_logging = self.config['logging']
        log_path = conf_logging['handlers']['file']['filename']

        log_dir = os.path.dirname(log_path)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        logging.config.dictConfig(conf_logging)
        logger.debug('Successfully log init')


    @staticmethod
    def get_config():
        try:
            parser = argparse.ArgumentParser()
            parser.add_argument(
                '--config',
                help='M7 configuration file name',
                type=str,
                default='biometry_utility_conf.json')
            parser.add_argument(
                '--m7_config',
                help='M7 configuration file name',
                type=str,
                default='/etc/m7/m7.json')
            args, _ = parser.parse_known_args()

            with open(args.config) as f:
                config = json.load(f)
            try:
                with open(args.m7_config) as f:
                    m7_config = json.load(f)
                if m7_config:
                    config.update(m7_config)
            except Exception as ex:
                print("Can't get file '/etc/m7/m7.json': ", ex)

            return config
        except Exception as ex:
            print('Error get_config ', ex)
