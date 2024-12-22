"""Main function for execute upload biometry"""
from typing import List

import argparse
import json
import os
import logging
import logging.config

from aiohttp import ClientSession
from multidict import MultiDict

from const import M7_PEOPLE_NAME, M7_PEOPLE_LAST_NAME, M7_PEOPLE_PATRONYMIC, M7_BIOMETRY_URL

global config_data

logger = logging.getLogger('biometry_utility')


class BiometryUploadBiometry:

    def __init__(self):
        self.token = None
        self.config = None

    @staticmethod
    def _get_login_header() -> MultiDict:
        headers = MultiDict({})
        headers.setdefault("Content-Type", "application/json")
        return headers

    def _get_header(self) -> MultiDict:
        headers = MultiDict({})
        headers.setdefault("X-M7-Authorization-Token", self.token)
        return headers


    def _get_biometry_url(self) -> str:
        protocol = self.config['m7']['protocol']
        domain = self.config['m7']['root_domain']

        biometry_url = M7_BIOMETRY_URL.format(protocol, domain)
        print('biometry_url ', biometry_url)
        return biometry_url

    async def _get_token(self):
        try:
            headers = self._get_login_header()
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


    async def _init_people_data(self, files: List[str]) -> dict:
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
        return people_data


    @staticmethod
    async def _add_data_m7_people(url: str,
                                  headers: MultiDict,
                                  person_data: dict) -> List[dict]:
        try:
            logger.exception('_add_m7_people: enter')
            initial_person_data = person_data['m7_people']
            last_name = initial_person_data[M7_PEOPLE_LAST_NAME]
            first_name = initial_person_data.get(M7_PEOPLE_NAME)

            person_dict = {
                M7_PEOPLE_LAST_NAME: last_name
            }
            if first_name:
                person_dict[M7_PEOPLE_NAME] = first_name
            method_params = {'person': person_dict}


            async with ClientSession() as client:
                response = await client.post(
                    url=url,
                    headers=headers,
                    json={
                        "method": "add",
                        "jsonrpc": "2.0",
                        "params": method_params,
                        "id": 0
                    }
                )
                resp_json_bytes = await response.content.read()
                resp_json = json.loads(resp_json_bytes.decode())
                person_id = resp_json['result']
                logger.debug('Got result: %s', person_id)
                return person_id
        except Exception as ex:
            logger.debug('Error _add_m7_people: %s', ex)


    @staticmethod
    async def _get_person_id_by_person_name_from_m7_people(url: str,
                                                           headers: MultiDict,
                                                           person_data: dict) -> List[dict]:
        try:
            logger.exception('_get_person_id_by_person_name_from_m7_people: enter')
            initial_person_data = person_data['m7_people']
            last_name = initial_person_data[M7_PEOPLE_LAST_NAME]
            first_name = initial_person_data.get(M7_PEOPLE_NAME)
            #patronymic = initial_person_data.get(M7_PEOPLE_PATRONYMIC)

            filter_m7_people = {
                M7_PEOPLE_LAST_NAME: {
                    'values': [last_name]
                }
            }
            if first_name:
                filter_m7_people[M7_PEOPLE_NAME] = {
                    'values': [first_name]
                }
            # if patronymic:
            #     filter_m7_people[M7_PEOPLE_PATRONYMIC] = {
            #         'values': [patronymic]
            #     }

            method_params = {
                'filter': filter_m7_people,
                'order': [],
                'limit': 100,
                'offset': 0
            }

            async with ClientSession() as client:
                response = await client.post(
                    url=url,
                    headers=headers,
                    json={
                        "method": "get_list_by_filter",
                        "jsonrpc": "2.0",
                        "params": method_params,
                        "id": 0
                    }
                )
                resp_json_bytes = await response.content.read()
                resp_json = json.loads(resp_json_bytes.decode())
                result = resp_json['result']
                person_id = resp_json['result'].get('person_id')
                logger.debug('Got result list: %s', result)
                return person_id
        except Exception as ex:
            print('Error _get_person_id_by_person_name_from_m7_people: ', ex)
            logger.debug('Error _get_person_id_by_person_name_from_m7_people: %s', ex)




    async def _create_update_data_m7_people_service(self, people_data: dict):
        headers = self._get_header()
        m7_people_url = self.config['m7']['endpoints']['people']
        logger.debug('create_update_data m7_people by url: %s', m7_people_url)

        for person_full_name, person_data in people_data.items():
            print('person_data ', person_data)
            logger.debug('Create_update person_data for: %s', person_full_name)
            print('Create_update person_data for: ', person_full_name)
            person_id = await self._get_person_id_by_person_name_from_m7_people(
                url=m7_people_url,
                headers=headers,
                person_data=person_data
            )
            if not person_id:
                person_id = await self._add_data_m7_people(
                    url=m7_people_url,
                    headers=headers,
                    person_data=person_data
                )
            person_data['m7_people']['person_id'] = person_id
        print('-------------')
        print('AFTER people_data ', people_data)
        return


    async def _create_update_data_m7_biometry_service(self, people_data: dict):
        headers = self._get_header()
        m7_biometry_url = self._get_biometry_url()
        logger.debug('create_update_data m7_biometry by url: %s', m7_biometry_url)

        for person_full_name, person_data in people_data.items():
            print('person_data ', person_data)
            logger.debug('Create_update person_data for: %s', person_full_name)
            print('Create_update person_data for: ', person_full_name)
            person_id = await self._get_person_id_by_person_name_from_m7_people(
                url=m7_biometry_url,
                headers=headers,
                person_data=person_data
            )
            if not person_id:
                person_id = await self._add_data_m7_people(
                    url=m7_biometry_url,
                    headers=headers,
                    person_data=person_data
                )
            person_data['m7_people']['person_id'] = person_id
        print('-------------')
        print('AFTER people_data ', people_data)
        return people_data





    async def execute_upload_biometry(self):

        try:
            print('execute_upload_biometry - into')
            self.config = self.get_config()
            source_biometry_folder = self.config['source_biometry_folder']
            print('config ',  self.config)
            self._init_log()
            sorted_files = await self._get_files(source_biometry_folder)
            people_data = await self._init_people_data(sorted_files)
            self.token = await self._get_token()

            people_data = await self._create_update_data_m7_people_service(people_data)

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
