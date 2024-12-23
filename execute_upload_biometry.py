"""Main function for execute upload biometry"""
import base64
from typing import List

import argparse
import json
import os
import logging
import logging.config

from aiohttp import ClientSession, FormData
from m7_aiohttp.auth.service_token import AioHttpServiceToken
from m7_aiohttp.services.endpoints import AioHttpEndpointsPort
from multidict._multidict import MultiDict

from const import M7_PEOPLE_NAME, M7_PEOPLE_LAST_NAME, M7_PEOPLE_PATRONYMIC, \
    M7_PEOPLE_ID, M7_BIOMETRY_OWNER_ID, M7_BIOMETRY_TYPE_ID, M7_BIOMETRY_ID, \
    M7_BIOMETRY_PROPERTIES, BIOMETRY_TYPE_ID_VISION_LABS_LUNA_SDK
from endpoint_service import EndpointServices
from init_utility import get_config, init_log
from utility_exceptions import NotFound, UtilityError

logger = logging.getLogger('biometry_utility')



class BiometryUploadBiometry:

    def __init__(self):
        self._init_others_service()



    def _init_others_service(self):
        try:
            self.config = get_config()
            init_log(self.config)
            print("self.config['m7']['endpoints']['auth_v2'] ", self.config['m7']['endpoints']['auth_v2'])
            print("self.config['m7']['credentials_file'] ", self.config['m7']['credentials_file'])

            service_token = AioHttpServiceToken(
                self.config['m7']['endpoints']['auth_v2'],
                self.config['m7']['credentials_file']
            )

            self.service_token = service_token
            logger.debug('self.service_token: %s', self.service_token)

            endpoints_port = AioHttpEndpointsPort(
                endpoints_service_url=self.config['m7']['endpoints']['endpoints'],
                service_token=self.service_token
            )
            self.endpoints_port = endpoints_port
            logger.debug('self.endpoints_port: %s', self.endpoints_port)



            endpoint_service = EndpointServices(
                service_token=self.service_token,
                endpoints_port=self.endpoints_port
            )
            self.endpoint_service = endpoint_service
            logger.debug('self.endpoint_service: %s', self.endpoint_service)

        except Exception as ex:
            print('Error: _init_others_service: %s', ex)
            logger.exception('Error: _init_others_service: %s', ex)

    @staticmethod
    def _get_sdk_type_by_biometry_type_id(biometry_type_id: str) -> str:
        sdk_type_dict = {
            BIOMETRY_TYPE_ID_VISION_LABS_LUNA_SDK: 'lunasdk'
        }
        return sdk_type_dict[biometry_type_id]

    def _get_biometry_upload_url(self) -> str:
        protocol = self.config['m7']['protocol']
        domain = self.config['m7']['root_domain']

        biometry_upload_url = self.config['utility_settings']['biometry_upload_url'].format(
            protocol, domain)
        print('biometry_upload_url ', biometry_upload_url)
        return biometry_upload_url

    def _get_stations_client_url(self) -> str:
        protocol = self.config['m7']['protocol']
        domain = self.config['m7']['root_domain']

        stations_client_url = self.config['utility_settings']['stations_client_url'].format(
            protocol, domain)
        print('stations_client_url ', stations_client_url)
        return stations_client_url


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

                if not resp_json.get('result'):
                    raise UtilityError('Error m7_people service: %s', resp_json)

                person_id = resp_json['result']
                logger.debug('%s successfully added to m7_people, person_id %s', person_id)
                return person_id
        except Exception as ex:
            logger.exception('Error _add_m7_people: %s', ex)
            raise


    async def _create_update_data_m7_people_service(self, people_data: dict):
        m7_people_url = self.config['m7']['endpoints']['people']
        logger.debug('create_update_data m7_people by url: %s', m7_people_url)

        for person_full_name, person_data in people_data.items():
            logger.debug('Process for for: %s', person_full_name)

            initial_person_data = person_data['m7_people']
            last_name = initial_person_data[M7_PEOPLE_LAST_NAME]
            first_name = initial_person_data.get(M7_PEOPLE_NAME)

            filter_m7_people = {
                M7_PEOPLE_LAST_NAME: {
                    'values': [last_name]
                }
            }
            if first_name:
                filter_m7_people[M7_PEOPLE_NAME] = {
                    'values': [first_name]
                }


            m7_people_list = await self.endpoint_service.get_list_by_filter_from_url(
                m7_people_url, filter_m7_people)

            if m7_people_list:
                person_data['m7_people']['person_id'] = m7_people_list[0].get(M7_PEOPLE_ID)
            else:
                person_id = await self.endpoint_service.add_data_m7_people(m7_people_url, person_data)
                person_data['m7_people']['person_id'] = person_id
                logger.debug('For %s got person_id: %s', person_full_name, person_id)
        return people_data


    @staticmethod
    async def _get_biometry_id_by_filter_from_m7_biometry(url: str,
                                                          biometry_type_id: str,
                                                          headers: MultiDict,
                                                          person_data: dict) -> List[dict]:
        try:
            initial_person_data = person_data['m7_people']
            last_name = initial_person_data[M7_PEOPLE_LAST_NAME]
            owner_id = initial_person_data[M7_PEOPLE_ID]
            filter_m7_biometry = {
                M7_BIOMETRY_OWNER_ID: {
                    'values': [owner_id]
                },
                M7_BIOMETRY_TYPE_ID: {
                    'values': [biometry_type_id]
                }
            }


            method_params = {
                'filter': filter_m7_biometry,
                'order': [],
                'limit': 1,
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

                if not resp_json.get('result'):
                    raise UtilityError('Error m7_biometry service: ', resp_json)

                biometry_id = resp_json['result'][0].get(M7_BIOMETRY_ID)
                logger.debug('%s already exist in m7_biometry, biometry_id: %s', biometry_id)
                return biometry_id
        except Exception as ex:
            print('Error _get_biometry_id_by_filter_from_m7_biometry: ', ex)
            logger.exception('Error _get_biometry_id_by_filter_from_m7_biometry: %s', ex)
            raise


    @staticmethod
    async def _add_data_m7_biometry(url: str,
                                    biometry_type_id: str,
                                    person_full_name: str,
                                    headers: MultiDict,
                                    person_data: dict) -> List[dict]:
        try:
            initial_person_data = person_data['m7_people']
            owner_id = initial_person_data[M7_PEOPLE_ID]

            data = {
                M7_BIOMETRY_OWNER_ID: owner_id,
                M7_BIOMETRY_TYPE_ID: biometry_type_id,
                M7_BIOMETRY_PROPERTIES: {}
            }
            method_params = {'data': data}

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

                if not resp_json.get('result'):
                    raise UtilityError('Error m7_biometry service: %s', resp_json)

                biometry_id = resp_json['result']
                logger.debug('successfully added to m7_biometry, biometry_id: %s', biometry_id)
                return biometry_id
        except Exception as ex:
            logger.exception('Error _add_data_m7_biometry: %s', ex)
            raise


    async def _create_update_data_m7_biometry_service(self, people_data: dict):
        m7_biometry_url = self._get_biometry_url()
        biometry_type_id = self.config['utility_settings']['biometry_type_id']

        logger.debug('create_update_data m7_biometry by url: %s', m7_biometry_url)

        for person_full_name, person_data in people_data.items():
            logger.debug('Create_update biometry for: %s', person_full_name)
            biometry_id = await self._get_biometry_id_by_filter_from_m7_biometry(
                url=m7_biometry_url,
                biometry_type_id=biometry_type_id,
                #headers=headers,
                person_data=person_data
            )
            if not biometry_id:
                biometry_id = await self._add_data_m7_biometry(
                    url=m7_biometry_url,
                    biometry_type_id=biometry_type_id,
                    person_full_name=person_full_name,
                    #headers=headers,
                    person_data=person_data
                )

            person_data['m7_people'][M7_BIOMETRY_ID] = biometry_id
        return people_data


    @staticmethod
    def _load_file_image(path_file_image: str) -> base64:
        try:
            file_bytes = bytearray()
            with open(path_file_image, 'rb') as file:
                while True:
                    chunk = file.read(1024)
                    if not chunk:
                        break
                    file_bytes.extend(chunk)
            logger.debug('Successfully loaded file img: %s', path_file_image)
            image_base64 = base64.b64encode(bytes(file_bytes)).decode('utf-8')
            return image_base64
        except IOError as ex:
            logger.exception('Error load_file_image: ', ex)
            print('Error load_file_image: ', ex)
            return None


    async def _get_template_by_image(self, image_base64: base64):
        try:
            stations_client_url = self._get_stations_client_url()
            biometry_type_id = self.config['utility_settings']['biometry_type_id']
            sdk_type = self._get_sdk_type_by_biometry_type_id(biometry_type_id)
            logger.debug('Try to get template by sdk: %s, by url: %s', sdk_type, stations_client_url)
            #image_base64 = base64.b64encode(image_base64).decode('utf-8')

            method_params = {
                'sdk_type': sdk_type,
                'image_buffer': {
                    'imageBuffer': {
                        'imageBuffer': image_base64
                    }
                }
            }

            async with ClientSession() as client:
                response = await client.post(
                    url=stations_client_url,
                    #headers=headers,
                    json={
                        "method": "get_template_by_image",
                        "jsonrpc": "2.0",
                        "params": method_params,
                        "id": 0
                    }
                )
                resp_json_bytes = await response.content.read()
                resp_json = json.loads(resp_json_bytes.decode())

                if not resp_json.get('result'):
                    raise UtilityError('Error "get_template_by_image": %s',  resp_json)

                logger.debug('Successfully get_template_by_image')


                return resp_json['result']
        except Exception as ex:
            logger.exception('Error get_template_by_image: %s', ex)
            raise


    async def _upload_m7_biometry_service(self, form_data: FormData) -> str:
        try:
            biometry_upload_url = self._get_biometry_upload_url()

            async with ClientSession() as client:
                response = await client.post(
                    url=biometry_upload_url,
                    #headers=headers,
                    data=form_data
                )
                resp_json_bytes = await response.content.read()
                resp_json = json.loads(resp_json_bytes.decode())
                if not resp_json.get('template_id'):
                    raise UtilityError('Error upload m7_biometry')
                logger.debug('Successfully upload_m7_biometry_service: %s', resp_json)
                return resp_json.get('template_id')
        except Exception as ex:
            logger.exception('Error upload_m7_biometry_service: %s', ex)
            raise




    async def _upload_template(self,
                               person_data: dict,
                               biometry_file_name: str,
                               source_biometry_folder: str):
        try:
            full_path = '{}/{}'.format(source_biometry_folder, biometry_file_name)
            person_id =  person_data['m7_people'][M7_PEOPLE_ID]
            biometry_id = person_data['m7_people'][M7_BIOMETRY_ID]

            image_base64 = self._load_file_image(full_path)
            image_template = await self._get_template_by_image(image_base64)

            face_quality = image_template['faceQuality']
            hash_template = image_template['hash']
            quality = image_template['quality']
            hash_type = image_template['hashType']

            template_properties = {
                'biometry_id': biometry_id,
                'attributes': {
                    'template_file_kind': 'template',
                    'id_photo_type': 'user',
                    'hashType': hash_type,
                    'quality': quality,
                    'faceQuality': face_quality
                }
            }

            form_data_template = FormData()
            form_data_template.add_field(
                'template_file',
                hash_template,
                #filename=biometry_file_name,
                content_type='plain/text'
            )
            form_data_template.add_field('template_upload_properties', json.dumps(template_properties))
            template_id = await self._upload_m7_biometry_service(form_data_template)

            image_properties = {
                'biometry_id': biometry_id,
                'attributes': {
                    'template_file_kind': 'image',
                    'id_photo_type': 'user',
                    'template_file_id': template_id
                }
            }

            form_data_image = FormData()
            form_data_image.add_field(
                'image_file',
                image_base64,
                #filename=biometry_file_name,
                content_type='image/jpeg'
            )
            form_data_template.add_field('image_upload_properties', json.dumps(image_properties))
            await self._upload_m7_biometry_service(form_data_template)
            logger.debug('Successfully upload files template and img')

        except Exception as ex:
            logger.exception('Error _upload_template: %s', ex)
            raise



    async def _create_update_templates_m7_biometry_service(self,
                                                           source_biometry_folder: str,
                                                           people_data: dict):

        for person_full_name, person_data in people_data.items():
            try:
                logger.debug('Create_update biometry templates for: %s', person_full_name)

                biometry_files = person_data['files']
                for biometry_file_name in biometry_files:
                    await self._upload_template(person_data, biometry_file_name, source_biometry_folder)
            except Exception as ex:
                logger.exception('Error create/update template for: %s: %s', person_full_name, ex)




    async def execute_upload_biometry(self):

        try:
            await self.service_token.start()
            print('execute_upload_biometry - into')
            source_biometry_folder = self.config['utility_settings']['source_biometry_folder']
            sorted_files = await self._get_files(source_biometry_folder)
            people_data = await self._init_people_data(sorted_files)
            #self.token = await self._get_token()

            people_data = await self._create_update_data_m7_people_service(people_data)
            people_data = await self._create_update_data_m7_biometry_service(people_data)
            # await self._create_update_templates_m7_biometry_service(
            #     source_biometry_folder,
            #     people_data)
        except Exception as ex:
            print('Error execute_upload_biometry: ', ex)
            logger.exception('Error execute_upload_biometry: %s', ex)


    async def stop_services(self):
        await self.service_token.stop()


    @staticmethod
    def _file_sort(value: str):
        return value.split('_')[0]

    async def _get_files(self, folder_path: str) -> List[str]:
        files = sorted(os.listdir(folder_path), key=self._file_sort)
        return files
