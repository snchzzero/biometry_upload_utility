"""Main function for execute upload biometry"""

from typing import List

import json
import os
import logging.config

from aiohttp import FormData
from m7_aiohttp.auth.service_token import AioHttpServiceToken
from m7_aiohttp.services.endpoints import AioHttpEndpointsPort

from const import M7_PEOPLE_NAME, M7_PEOPLE_LAST_NAME, M7_PEOPLE_PATRONYMIC, \
    M7_PEOPLE_ID, M7_BIOMETRY_OWNER_ID, M7_BIOMETRY_TYPE_ID, M7_BIOMETRY_ID, \
    M7_BIOMETRY_PROPERTIES, BIOMETRY_TYPE_ID_VISION_LABS_LUNA_SDK, \
    ENDPOINT_M7_BIOMETRY, M7_FILES_OBJECT_ID, \
    M7_FILE_ATTRIBUTES, M7_FILE_ATTR_TEMPLATE_FILE_KIND, M7_FILE_DOWNLOAD_URL
from endpoint_service import EndpointServices
from init_utility import get_config, init_log
from utility_exceptions import UtilityError

logger = logging.getLogger('biometry_utility')



class BiometryUploadBiometry:

    def __init__(self):
        self._init_sub_service()
        self.total = {
            'new_person': 0,
            'new_bio_templates': 0
        }


    def _init_sub_service(self):
        try:
            self.config = get_config()
            init_log(self.config)

            self.service_token = AioHttpServiceToken(
                self.config['m7']['endpoints']['auth_v2'],
                self.config['m7']['credentials_file']
            )
            self.endpoints_port = AioHttpEndpointsPort(
                endpoints_service_url=self.config['m7']['endpoints']['endpoints'],
                service_token=self.service_token
            )
            self.endpoint_service = EndpointServices(
                service_token=self.service_token,
                endpoints_port=self.endpoints_port
            )
        except Exception as ex:
            logger.exception('Error: init_sub_service: %s', ex)


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
        return biometry_upload_url


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
            people_data[full_name]['files'].sort()
        return people_data


    async def _create_update_data_m7_people_service(self, people_data: dict):
        m7_people_url = self.config['m7']['endpoints']['people']
        logger.debug('create_update_data m7_people by url: %s', m7_people_url)

        for person_full_name, person_data in people_data.items():
            logger.debug('Process for for: %s', person_full_name)

            initial_person_data = person_data['m7_people']
            last_name = initial_person_data[M7_PEOPLE_LAST_NAME]
            first_name = initial_person_data.get(M7_PEOPLE_NAME)
            patronymic = initial_person_data.get(M7_PEOPLE_PATRONYMIC)


            filter_m7_people = {
                M7_PEOPLE_LAST_NAME: {
                    'values': [last_name]
                }
            }
            if first_name:
                filter_m7_people[M7_PEOPLE_NAME] = {
                    'values': [first_name]
                }
            if patronymic:
                filter_m7_people[M7_PEOPLE_PATRONYMIC] = {
                    'values': [patronymic]
                }


            m7_people_list = await self.endpoint_service.get_list_by_filter_from_url(
                m7_people_url, filter_m7_people)

            if m7_people_list:
                person_id = m7_people_list[0].get(M7_PEOPLE_ID)
                person_data['m7_people']['person_id'] = person_id
            else:
                m7_person_dict = {
                    M7_PEOPLE_LAST_NAME: last_name
                }
                if first_name:
                    m7_person_dict[M7_PEOPLE_NAME] = first_name
                if patronymic:
                    m7_person_dict[M7_PEOPLE_PATRONYMIC] = patronymic

                person_id = await self.endpoint_service.add_data_m7_people(
                    service_url=m7_people_url,
                    m7_person_data=m7_person_dict)
                self.total['new_person'] += 1

            album_id = await self.endpoint_service.get_album_id_by_person_id(
                person_id=person_id,
                name=last_name
            )

            await self.endpoint_service.assign_person_id_to_album_id(person_id, album_id)

            person_data['m7_people']['person_id'] = person_id
            person_data['m7_people']['album_id'] = album_id

            logger.debug('For %s got person_id: %s, album_id: %s',
                         person_full_name, person_id, album_id)
        return people_data


    async def _create_update_data_m7_biometry_service(self, people_data: dict):
        m7_biometry_url = await self.endpoint_service.get_url(ENDPOINT_M7_BIOMETRY)
        biometry_type_id = self.config['utility_settings']['biometry_type_id']

        logger.debug('create_update_data m7_biometry by url: %s', m7_biometry_url)

        for person_full_name, person_data in people_data.items():
            logger.debug('Create_update biometry for: %s', person_full_name)

            initial_person_data = person_data['m7_people']
            owner_id = initial_person_data[M7_PEOPLE_ID]
            filter_m7_biometry = {
                M7_BIOMETRY_OWNER_ID: {
                    'values': [owner_id]
                },
                M7_BIOMETRY_TYPE_ID: {
                    'values': [biometry_type_id]
                }
            }

            m7_biometry_list = await self.endpoint_service.get_list_by_filter_from_url(
                service_url=m7_biometry_url,
                filter_dict=filter_m7_biometry
            )
            if m7_biometry_list:
                biometry_id = m7_biometry_list[0].get(M7_BIOMETRY_ID)
                person_data['m7_people'][M7_BIOMETRY_ID] = biometry_id
            else:
                biometry_data = {
                    M7_BIOMETRY_OWNER_ID: owner_id,
                    M7_BIOMETRY_TYPE_ID: biometry_type_id,
                    M7_BIOMETRY_PROPERTIES: {}
                }
                biometry_id = await self.endpoint_service.add_data_m7_biometry(
                    service_url=m7_biometry_url,
                    m7_biometry_data=biometry_data
                )
                person_data['m7_people'][M7_BIOMETRY_ID] = biometry_id
            logger.debug('For %s got biometry_id: %s', person_full_name, biometry_id)

        return people_data


    @staticmethod
    def _get_file_image_bytes(path_file_image: str) -> bytes:
        try:
            file_bytes = bytearray()
            with open(path_file_image, 'rb') as file:
                while True:
                    chunk = file.read(1024)
                    if not chunk:
                        break
                    file_bytes.extend(chunk)
            logger.debug('Successfully got file img bytes: %s', path_file_image)
            return bytes(file_bytes)
        except IOError as ex:
            raise UtilityError('Error _get_file_image_bytes: ', ex)


    async def _get_all_current_templates_by_biometry_id(self,
                                                        biometry_id: str,
                                                        sdk_type: str
                                                        ) -> List[dict] or None:
        m7_files_url = self.config['m7']['endpoints']['files']
        templates_list = []

        file_filter = {
            M7_FILES_OBJECT_ID: {
                'values': [biometry_id]
            },
            M7_FILE_ATTRIBUTES: {
                M7_FILE_ATTR_TEMPLATE_FILE_KIND: {
                    'values': ['image']
                }
            }
        }
        m7_files = await self.endpoint_service.get_list_by_filter_from_url(m7_files_url, file_filter)
        if not m7_files:
            return []

        download_urls_image_files = [m7_file.get(M7_FILE_DOWNLOAD_URL) for m7_file in m7_files]
        for url in download_urls_image_files:
            file_bytes = await self.endpoint_service.get_file_bytes(url)
            image_template = await self.endpoint_service.get_template_by_image(
                sdk_type=sdk_type,
                file_bytes=file_bytes
            )
            templates_list.append(image_template)

        return templates_list



    async def _create_template_by_file(self,
                                       person_data: dict,
                                       biometry_file_name: str,
                                       source_biometry_folder: str):
        try:
            full_path = '{}/{}'.format(source_biometry_folder, biometry_file_name)
            biometry_id = person_data['m7_people'][M7_BIOMETRY_ID]
            album_id = person_data['m7_people']['album_id']

            file_bytes = self._get_file_image_bytes(full_path)

            biometry_type_id = self.config['utility_settings']['biometry_type_id']
            sdk_type = self._get_sdk_type_by_biometry_type_id(biometry_type_id)

            templates_list_for_check = []
            image_template = await self.endpoint_service.get_template_by_image(
                sdk_type=sdk_type,
                file_bytes=file_bytes
            )
            templates_list_for_check.append(image_template)

            all_current_templates = await self._get_all_current_templates_by_biometry_id(
                biometry_id=biometry_id,
                sdk_type=sdk_type
            )

            if all_current_templates:
                templates_list_for_check.extend(all_current_templates)
                check_biorecord_photos_config = \
                    self.config['utility_settings']['check_biorecord_photos']
                await self.endpoint_service.check_biorecord_photos(
                    templates_list=templates_list_for_check,
                    sdk_type=sdk_type,
                    check_config=check_biorecord_photos_config
                )

            face_quality = image_template['faceQuality']
            hash_template = image_template['hash']
            quality = image_template['quality']
            hash_type = image_template['hashType']

            logger.debug('hash_template : %s', hash_template)


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

            form_data = FormData()
            form_data.add_field(
                'template_file',
                hash_template,
                filename=biometry_file_name,
                content_type='plain/text'
            )
            form_data.add_field('template_upload_properties', json.dumps(template_properties))

            biometry_upload_url = self._get_biometry_upload_url()

            image_properties = {
                'biometry_id': biometry_id,
                'attributes': {
                    'template_file_kind': 'image',
                    'id_photo_type': 'user'
                }
            }

            form_data.add_field(
                'image_file',
                file_bytes,
                filename=biometry_file_name,
                content_type='image/jpeg'
            )
            form_data.add_field('image_upload_properties', json.dumps(image_properties))
            await self.endpoint_service.upload_m7_biometry(
                url=biometry_upload_url,
                form_data=form_data
            )
            self.total['new_bio_templates'] += 1
            logger.debug('Successfully upload files template and img')

            await self.add_photo_to_photo_album(
                file_bytes=file_bytes,
                album_id=album_id,
                file_name=biometry_file_name
            )
        except Exception:
            raise


    async def add_photo_to_photo_album(self,
                                       file_bytes: bytes,
                                       album_id: str,
                                       file_name: str
                                       ):

        result = await self.endpoint_service.get_photo_list_by_filter(album_id)
        if result:
            logger.debug('For album_id: %s already had any photos', album_id)
            return

        await self.endpoint_service.upload_m7_photo_album(
            file_bytes=file_bytes,
            album_id=album_id,
            file_name=file_name
        )


    async def _create_templates_m7_biometry_service(self,
                                                    source_biometry_folder: str,
                                                    people_data: dict):

        for person_full_name, person_data in people_data.items():
            logger.debug('Create_update biometry templates for: %s', person_full_name)
            biometry_files = person_data['files']
            logger.debug('User image files: %s', biometry_files)

            for biometry_file_name in biometry_files:
                try:
                    await self._create_template_by_file(
                        person_data=person_data,
                        biometry_file_name=biometry_file_name,
                        source_biometry_folder=source_biometry_folder)
                except Exception as ex:
                    logger.exception(
                        'Error create template for: %s: %s: %s',
                        person_full_name, biometry_file_name, ex)


    async def execute_upload_biometry(self):

        try:
            await self.service_token.start()

            print('Start executing upload_biometry')
            logger.debug('Start executing upload_biometry')

            source_biometry_folder = self.config['utility_settings']['source_biometry_folder']
            sorted_files = await self._get_files(source_biometry_folder)
            people_data = await self._init_people_data(sorted_files)

            people_data = await self._create_update_data_m7_people_service(people_data)
            people_data = await self._create_update_data_m7_biometry_service(people_data)
            await self._create_templates_m7_biometry_service(
                source_biometry_folder,
                people_data)

            print('Finish executing upload_biometry')
            print(
                'Info: added new people: {}, added new bio_templates: {}'.format(
                self.total['new_person'], self.total['new_bio_templates']
            ))
            logger.debug('Finish executing upload_biometry')
            logger.debug(
                'Info: added new people: %s, added new bio_templates: %s',
                self.total['new_person'], self.total['new_bio_templates']
            )
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
