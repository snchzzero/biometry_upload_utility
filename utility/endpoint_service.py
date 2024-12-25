"""Service for execute requests"""
import base64
import json
import logging
from typing import List

from aiohttp import ClientSession, FormData
from m7_aiohttp.auth.service_token import AioHttpServiceToken
from m7_aiohttp.exceptions import NotFound, AlreadyExists
from m7_aiohttp.services.endpoints import AioHttpEndpointsPort

from const import ENDPOINT_ID_STATIONS_CLIENT, ENDPOINT_M7_PROFILE_ALBUM, \
    ENDPOINT_M7_PHOTO_ALBUM, M7_PHOTO_ALBUM_NAME, ENDPOINT_M7_PHOTO_ALBUM_UPLOAD, M7_PHOTO_ALBUM_ID
from utility_exceptions import UtilityError

logger = logging.getLogger('endpoint_service')


class EndpointServices:
    """
    Get url by service name and
    get data as list by filter from id services or m7 services
    """

    def __init__(
            self,
            service_token: AioHttpServiceToken,
            endpoints_port: AioHttpEndpointsPort
    ):
        self._service_token = service_token
        self._endpoints_port = endpoints_port


    async def get_url(self, endpoint_names: str) -> str:
        """
        Get url by endpoint name from endpoint service
        """

        try:
            url = await self._endpoints_port.get_endpoint_url(name=endpoint_names)
            logger.debug("got url: %s", url)
        except UtilityError as ex:
            raise ('Can''t get url by endpoint_name: {}'.format(endpoint_names)) from ex
        return url


    async def get_list_by_filter_from_url(self,
                                          service_url: str,
                                          filter_dict: dict = None) -> List[dict]:
        """
        Get list elements by filter from url
        Args:
            service_url: url of service
            filter_dict: filter dict. Defaults to None.
        Returns:
            list: list of elements
        """

        try:
            logger.debug('get_list_by_filter_from_url: %s - enter', service_url)
            filter_dict = {} if filter_dict is None else filter_dict

            async with self._service_token.create_client(service_url) as service_client:
                elements_list = await service_client.get_list_by_filter(filter_dict, [], 100, 0)
                logger.debug(
                    'From url: %s - got list: %s', service_url, elements_list)
                return elements_list
        except Exception as ex:
            raise UtilityError(
                'Can''t get list elements from url: {}: {}'.format(service_url, ex)
            ) from ex


    async def add_data_m7_people(self, service_url: str, m7_person_data: dict) -> str:
        try:
            logger.debug('add_data_m7_people: enter')
            async with self._service_token.create_client(service_url) as service_client:
                person_id = await service_client.add(m7_person_data)
                return person_id
        except Exception as ex:
            raise UtilityError('Can''t add_data_m7_people: {}'.format(ex)) from ex


    async def add_data_m7_biometry(self, service_url: str, m7_biometry_data: dict) -> str:
        try:
            logger.debug('add_data_m7_biometry: enter')
            async with self._service_token.create_client(service_url) as service_client:
                biometry_id = await service_client.add(m7_biometry_data)
                return biometry_id
        except Exception as ex:
            logger.exception('Can''t add_data_m7_biometry: %s', ex)
            raise


    async def get_template_by_image(self, file_bytes: bytes, sdk_type: str):
        try:
            stations_client_url = await self.get_url(ENDPOINT_ID_STATIONS_CLIENT)
            logger.debug('Try to get template by sdk: %s, by url: %s', sdk_type, stations_client_url)

            image_buffer_data = {
                'imageBuffer': {
                    'imageBuffer': base64.b64encode(bytes(file_bytes)).decode('utf-8')
                }
            }

            async with self._service_token.create_client(stations_client_url) as service_client:
                template = await service_client.get_template_by_image(
                    sdk_type=sdk_type,
                    image_buffer=image_buffer_data
                )
                logger.debug('Successfully get_template_by_image')
                return template
        except Exception as ex:
            logger.exception('Can''t get_template_by_image: %s', ex)
            raise


    async def upload_m7_biometry(self, url: str, form_data: FormData) -> str:
        try:
            logger.debug('Try to upload template by url: %s', url)
            headers = self._service_token.client_builder.headers

            async with ClientSession() as client:
                response = await client.post(
                    url=url,
                    data=form_data,
                    headers=headers
                )
                resp_json_bytes = await response.content.read()
                resp_json = json.loads(resp_json_bytes.decode())

                if not resp_json.get('template_id'):
                    raise UtilityError('Error upload m7_biometry: %s', resp_json)

                file_id = resp_json.get('template_id')

                logger.debug('Successfully upload_m7_biometry_service: %s', file_id)
                return file_id
        except Exception as ex:
            logger.exception('Error upload_m7_biometry_service: %s', ex)
            raise


    async def get_album_id_by_person_id(self, person_id: str, name: str) -> str:
        try:
            m7_profile_album_url = await self.get_url(ENDPOINT_M7_PROFILE_ALBUM)
            logger.debug('Try get album_id by url: %s', m7_profile_album_url)
            try:
                async with self._service_token.create_client(m7_profile_album_url) as service_client:
                    result = await service_client.get([person_id])
                    album_id = result.get(person_id)
                    if not album_id:
                        raise NotFound
                    return album_id
            except NotFound:
                logger.debug('NotFound album_id, try to create new album')

            m7_photo_album_url = await self.get_url(ENDPOINT_M7_PHOTO_ALBUM)
            logger.debug('Try add photo-album by url: %s', m7_photo_album_url)

            album_datat = {
                M7_PHOTO_ALBUM_NAME: name
            }

            async with self._service_token.create_client(m7_photo_album_url) as service_client:
                album_id = await service_client.add(album_datat)
                return album_id

        except Exception as ex:
            logger.exception('Error get_album_id_by_person_id: %s', ex)
            raise

    async def assign_person_id_to_album_id(self, person_id: str, album_id: str):
        try:
            m7_profile_album_url = await self.get_url(ENDPOINT_M7_PROFILE_ALBUM)
            logger.debug('Try assign person_id to album_id by url: %s', m7_profile_album_url)
            try:
                async with self._service_token.create_client(
                        endpoint=m7_profile_album_url,
                        jsonrpc_codes={AlreadyExists.code: AlreadyExists}
                ) as service_client:
                    await service_client.assign(object_id=person_id, album_id=album_id)
                    logger.debug('Successfully assigned')
            except AlreadyExists:
                return
        except Exception as ex:
            logger.exception('Error assign_person_id_to_album_id: %s', ex)
            raise


    async def upload_m7_photo_album(self,
                                    file_bytes: bytes,
                                    album_id: str,
                                    file_name: str
                                    ):
        try:
            m7_upload_photo_album_url = await self.get_url(ENDPOINT_M7_PHOTO_ALBUM_UPLOAD)
            m7_upload_photo_album_url = '{}/{}'.format(m7_upload_photo_album_url,
                                                       album_id)
            logger.debug('Try upload file img by url: %s', m7_upload_photo_album_url)
            form_data = FormData()
            form_data.add_field(
                'file',
                file_bytes,
                filename=file_name,
                content_type='image/jpeg'
            )

            headers = self._service_token.client_builder.headers
            async with ClientSession() as client:
                response = await client.post(
                    url=m7_upload_photo_album_url,
                    data=form_data,
                    headers=headers
                )
                resp_json_bytes = await response.content.read()
                resp_json = json.loads(resp_json_bytes.decode())

                if not resp_json.get('file_id'):
                    raise UtilityError('Error upload m7_photo_album: %s', resp_json)

                file_id = resp_json.get('file_id')

                logger.debug('Successfully upload m7_photo_album service: %s', file_id)
                return file_id

        except Exception as ex:
            logger.exception('Error upload_m7_photo_album: %s', ex)
            raise


    async def get_file_bytes(self, download_url: str) -> bytes:
        """
        Get file bytes from m7-files service
        Args:
            download_url: full url for download file
        Returns:
            bytes
        """

        async with self._service_token.create_client_session() as service_client:
            response = await service_client.get(url=download_url)
            file_bytes = await response.content.read()
            return file_bytes


    async def check_biorecord_photos(self,
                                        templates_list: list,
                                        sdk_type: str,
                                        check_config: dict
                                        ):
        try:
            stations_client_url = await self.get_url(ENDPOINT_ID_STATIONS_CLIENT)
            logger.debug('Try to check_biorecord_photos by sdk: %s, by url: %s',
                         sdk_type, stations_client_url)

            async with self._service_token.create_client(stations_client_url) as service_client:
                result = await service_client.check_biorecord_photos(
                    sdk_type=sdk_type,
                    photos=templates_list
                )
                logger.debug('Got result: %s', result)

                if check_config['badImages'] and 0 in result['badImages']:
                    raise UtilityError('IdBioxidBadQualityOfImage')
                if check_config['mismatchImages'] and 0 in result['mismatchImages']:
                    raise UtilityError('IdBioxidMismatchImage')
                if check_config['duplicateImages'] and 0 in result['duplicateImages']:
                    raise UtilityError('IdBioxidDuplicateImage')

        except Exception as ex:
            logger.exception('Error check_biorecord_photos: %s', ex)
            raise


    async def get_photo_list_by_filter(self, album_id: str):
        try:
            m7_photo_album_url = await self.get_url(ENDPOINT_M7_PHOTO_ALBUM)

            filter_photo_album = {
                M7_PHOTO_ALBUM_ID: {
                    'values': [album_id]
                }
            }
            async with self._service_token.create_client(m7_photo_album_url) as service_client:
                return await service_client.get_photo_list_by_filter(
                    filter=filter_photo_album,
                    order=[],
                    limit=100,
                    offset=0
                )

        except Exception as ex:
            logger.exception('Error get_photo_list_by_filter: %s', ex)
            raise
