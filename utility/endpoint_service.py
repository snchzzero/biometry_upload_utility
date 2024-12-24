"""Service for execute requests"""
import json
import logging
from typing import List

from aiohttp import ClientSession, FormData
from m7_aiohttp.auth.service_token import AioHttpServiceToken
from m7_aiohttp.exceptions import NotFound
from m7_aiohttp.services.endpoints import AioHttpEndpointsPort

from const import ENDPOINT_ID_STATIONS_CLIENT, ENDPOINT_M7_PROFILE_ALBUM, \
    ENDPOINT_M7_PHOTO_ALBUM, M7_PHOTO_ALBUM_NAME
from utility.utility_exceptions import UtilityError

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
            raise UtilityError('Can''t add_data_m7_biometry: {}'.format(ex)) from ex


    async def get_template_by_image(self, image_buffer: dict, sdk_type: str):
        try:
            stations_client_url = await self.get_url(ENDPOINT_ID_STATIONS_CLIENT)
            logger.debug('Try to get template by sdk: %s, by url: %s', sdk_type, stations_client_url)
            #image_base64 = base64.b64encode(image_base64).decode('utf-8')

            async with self._service_token.create_client(stations_client_url) as service_client:
                template = await service_client.get_template_by_image(
                    sdk_type=sdk_type,
                    image_buffer=image_buffer
                )
                logger.debug('Successfully get_template_by_image')
                return template
        except Exception as ex:
            raise UtilityError('Can''t get_template_by_image: {}'.format(ex)) from ex


    async def upload_m7_biometry(self, url: str, form_data: FormData) -> str:
        try:
            logger.debug('Try to upload template by url: %s', url)
            headers = self._service_token.client_builder.headers
            logger.debug('form_data %s', form_data)
            logger.debug('type form_data %s', type(form_data))

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
