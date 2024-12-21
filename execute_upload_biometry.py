"""Main function for execute upload biometry"""
import argparse
import json
import os
import logging
import logging.config
import sys

global config_data

logger = logging.getLogger('biometry_utility')


async def execute_upload_biometry():
    global config_data

    try:
        print('execute_upload_biometry - into')
        config_data = get_config()
        print('global ', config_data)
        _init_log()

        pass
    except Exception as ex:
        print('Error execute_upload_biometry: ', ex)

def run_utility():
    global config_data
    print(config_data)
    pass

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