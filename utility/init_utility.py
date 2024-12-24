import argparse
import json
import os
import logging
import logging.config

logger = logging.getLogger('biometry_utility')


def get_config():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--config',
            help='M7 configuration file name',
            type=str,
            default='utility_conf.json')
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


def init_log(config: dict):

    conf_logging = config['logging']
    log_path = conf_logging['handlers']['file']['filename']

    log_dir = os.path.dirname(log_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.config.dictConfig(conf_logging)
    logger.debug('Successfully log init')
