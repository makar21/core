import pathlib

from . import local_settings

DEBUG = local_settings.DEBUG

PRODUCER_HOST = 'localhost'
PRODUCER_PORT = '8080'

RAVEN_DSN = local_settings.RAVEN_DSN

ROOT_DIR = pathlib.Path(__file__).parents[2]
