import os

from . import local_settings

DEBUG = local_settings.DEBUG

PRODUCER_HOST = 'localhost'
PRODUCER_PORT = '8080'

RAVEN_DSN = local_settings.RAVEN_DSN

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
