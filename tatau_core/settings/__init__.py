import pathlib

from . import local_settings

DEBUG = local_settings.DEBUG

PRODUCER_HOST = 'localhost'
PRODUCER_PORT = '8080'

RAVEN_DSN = local_settings.RAVEN_DSN

ROOT_DIR = pathlib.Path(__file__).parents[2]

WORKER_PROCESS_OLD_TASKS_INTERVAL = 300
VERIFIER_PROCESS_OLD_TASKS_INTERVAL = 300

VALID_TRANSACTIONS_STREAM_URL = local_settings.VALID_TRANSACTIONS_STREAM_URL

BDB_ROOT_URL = local_settings.BDB_ROOT_URL

MONGO_DB_HOST = local_settings.MONGO_DB_HOST
MONGO_DB_PORT = local_settings.MONGO_DB_PORT

IPFS_HOST = local_settings.IPFS_HOST
IPFS_PORT = local_settings.IPFS_PORT
