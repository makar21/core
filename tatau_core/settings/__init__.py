# noinspection PyPackageRequirements
from dotenv import load_dotenv, find_dotenv
import pathlib
import os

load_dotenv(find_dotenv(), override=True)


DEBUG = bool(os.getenv('TATAU_DEBUG'))

RAVEN_DSN = os.getenv('TATAU_RAVEN_DSN')

BDB_HOST = os.getenv('TATAU_DBD_HOST')

ROOT_DIR = pathlib.Path(__file__).parents[2]

WORKER_PROCESS_OLD_TASKS_INTERVAL = 300
VERIFIER_PROCESS_OLD_TASKS_INTERVAL = 300

VALID_TRANSACTIONS_STREAM_URL = 'ws://{}:9985/api/v1/streams/valid_transactions'.format(BDB_HOST)

BDB_ROOT_URL = 'http://{}:9984'.format(BDB_HOST)

MONGO_DB_HOST = os.getenv('TATAU_MONGO_DB_HOST')
MONGO_DB_PORT = os.getenv('TATAU_MONGO_DB_PORT')

IPFS_HOST = local_settings.IPFS_HOST
IPFS_PORT = local_settings.IPFS_PORT


PRODUCER_PROCESS_INTERVAL = 5
WORKER_PROCESS_INTERVAL = 5
VERIFIER_PROCESS_INTERVAL = 5
