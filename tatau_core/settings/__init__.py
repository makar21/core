# noinspection PyPackageRequirements
from dotenv import load_dotenv, find_dotenv
import pathlib
import os

load_dotenv(find_dotenv(), override=True)


DEBUG = os.getenv('TATAU_DEBUG').lower() == 'true'

RAVEN_DSN = os.getenv('TATAU_RAVEN_DSN')

BDB_HOST = os.getenv('TATAU_DBD_HOST', 'bigchaindb')

ROOT_DIR = pathlib.Path(__file__).parents[2]

WORKER_PROCESS_OLD_TASKS_INTERVAL = 300
VERIFIER_PROCESS_OLD_TASKS_INTERVAL = 300

VALID_TRANSACTIONS_STREAM_URL = 'ws://{}:9985/api/v1/streams/valid_transactions'.format(BDB_HOST)

BDB_ROOT_URL = 'http://{}:9984'.format(BDB_HOST)


MONGO_DB_HOST = os.getenv('TATAU_MONGO_DB_HOST', 'mongodb')
MONGO_DB_PORT = int(os.getenv('TATAU_MONGO_DB_PORT', 27017))

IPFS_HOST = os.getenv('TATAU_IPFS_HOST', 'ipfs')
IPFS_PORT = int(os.getenv('TATAU_IPFS_PORT', 5001))


PRODUCER_PROCESS_INTERVAL = int(os.getenv('PRODUCER_PROCESS_INTERVAL', 5))
WORKER_PROCESS_INTERVAL = int(os.getenv('WORKER_PROCESS_INTERVAL', 5))
VERIFIER_PROCESS_INTERVAL = int(os.getenv('VERIFIER_PROCESS_INTERVAL', 5))

GPU_TFLOPS = float(os.getenv('GPU_TFLOPS', 1.455))
CPU_TFLOPS = float(os.getenv('CPU_TFLOPS', 0.14))

RING_NAME = os.getenv('RING', '')

PARITY_JSONRPC_PORT = int(os.getenv('PARITY_JSONRPC_PORT', 8545))
PARITY_WEBSOCKET_PORT = int(os.getenv('PARITY_WEBSOCKET_PORT', 8546))
PARITY_HOST = os.getenv('PARITY_HOST', 'parity')

CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS')