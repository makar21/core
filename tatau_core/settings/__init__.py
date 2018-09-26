# noinspection PyPackageRequirements
import tempfile

from dotenv import load_dotenv, find_dotenv
import pathlib
import os

load_dotenv(find_dotenv(), override=True)

DEBUG = os.getenv('TATAU_DEBUG').lower() == 'true'

RAVEN_DSN = os.getenv(
    'TATAU_RAVEN_DSN', 'https://390106f50045440aa9975cc760b202be:e583850ec1cd4908bdce1144ee1a1452@sentry.io/1264808')

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

GPU_TFLOPS = float(os.getenv('GPU_TFLOPS', 11.64))
CPU_TFLOPS = float(os.getenv('CPU_TFLOPS', 0.14))

RING_NAME = os.getenv('RING', '')

PARITY_JSONRPC_PORT = int(os.getenv('PARITY_JSONRPC_PORT', 8545))
PARITY_WEBSOCKET_PORT = int(os.getenv('PARITY_WEBSOCKET_PORT', 8546))

PARITY_HOST = os.getenv('PARITY_HOST', 'parity')
NET = os.getenv('NET', 'sandbox')

POA_DEFAULTS = {
    'sandbox': {
        # Sandbox v4
        'CONTRACT_ADDRESS': '0x815d9f345e2f5b20b63650961b68775ce936f408',
    }
}

IPFS_DEFAULTS = {
    'sandbox': {
        'IPFS_GATEWAY_HOST': 'sandbox.ipfs.tatau.io'
    }
}

IPFS_GATEWAY_HOST = IPFS_DEFAULTS[NET]['IPFS_GATEWAY_HOST']

CONTRACT_ADDRESS = POA_DEFAULTS[NET]['CONTRACT_ADDRESS']

TFLOPS_COST = float(os.getenv('TFLOPS_COST', 0.002036400662))

KEYS_PATH = os.path.join(os.getenv('KEYS_ROOT'), NET)

WHITELIST_JSON_PATH = os.getenv(
    'WHITELIST_JSON_PATH',
    os.path.join(KEYS_PATH, "whitelist.json")
)

DOWNLOAD_POOL_SIZE = int(os.getenv('DOWNLOAD_POOL_SIZE', 16))

WAIT_ESTIMATE_TIMEOUT = int(os.getenv('WAIT_ESTIMATE_TIMEOUT', 600))
WAIT_TRAIN_TIMEOUT = int(os.getenv('WAIT_TRAIN_TIMEOUT', 1800))
WAIT_VERIFY_TIMEOUT = int(os.getenv('WAIT_VERIFY_TIMEOUT', 1800))


TATAU_STORAGE_BASE_DIR = os.path.join(tempfile.gettempdir(), 'tatau')

PERFORM_BENCHMARK = False
