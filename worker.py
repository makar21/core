import os
import sys
from logging import getLogger

from tatau_core import settings
from tatau_core.tatau.node.worker import Worker
from tatau_core.utils.logging import configure_logging

configure_logging('worker')

logger = getLogger()


def load_credentials(account_address_var_name):
    address = os.getenv(account_address_var_name)
    if address is None:
        raise ValueError('{} is not specified'.format(account_address_var_name))

    storage_path = settings.KEYS_PATH
    dir_name = address.replace('0x', '')
    with open(os.path.join(storage_path, dir_name, 'rsa_pk.pem'), 'r') as f:
        pk = f.read()

    return address, pk.encode()


if __name__ == '__main__':
    try:
        index = '_{}'.format(sys.argv[1])
    except IndexError:
        index = ''

    account_address, rsa_pk = load_credentials(
        account_address_var_name='WORKER_ACCOUNT_ADDRESS{}'.format(index),
    )

    worker = Worker(
        account_address=account_address,
        rsa_pk=rsa_pk
    )

    logger.info('Start {} address: {}'.format(worker.asset, worker.asset.account_address))
    if os.getenv('USE_SOCKET', False):
        worker.run_transaction_listener()
    else:
        worker.search_tasks()

