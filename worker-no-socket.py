import os
import sys
from logging import getLogger

from tatau_core.tatau.node.worker import Worker
from tatau_core.utils.logging import configure_logging

configure_logging('worker-no-socket')

logger = getLogger()


if __name__ == '__main__':
    try:
        index = sys.argv[1]
    except IndexError:
        index = ''

    account_address = os.getenv('ACCOUNT_ADDRESS')
    if account_address is None:
        logger.error('ACCOUNT_ADDRESS is not specified')
        exit(-1)

    worker = Worker(
        account_address=account_address,
        rsa_pk_fs_name='worker-no-socket{}'.format(index)
    )

    logger.info('Start {} address: {}'.format(worker.asset, account_address))
    worker.search_tasks()

