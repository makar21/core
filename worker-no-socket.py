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

    worker = Worker(rsa_pk_fs_name='worker-no-socket{}'.format(index))
    logger.info('Start {}'.format(worker.asset))
    worker.search_tasks()

