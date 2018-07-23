import logging
import sys

from tatau_core.tatau.node.worker import Worker
from tatau_core.utils.logging import configure_logging

configure_logging('worker')

log = logging.getLogger()


if __name__ == '__main__':
    try:
        index = sys.argv[1]
    except IndexError:
        index = ''
    worker = Worker(rsa_pk_fs_name='worker{}'.format(index))
    worker.run_transaction_listener()
