import logging
import time
from multiprocessing import Process

from tatau_core import settings
from tatau_core.tatau.node.worker import Worker
from tatau_core.utils.logging import configure_logging

configure_logging('worker')

log = logging.getLogger()


if __name__ == '__main__':
    worker = Worker(rsa_pk_fs_name='worker')
    worker.run_transaction_listener()
