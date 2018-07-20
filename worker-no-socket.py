import logging

from tatau_core.tatau.node.worker import Worker
from tatau_core.utils.logging import configure_logging

configure_logging('worker-no-socket')

log = logging.getLogger()


if __name__ == '__main__':
    worker = Worker(rsa_pk_fs_name='worker-no-socket')
    worker.search_tasks()

