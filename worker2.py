import logging

from tatau_core.tatau.node.worker import Worker
from tatau_core.utils.logging import configure_logging

configure_logging('worker2')

log = logging.getLogger()


if __name__ == '__main__':
    worker = Worker(rsa_pk_fs_name='worker2')
    worker.search_tasks()
    # worker.run_transaction_listener()
