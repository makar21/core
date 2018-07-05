import logging

from tatau.node.worker import Worker
from utils.logging import configure_logging

configure_logging('worker')

logger = logging.getLogger()

if __name__ == '__main__':
    try:
        w = Worker(rsa_pk_fs_name='worker')
        w.run_transaction_listener()
    except Exception as ex:
        logger.fatal(ex)
