import sys
from logging import getLogger

from tatau_core.tatau.node import WorkerEstimator
from tatau_core.utils.logging import configure_logging

configure_logging('worker-estimator')

logger = getLogger()


if __name__ == '__main__':
    try:
        index = sys.argv[1]
    except IndexError:
        index = ''
    worker = WorkerEstimator(rsa_pk_fs_name='worker-estimator{}'.format(index))
    logger.info('Start {}'.format(worker.asset))
    worker.run_transaction_listener()
