import sys
from logging import getLogger

from tatau_core.contract import NodeContractInfo
from tatau_core.tatau.node import WorkerEstimator
from tatau_core.utils.logging import configure_logging

configure_logging('worker-estimator-no-socket')

logger = getLogger()


if __name__ == '__main__':
    try:
        index = sys.argv[1]
    except IndexError:
        index = ''

    NodeContractInfo.init_poa(key_name='worker')
    worker = WorkerEstimator(
        account_address=NodeContractInfo.get_account_address(),
        rsa_pk_fs_name='worker-estimator-no-socket{}'.format(index)
    )

    logger.info('Start {}'.format(worker.asset))
    worker.search_tasks()

