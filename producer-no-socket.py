from logging import getLogger

from tatau_core.contract import NodeContractInfo
from tatau_core.tatau.node.producer import Producer
from tatau_core.utils.logging import configure_logging

configure_logging('producer')

logger = getLogger()


if __name__ == '__main__':
    NodeContractInfo.init_poa(key_name='producer')

    producer = Producer(
        account_address=NodeContractInfo.get_account_address(),
        rsa_pk_fs_name='producer'
    )

    logger.debug('Start {}'.format(producer))
    producer.process_tasks()
