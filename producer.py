from logging import getLogger

from tatau_core.tatau.node.producer import Producer
from tatau_core.utils.logging import configure_logging

configure_logging('producer')

logger = getLogger()


if __name__ == '__main__':
    producer = Producer(rsa_pk_fs_name='producer')
    logger.info('Start {}'.format(producer.asset))
    producer.run_transaction_listener()

