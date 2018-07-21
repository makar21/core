import logging

from tatau_core.tatau.node.producer import Producer
from tatau_core.utils.logging import configure_logging

configure_logging('producer')

log = logging.getLogger()


if __name__ == '__main__':
    producer = Producer(rsa_pk_fs_name='producer')
    log.debug('Start {}'.format(producer))
    producer.run_transaction_listener()