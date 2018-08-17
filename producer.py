import os
from logging import getLogger

from tatau_core import settings
from tatau_core.tatau.node.producer import Producer
from tatau_core.utils.logging import configure_logging

configure_logging('producer')

logger = getLogger()


def load_credentials(account_address_var_name):
    address = os.getenv(account_address_var_name)
    if address is None:
        raise ValueError('{} is not specified'.format(account_address_var_name))

    storage_path = settings.KEYS_PATH
    dir_name = address.replace('0x', '')
    with open(os.path.join(storage_path, dir_name, 'rsa_pk.pem'), 'r') as f:
        pk = f.read()

    return address, pk.encode()


def load_producer():
    account_address, rsa_pk = load_credentials(
        account_address_var_name='PRODUCER_ACCOUNT_ADDRESS'
    )

    p = Producer(
        account_address=account_address,
        rsa_pk=rsa_pk
    )

    logger.info('Load {}, account_address: {}'.format(p.asset, p.asset.account_address))
    return p


if __name__ == '__main__':
    producer = load_producer()

    if os.getenv('USE_SOCKET', False):
        producer.run_transaction_listener()
    else:
        producer.process_tasks()
