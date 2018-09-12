import os
import sys
from logging import getLogger

from tatau_core import settings
from tatau_core.contract import NodeContractInfo
from tatau_core.node import VerifierEstimator
from tatau_core.utils.logging import configure_logging

configure_logging('verifier')

logger = getLogger()


def load_credentials(account_address_var_name):
    address = os.getenv(account_address_var_name)
    if address is None:
        raise ValueError('{} is not specified'.format(account_address_var_name))

    storage_path = settings.KEYS_PATH
    dir_name = address.replace('0x', '')
    with open(os.path.join(storage_path, dir_name, 'rsa_pk.pem'), 'r') as f:
        pk = f.read()

    with open(os.path.join(storage_path, dir_name, 'wallet.json'), 'r') as f:
        wallet = f.read()

    with open(os.path.join(storage_path, dir_name, 'wallet.pass'), 'r') as f:
        wallet_password = f.read()

    return address, wallet, wallet_password, pk.encode()


if __name__ == '__main__':
    try:
        index = '_{}'.format(sys.argv[1])
    except IndexError:
        index = ''

    account_address, encrypted_key, password, rsa_pk = load_credentials(
        account_address_var_name='VERIFIER_ACCOUNT_ADDRESS{}'.format(index)
    )

    while True:
        try:
            NodeContractInfo.configure(encrypted_key, password)

            verifier = VerifierEstimator(
                account_address=account_address,
                rsa_pk=rsa_pk
            )

            logger.info('Start {}, address {}'.format(verifier.asset, verifier.asset.account_address))
            verifier.search_tasks()
        except Exception as ex:
            logger.info(ex)
