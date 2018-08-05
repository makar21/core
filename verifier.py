import sys
from logging import getLogger

from tatau_core.tatau.node.verifier import Verifier
from tatau_core.utils.logging import configure_logging

configure_logging('verifier')

logger = getLogger()


if __name__ == '__main__':
    try:
        index = sys.argv[1]
    except IndexError:
        index = ''
    verifier = Verifier(rsa_pk_fs_name='verifier{}'.format(index))
    logger.info('Start {}'.format(verifier.asset))
    verifier.run_transaction_listener()
