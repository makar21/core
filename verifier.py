import logging

from tatau_core.tatau.node import Verifier
from tatau_core.utils.logging import configure_logging

configure_logging('verifier')

logger = logging.getLogger()


if __name__ == '__main__':
    try:
        v = Verifier(rsa_pk_fs_name='verifier')
        v.run_transaction_listener()
    except Exception as e:
        logger.fatal(e)
