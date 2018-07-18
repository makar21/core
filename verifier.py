import logging

from tatau_core.tatau.node import Verifier
from tatau_core.utils.logging import configure_logging

configure_logging('verifier')

log = logging.getLogger()


if __name__ == '__main__':
    try:
        verifier = Verifier(rsa_pk_fs_name='verifier')
        verifier.run_transaction_listener()
    except Exception as e:
        log.fatal(e)
