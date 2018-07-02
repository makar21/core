import logging

from tatau.node import Verifier
from utils.logging import configure_logging

configure_logging('verifier')

logger = logging.getLogger()


if __name__ == '__main__':
    try:
        v = Verifier(fs_keys_name='verifier')
        v.run_transaction_listener()
    except Exception as e:
        logger.fatal(e)
