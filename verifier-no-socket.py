import logging

from tatau_core.tatau.node import Verifier
from tatau_core.utils.logging import configure_logging

configure_logging('verifier-no-socket')

log = logging.getLogger()


if __name__ == '__main__':
    verifier = Verifier(rsa_pk_fs_name='verifier-no-socket')
    verifier.search_tasks()
