import logging
import time
from multiprocessing import Process

from tatau_core import settings
from tatau_core.tatau.node import Verifier
from tatau_core.utils.logging import configure_logging

configure_logging('verifier')

log = logging.getLogger()


def process_old_verification_declarations(verifier_node):
    while True:
        verifier_node.process_old_verification_declarations()
        time.sleep(settings.VERIFIER_PROCESS_OLD_TASKS_INTERVAL)


if __name__ == '__main__':
    try:
        verifier = Verifier(rsa_pk_fs_name='verifier')

        # process_class = Process
        # if settings.DEBUG:
        #     import threading
        #
        #     process_class = threading.Thread
        #
        # process_old_tasks_process = process_class(target=process_old_verification_declarations, args=(verifier,))
        # process_old_tasks_process.start()

        verifier.run_transaction_listener()
    except Exception as e:
        log.fatal(e)
