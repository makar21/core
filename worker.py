import logging
import time
from multiprocessing import Process

from tatau_core import settings
from tatau_core.tatau.node.worker import Worker
from tatau_core.utils.logging import configure_logging

configure_logging('worker')

log = logging.getLogger()


def process_old_task_declarations(worker_node):
    while True:
        worker_node.process_old_task_declarations()
        time.sleep(settings.WORKER_PROCESS_OLD_TASKS_INTERVAL)


if __name__ == '__main__':
    try:
        worker = Worker(rsa_pk_fs_name='worker')
        # read_old_tasks_process = Process()
        # process_class = Process
        # if settings.DEBUG:
        #     import threading
        #     process_class = threading.Thread
        #
        # process_old_tasks_process = process_class(target=process_old_task_declarations, args=(worker,))
        # process_old_tasks_process.start()

        worker.run_transaction_listener()
    except Exception as ex:
        log.fatal(ex)
