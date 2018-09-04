from logging import getLogger

from tatau_core.nn.tatau.model import TrainProgress
from tatau_core.models import TaskAssignment

logger = getLogger()


class TaskProgress(TrainProgress):
    def __init__(self, worker, asset_id, interprocess):
        self.worker = worker
        self.asset_id = asset_id
        self.interprocess = interprocess

    def progress_callback(self, progress):
        task_assignment = TaskAssignment.get(self.asset_id, self.worker.db, self.worker.encryption)

        logger.debug('{} progress is {}'.format(task_assignment, progress))

        task_assignment.set_encryption_key(task_assignment.producer.enc_key)
        task_assignment.progress = progress
        task_assignment.tflops = self.interprocess.get_tflops()
        task_assignment.save()
