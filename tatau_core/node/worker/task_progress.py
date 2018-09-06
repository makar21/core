from logging import getLogger

from tatau_core.nn.tatau.model import TrainProgress
from tatau_core.models import TrainResult

logger = getLogger()


class TaskProgress(TrainProgress):
    def __init__(self, worker, asset_id, interprocess):
        self.worker = worker
        self.asset_id = asset_id
        self.interprocess = interprocess

    def progress_callback(self, progress):
        train_result = TrainResult.get(self.asset_id, self.worker.db, self.worker.encryption)

        logger.debug('{} progress is {}'.format(train_result.task_assignment, progress))

        # share with producer
        train_result.set_encryption_key(train_result.task_assignment.producer.enc_key)
        train_result.progress = progress
        train_result.tflops = self.interprocess.get_tflops()
        train_result.save()
