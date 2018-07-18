import logging

from tatau_core.tatau.models import TaskAssignment

log = logging.getLogger()


class TaskProgress:
    def __init__(self, worker, asset_id, interprocess):
        self.worker = worker
        self.asset_id = asset_id
        self.interprocess = interprocess

    def progress_callback(self, progress):
        pass
        # task_assignment = TaskAssignment.get(self.asset_id, self.worker.db, self.worker.encryption)

        # log.debug('{} progress is {}'.format(task_assignment, progress))

        # task_assignment.set_encryption_key(task_assignment.producer.enc_key)
        # task_assignment.progress = progress
        # task_assignment.tflops = self.interprocess.get_tflops()
        # do not update very often asset
        # task_assignment.save()
