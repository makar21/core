import time
from logging import getLogger

from tatau_core import settings
from tatau_core.models import TaskDeclaration, VerifierNode
from tatau_core.node.estimator.estimator_node import Estimator
from tatau_core.node.verifier import Verifier

logger = getLogger()


class VerifierEstimator(Verifier, Estimator):

    asset_class = VerifierNode

    def _process_task_declaration(self, task_declaration):
        try:
            Estimator._process_task_declaration(self, task_declaration)
        except Exception as ex:
            logger.exception(ex)

        try:
            Verifier._process_task_declaration(self, task_declaration)
        except Exception as ex:
            logger.exception(ex)

    def _process_task_declarations(self):
        task_declarations = TaskDeclaration.enumerate(created_by_user=False, db=self.db, encryption=self.encryption)
        for task_declaration in task_declarations:
            try:
                self._process_task_declaration(task_declaration)
            except Exception as ex:
                logger.exception(ex)

    def search_tasks(self):
        while True:
            try:
                self._process_task_declarations()
                self._process_estimation_assignments()
                self._process_verification_assignments()
                time.sleep(settings.WORKER_PROCESS_INTERVAL)
            except Exception as ex:
                logger.exception(ex)