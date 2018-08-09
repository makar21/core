import json
import time
from logging import getLogger

from tatau_core import settings
from tatau_core.nn.tatau.sessions.estimation import EstimationSession
from tatau_core.tatau.models import TaskDeclaration, EstimationAssignment
from tatau_core.tatau.node import Node

logger = getLogger()


class Estimator(Node):
    # estimator is not stand alone role
    asset_class = Node

    def _get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self._process_task_declaration_transaction,
            EstimationAssignment.get_asset_name(): self._process_estimation_assignment_transaction,
        }

    def _process_task_declaration_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'TRANSFER':
            return

        task_declaration = TaskDeclaration.get(asset_id)
        logger.info('Received {}, estimators_needed: {}'.format(task_declaration, task_declaration.estimators_needed))
        if task_declaration.estimators_needed == 0:
            return

        self._process_task_declaration(task_declaration)

    def _process_task_declaration(self, task_declaration):
        if task_declaration.state == TaskDeclaration.State.ESTIMATE_IS_REQUIRED \
                and task_declaration.estimators_needed > 0:
            logger.info('Process {}'.format(task_declaration))
            exists = EstimationAssignment.exists(
                additional_match={
                    'assets.data.worker_id': self.asset_id,
                    'assets.data.task_declaration_id': task_declaration.asset_id,
                },
                created_by_user=False
            )

            if exists:
                logger.info('{} has already created estimation assignment for {}'.format(self, task_declaration))
                return

            estimation_assignment = EstimationAssignment.create(
                worker_id=self.asset_id,
                producer_id=task_declaration.producer_id,
                task_declaration_id=task_declaration.asset_id,
                recipients=task_declaration.producer.address
            )

            logger.info('Added {}'.format(estimation_assignment))

    def _process_estimation_assignment_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        estimation_assignment = EstimationAssignment.get(asset_id)

        # skip another assignment
        if estimation_assignment.worker_id != self.asset_id:
            return

        self._process_estimation_assignment(estimation_assignment)

    def _process_estimation_assignment(self, estimation_assignment):
        logger.debug('{} process {} state:{}'.format(self, estimation_assignment, estimation_assignment.state))

        if estimation_assignment.state == EstimationAssignment.State.IN_PROGRESS:
            if estimation_assignment.task_declaration.state == TaskDeclaration.State.ESTIMATE_IN_PROGRESS:
                estimation_assignment.state = EstimationAssignment.State.DATA_IS_READY

        if estimation_assignment.state == EstimationAssignment.State.RETRY:
            estimation_assignment.state = EstimationAssignment.State.INITIAL
            estimation_assignment.save(recipients=estimation_assignment.producer.address)
            return

        if estimation_assignment.state == EstimationAssignment.State.DATA_IS_READY:
            estimation_assignment.state = EstimationAssignment.State.IN_PROGRESS
            estimation_assignment.save()

            self._estimate(estimation_assignment.asset_id)

    # noinspection PyMethodMayBeStatic
    def _estimate(self, asset_id):
        logger.info('Start estimate process')
        estimation_assignment = EstimationAssignment.get(asset_id)

        session = EstimationSession()

        try:
            try:
                session.process_assignment(assignment=estimation_assignment)
            except Exception as e:
                error_dict = {'exception': type(e).__name__}
                msg = str(e)
                if msg:
                    error_dict['message'] = msg

                estimation_assignment.error = json.dumps(error_dict)
                logger.exception(e)

            estimation_assignment.tflops = session.get_tflops()
            estimation_assignment.state = EstimationAssignment.State.FINISHED
            estimation_assignment.set_encryption_key(estimation_assignment.producer.enc_key)
            estimation_assignment.save(recipients=estimation_assignment.producer.address)

            logger.info('Finished estimation {}, tflops: {}, error: {}'.format(
                estimation_assignment, estimation_assignment.tflops, estimation_assignment.error))
        finally:
            session.clean()

    def _process_task_declarations(self):
        for task_declaration in TaskDeclaration.enumerate(created_by_user=False):
            self._process_task_declaration(task_declaration)

    def _process_estimation_assignments(self):
        for estimation_assignment in EstimationAssignment.enumerate():
            self._process_estimation_assignment(estimation_assignment)

    def search_tasks(self):
        while True:
            try:
                self._process_task_declarations()
                self._process_estimation_assignments()
                time.sleep(settings.WORKER_PROCESS_INTERVAL)
            except Exception as ex:
                logger.exception(ex)
