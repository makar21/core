import json
import time
from logging import getLogger

from tatau_core import settings
from tatau_core.models import TaskDeclaration, EstimationAssignment
from tatau_core.models.estimation import EstimationResult
from tatau_core.nn.tatau.sessions.estimation import EstimationSession
from tatau_core.node import Node

logger = getLogger()


# noinspection PyMethodMayBeStatic
class Estimator(Node):
    # estimator is not stand alone role
    asset_class = Node

    def _process_task_declaration(self, task_declaration: TaskDeclaration):
        if task_declaration.state == TaskDeclaration.State.ESTIMATE_IS_REQUIRED \
                and task_declaration.estimators_needed > 0:

            logger.info('Process {}'.format(task_declaration))
            exists = EstimationAssignment.exists(
                additional_match={
                    'assets.data.estimator_id': self.asset_id,
                    'assets.data.task_declaration_id': task_declaration.asset_id,
                },
                created_by_user=False,
                db=self.db
            )

            if exists:
                logger.debug('{} has already created estimation assignment for {}'.format(self, task_declaration))
                return

            estimation_assignment = EstimationAssignment.create(
                estimator_id=self.asset_id,
                producer_id=task_declaration.producer_id,
                task_declaration_id=task_declaration.asset_id,
                db=self.db,
                encryption=self.encryption
            )

            estimation_result = EstimationResult.create(
                estimation_assignment_id=estimation_assignment.asset_id,
                # share data with producer
                public_key=estimation_assignment.producer.enc_key,
                db=self.db,
                encryption=self.encryption
            )

            estimation_assignment.estimation_result_id = estimation_result.asset_id
            estimation_assignment.state = EstimationAssignment.State.READY
            # give ownership to producer
            estimation_assignment.save(recipients=task_declaration.producer.address)

            logger.info('Added {}'.format(estimation_assignment))

    def _process_estimation_assignment(self, estimation_assignment: EstimationAssignment):
        if estimation_assignment.task_declaration.in_finished_state:
            return

        logger.debug('{} process {} state:{}'.format(self, estimation_assignment, estimation_assignment.state))

        if estimation_assignment.state == EstimationAssignment.State.ESTIMATING:
            if estimation_assignment.estimation_result.state != EstimationResult.State.FINISHED:
                self._estimate(estimation_assignment)
            return

        if estimation_assignment.state == EstimationAssignment.State.REASSIGN:
            estimation_assignment.state = EstimationAssignment.State.INITIAL
            estimation_assignment.save(recipients=estimation_assignment.task_declaration.producer.address)
            return

    def _estimate(self, estimation_assignment: EstimationAssignment):
        logger.info('Start of estimation for {}'.format(estimation_assignment.task_declaration))

        session = EstimationSession()

        try:
            try:
                session.process_assignment(assignment=estimation_assignment)
            except Exception as e:
                estimation_assignment.estimation_result.error = json.dumps(self._parse_exception(ex))
                logger.exception(e)

            estimation_assignment.estimation_result.tflops = session.get_tflops()
            estimation_assignment.estimation_result.progress = 100.0
            estimation_assignment.estimation_result.state = EstimationResult.State.FINISHED
            estimation_assignment.estimation_result.save()

            logger.info('End of estimation for {}, tflops: {}, error: {}'.format(
                estimation_assignment.task_declaration,
                estimation_assignment.estimation_result.tflops,
                estimation_assignment.estimation_result.error))

        finally:
            session.clean()

    def _process_task_declarations(self):
        task_declarations = TaskDeclaration.enumerate(created_by_user=False, db=self.db, encryption=self.encryption)
        for task_declaration in task_declarations:
            try:
                self._process_task_declaration(task_declaration)
            except Exception as ex:
                logger.exception(ex)

    def _process_estimation_assignments(self):
        for estimation_assignment in EstimationAssignment.enumerate(db=self.db, encryption=self.encryption):
            try:
                self._process_estimation_assignment(estimation_assignment)
            except Exception as ex:
                logger.exception(ex)

    def search_tasks(self):
        while True:
            try:
                self._process_task_declarations()
                self._process_estimation_assignments()
                time.sleep(settings.WORKER_PROCESS_INTERVAL)
            except Exception as ex:
                logger.exception(ex)
