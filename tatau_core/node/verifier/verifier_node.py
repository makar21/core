import json
import time
from logging import getLogger

from tatau_core import settings
from tatau_core.contract import poa_wrapper
from tatau_core.models import VerifierNode, TaskDeclaration, VerificationAssignment
from tatau_core.models.verification import VerificationResult, DistributeHistory
from tatau_core.nn.tatau.sessions.eval_verification import VerificationEvalSession
from tatau_core.nn.tatau.sessions.summarize import SummarizeSession
from tatau_core.node.node import Node
from tatau_core.utils.ipfs import Downloader

logger = getLogger()


# noinspection PyMethodMayBeStatic
class Verifier(Node):

    asset_class = VerifierNode

    def _process_task_declaration(self, task_declaration):
        if task_declaration.in_finished_state:
            self._finish_job(task_declaration)
            Downloader(task_declaration.asset_id).remove_storage()
            return

        if task_declaration.state in [TaskDeclaration.State.DEPLOYMENT, TaskDeclaration.State.DEPLOYMENT_VERIFICATION] \
                and task_declaration.verifiers_needed > 0:

            exists = VerificationAssignment.exists(
                additional_match={
                    'assets.data.verifier_id': self.asset_id,
                    'assets.data.task_declaration_id': task_declaration.asset_id,
                },
                created_by_user=True,
                db=self.db
            )

            if exists:
                logger.debug('{} has already created verification assignment to {}'.format(self, task_declaration))
                return

            verification_assignment = VerificationAssignment.create(
                producer_id=task_declaration.producer_id,
                verifier_id=self.asset_id,
                task_declaration_id=task_declaration.asset_id,
                db=self.db,
                encryption=self.encryption
            )

            distribute_history = DistributeHistory.create(
                task_declaration_id=task_declaration.asset_id,
                verification_assignment_id=verification_assignment.asset_id,
                db=self.db,
                encryption=self.encryption
            )

            verification_result = VerificationResult.create(
                verification_assignment_id=verification_assignment.asset_id,
                public_key=verification_assignment.producer.enc_key,
                db=self.db,
                encryption=self.encryption
            )

            verification_assignment.verification_result_id = verification_result.asset_id
            verification_assignment.distribute_history_id = distribute_history.asset_id
            verification_assignment.state = VerificationAssignment.State.READY
            verification_assignment.save(recipients=task_declaration.producer.address)

            logger.info('Added {}'.format(verification_assignment))

    def _process_verification_assignment(self, verification_assignment):
        if verification_assignment.task_declaration.in_finished_state:
            return

        if verification_assignment.state == VerificationAssignment.State.REASSIGN:
            verification_assignment.state = VerificationAssignment.State.READY
            # give ownership to producer
            verification_assignment.save(recipients=verification_assignment.producer.address)
            return

        if verification_assignment.state == VerificationAssignment.State.VERIFYING:
            if verification_assignment.verification_result.state == VerificationResult.State.VERIFICATION_FINISHED:
                self._distribute(verification_assignment)
                return

            if not verification_assignment.iteration_is_finished:
                self._verify(verification_assignment)
                return

    def _distribute(self, verification_assignment):
        task_declaration = verification_assignment.task_declaration
        if task_declaration.balance_in_wei < task_declaration.iteration_cost_in_wei:
            logger.info(
                'Cant distribute iteration, {} does not have enough balance, job balance: {}, iteration cost: {}'.format(
                    task_declaration, task_declaration.balance, task_declaration.iteration_cost))
            return

        poa_wrapper.distribute(task_declaration, verification_assignment)
        if task_declaration.last_iteration:
            poa_wrapper.finish_job(verification_assignment.task_declaration)

        verification_assignment.verification_result.state = VerificationResult.State.FINISHED
        verification_assignment.verification_result.save()

    def _finish_job(self, task_declaration):
        if not poa_wrapper.does_job_exist(task_declaration):
            return

        if poa_wrapper.does_job_finished(task_declaration):
            return

        verification_assignments = VerificationAssignment.list(
            additional_match={
                'assets.data.task_declaration_id': task_declaration.asset_id
            },
            created_by_user=True,
            db=self.db,
            encryption=self.encryption
        )

        # task canceled before train
        if len(verification_assignments) == 0:
            poa_wrapper.finish_job(task_declaration)
            return

        # TODO: support multiple verification
        assert len(verification_assignments) == 1
        verification_assignment = verification_assignments[0]

        # pay to workers if verification was failed
        poa_wrapper.distribute(task_declaration, verification_assignment)
        poa_wrapper.finish_job(task_declaration)

    def _dump_error(self, assignment, ex: Exception):
        assignment.verification_result.error = json.dumps(self._parse_exception(ex))
        assignment.verification_result.state = VerificationResult.State.FINISHED
        assignment.verification_result.save()
        logger.exception(ex)

    def _run_verification_session(self, verification_assignment: VerificationAssignment):
        # verifier repository can be absent
        from verifier.session import VerifySession
        return self._run_session(verification_assignment, session=VerifySession())

    def _is_fake_worker_present(self, verification_assignment):
        for result in verification_assignment.verification_result.result:
            if result['is_fake']:
                return True
        return False

    def _verify(self, verification_assignment: VerificationAssignment):
        verification_assignment.verification_result.clean()
        verification_assignment.verification_result.current_iteration = \
            verification_assignment.verification_data.current_iteration
        verification_assignment.verification_result.current_iteration_retry = \
            verification_assignment.verification_data.current_iteration_retry
        verification_assignment.verification_result.state = VerificationResult.State.IN_PROGRESS
        verification_assignment.verification_result.save()

        task_declaration = verification_assignment.task_declaration
        logger.info('Start of verification for {}'.format(task_declaration))
        if task_declaration.balance_in_wei < task_declaration.iteration_cost_in_wei:
            logger.info('Ignore {}, does not have enough balance.'.format(task_declaration))
            return

        failed, verify_tflops = self._run_verification_session(verification_assignment)
        if failed:
            return

        summarize_tflops = 0.0
        if not self._is_fake_worker_present(verification_assignment):
            failed, summarize_tflops = self._run_session(verification_assignment, session=SummarizeSession())
            if failed:
                self._distribute(verification_assignment)
                return

        eval_tflops = 0.0
        if task_declaration.last_iteration:
            failed, eval_tflops = self._run_session(verification_assignment, session=VerificationEvalSession())
            if failed:
                self._distribute(verification_assignment)
                return

        verification_assignment.verification_result.progress = 100.0
        verification_assignment.verification_result.tflops = verify_tflops + summarize_tflops + eval_tflops
        verification_assignment.verification_result.state = VerificationResult.State.VERIFICATION_FINISHED
        verification_assignment.verification_result.save()

        logger.info('End of verification for {} results: {}'.format(
            task_declaration, verification_assignment.verification_result.result))

        self._distribute(verification_assignment)

    def _process_task_declarations(self):
        task_declarations = TaskDeclaration.enumerate(created_by_user=False, db=self.db, encryption=self.encryption)
        for task_declaration in task_declarations:
            try:
                self._process_task_declaration(task_declaration)
            except Exception as ex:
                logger.exception(ex)

    def _process_verification_assignments(self):
        for verification_assignment in VerificationAssignment.enumerate(db=self.db, encryption=self.encryption):
            try:
                self._process_verification_assignment(verification_assignment)
            except Exception as ex:
                logger.exception(ex)

    def search_tasks(self):
        while True:
            try:
                self._process_task_declarations()
                self._process_verification_assignments()
                time.sleep(settings.VERIFIER_PROCESS_INTERVAL)
            except Exception as ex:
                logger.exception(ex)
