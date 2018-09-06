import json
import time
from logging import getLogger

from tatau_core import settings
from tatau_core.contract import poa_wrapper
from tatau_core.models import VerifierNode, TaskDeclaration, VerificationAssignment
from tatau_core.models.verification import VerificationResult, DistributeHistory
from tatau_core.nn.tatau.sessions.summarize import SummarizeSession
from tatau_core.node.node import Node
from verifier.session import VerifySession

logger = getLogger()


# noinspection PyMethodMayBeStatic
class Verifier(Node):

    asset_class = VerifierNode

    def _process_task_declaration(self, task_declaration):
        if task_declaration.is_in_finished_state():
            self._finish_job(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.DEPLOYMENT \
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
        if verification_assignment.task_declaration.is_in_finished_state():
            return

        if verification_assignment.state == VerificationAssignment.State.VERIFYING:
            if verification_assignment.verification_result.state == VerificationResult.State.VERIFICATION_FINISHED:
                self._distribute(verification_assignment)
                return

            if not verification_assignment.iteration_is_finished:
                self._verify(verification_assignment)
                return

    def _distribute(self, verification_assignment):
        if not verification_assignment.task_declaration.job_has_enough_balance():
            return

        # poa_wrapper.distribute(verification_assignment)
        # if verification_assignment.task_declaration.is_last_epoch():
        #     poa_wrapper.finish_job(verification_assignment.task_declaration)

        verification_assignment.verification_result.state = VerificationResult.State.FINISHED
        verification_assignment.verification_result.save()

    def _finish_job(self, task_declaration):
        if not poa_wrapper.does_job_exist(task_declaration):
            return

        if poa_wrapper.does_job_finished(task_declaration):
            return

        # TODO: support multiple verification
        verification_assignments = VerificationAssignment.list(
            additional_match={
                'assets.data.task_declaration_id': task_declaration.asset_id
            },
            created_by_user=False,
            db=self.db,
            encryption=self.encryption
        )

        # task canceled before train
        if len(verification_assignments) == 0:
            poa_wrapper.finish_job(task_declaration)
            return

        assert len(verification_assignments) == 1
        verification_assignment = verification_assignments[0]

        # pay to workers if verification was failed
        poa_wrapper.distribute(verification_assignment)
        poa_wrapper.finish_job(task_declaration)

    def _run_verification_session(self, verification_assignment: VerificationAssignment):
        failed = False

        session = VerifySession()
        try:
            session.process_assignment(assignment=verification_assignment)
        except Exception as e:
            error_dict = {'step': 'verification', 'exception': type(e).__name__}
            msg = str(e)
            if msg:
                error_dict['message'] = msg

            verification_assignment.verification_result.error = json.dumps(error_dict)
            verification_assignment.verification_result.state = VerificationResult.State.FINISHED
            verification_assignment.verification_result.save()
            logger.exception(e)
            failed = True
        finally:
            session.clean()

        return failed, session.get_tflops()

    def _run_summarize_session(self, verification_assignment: VerificationAssignment):
        failed = False

        session = SummarizeSession()
        try:
            session.process_assignment(assignment=verification_assignment)
        except Exception as e:
            error_dict = {'step': 'summarization', 'exception': type(e).__name__}
            msg = str(e)
            if msg:
                error_dict['message'] = msg

            verification_assignment.verification_result.error = json.dumps(error_dict)
            verification_assignment.verification_result.state = VerificationResult.State.FINISHED
            verification_assignment.verification_result.save()
            logger.exception(e)
            failed = True
        finally:
            session.clean()

        return failed, session.get_tflops()

    def _is_fake_worker_present(self, verification_assignment):
        for result in verification_assignment.verification_result.result:
            if result['is_fake']:
                return True
        return False

    def _verify(self, verification_assignment: VerificationAssignment):
        verification_assignment.verification_result.clean()
        verification_assignment.verification_result.current_iteration = \
            verification_assignment.verification_data.current_iteration
        verification_assignment.verification_result.state = VerificationResult.State.IN_PROGRESS
        verification_assignment.verification_result.save()

        logger.info('Start of verification for {}'.format(verification_assignment.task_declaration))
        if not verification_assignment.task_declaration.job_has_enough_balance():
            return

        failed, verify_tflops = self._run_verification_session(verification_assignment)
        if failed:
            return

        summarize_tflops = 0.0
        if not self._is_fake_worker_present(verification_assignment):
            failed, summarize_tflops = self._run_summarize_session(verification_assignment)
            if failed:
                return

        verification_assignment.verification_result.progress = 100.0
        verification_assignment.verification_result.tflops = verify_tflops + summarize_tflops
        verification_assignment.verification_result.state = VerificationResult.State.VERIFICATION_FINISHED
        current_iteration = verification_assignment.verification_data.current_iteration
        verification_assignment.verification_result.current_iteration = current_iteration
        verification_assignment.verification_result.save()

        logger.info('End of verification for {} results: {}'.format(
            verification_assignment.task_declaration, verification_assignment.verification_result.result))

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
