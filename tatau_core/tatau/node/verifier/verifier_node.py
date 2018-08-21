import json
import time
from logging import getLogger
from tatau_core import settings
from tatau_core.contract import poa_wrapper
from tatau_core.nn.tatau.sessions.summarize import SummarizeSession
from tatau_core.tatau.models import VerifierNode, TaskDeclaration, VerificationAssignment
from tatau_core.tatau.node.node import Node


logger = getLogger()


class Verifier(Node):

    asset_class = VerifierNode

    def _get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self._process_task_declaration_transaction,
            VerificationAssignment.get_asset_name(): self._process_verification_assignment_transaction,
        }

    def _process_task_declaration_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'TRANSFER':
            return

        task_declaration = TaskDeclaration.get(asset_id, db=self.db, encryption=self.encryption)
        if task_declaration.verifiers_needed == 0:
            return

        self._process_task_declaration(task_declaration)

    def _process_task_declaration(self, task_declaration):
        if task_declaration.state in (TaskDeclaration.State.COMPLETED, TaskDeclaration.State.FAILED):
            if poa_wrapper.does_job_exist(task_declaration) and not poa_wrapper.does_job_finished(task_declaration):
                poa_wrapper.finish_job(task_declaration)

        if task_declaration.state == TaskDeclaration.State.DEPLOYMENT \
                and task_declaration.verifiers_needed > 0:

            exists = VerificationAssignment.exists(
                additional_match={
                    'assets.data.verifier_id': self.asset_id,
                    'assets.data.task_declaration_id': task_declaration.asset_id,
                },
                created_by_user=False,
                db=self.db
            )

            if exists:
                logger.debug('{} has already created verification assignment to {}'.format(self, task_declaration))
                return

            verification_assignment = VerificationAssignment.create(
                producer_id=task_declaration.producer_id,
                verifier_id=self.asset_id,
                task_declaration_id=task_declaration.asset_id,
                recipients=task_declaration.producer.address,
                db=self.db,
                encryption=self.encryption
            )
            logger.info('{} added {}'.format(self, verification_assignment))

    def _process_verification_assignment_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        # skip another assignment
        verification_assignment = VerificationAssignment.get(asset_id, db=self.db, encryption=self.encryption)
        if verification_assignment.verifier_id != self.asset_id:
            return

        self._process_verification_assignment(verification_assignment)

    def _distribute(self, verification_assignment):
        poa_wrapper.distribute(verification_assignment)
        verification_assignment.state = VerificationAssignment.State.FINISHED
        verification_assignment.set_encryption_key(verification_assignment.producer.enc_key)
        verification_assignment.save(recipients=verification_assignment.producer.address)

    def _process_verification_assignment(self, verification_assignment):
        if verification_assignment.task_declaration.state in [TaskDeclaration.State.FAILED,
                                                              TaskDeclaration.State.COMPLETED]:
            return

        if verification_assignment.state == VerificationAssignment.State.PARTIAL_DATA_IS_READY:
            logger.info('{} start process partial data: {}'.format(self, verification_assignment))

            for worker_result in verification_assignment.train_results:
                if worker_result['result'] is not None:
                    self._ipfs_prefetch_async(worker_result['result'])

            verification_assignment.state = VerificationAssignment.State.PARTIAL_DATA_IS_DOWNLOADED
            verification_assignment.set_encryption_key(verification_assignment.producer.enc_key)
            verification_assignment.save(recipients=verification_assignment.producer.address)
            return

        if verification_assignment.state == VerificationAssignment.State.VERIFICATION_FINISHED:
            if verification_assignment.task_declaration.job_has_enough_balance():
                self._distribute(verification_assignment)
                if verification_assignment.task_declaration.is_last_epoch():
                    poa_wrapper.finish_job(verification_assignment.task_declaration)
            return

        if verification_assignment.state == VerificationAssignment.State.DATA_IS_READY:
            logger.info('{} start verify {}'.format(self, verification_assignment))
            if not verification_assignment.task_declaration.job_has_enough_balance():
                return

            from verifier.session import VerifySession

            session_verify = VerifySession()
            try:
                session_verify.process_assignment(assignment=verification_assignment)
            except Exception as e:
                error_dict = {'step': 'verification', 'exception': type(e).__name__}
                msg = str(e)
                if msg:
                    error_dict['message'] = msg

                verification_assignment.error = json.dumps(error_dict)
                verification_assignment.state = VerificationAssignment.State.FINISHED
                verification_assignment.save()
                logger.exception(e)
                return
            finally:
                session_verify.clean()

            # check is all workers are not fake
            found_fake_workers = False
            for result in verification_assignment.result:
                if result['is_fake']:
                    found_fake_workers = True

            session_summarize_tflops = 0.0
            if not found_fake_workers:
                session_summarize = SummarizeSession()
                try:
                    session_summarize.process_assignment(assignment=verification_assignment)
                    session_summarize_tflops = session_summarize.get_tflops()
                except Exception as e:
                    error_dict = {'step': 'summarization', 'exception': type(e).__name__}
                    msg = str(e)
                    if msg:
                        error_dict['message'] = msg

                    verification_assignment.error = json.dumps(error_dict)
                    verification_assignment.state = VerificationAssignment.State.FINISHED
                    verification_assignment.save()
                    logger.exception(e)
                    return
                finally:
                    session_summarize.clean()

            verification_assignment.progress = 100
            verification_assignment.tflops = session_verify.get_tflops() + session_summarize_tflops
            verification_assignment.state = VerificationAssignment.State.VERIFICATION_FINISHED
            verification_assignment.save()
            self._distribute(verification_assignment)

            logger.info('{} finish verify {} results: {}'.format(
                self, verification_assignment, verification_assignment.result))

            if verification_assignment.task_declaration.is_last_epoch():
                poa_wrapper.finish_job(verification_assignment.task_declaration)

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
