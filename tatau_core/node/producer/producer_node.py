import datetime
import time
from logging import getLogger

from tatau_core import settings
from tatau_core.db.db import async_commit, use_async_commits
from tatau_core.models import ProducerNode, TaskDeclaration, TaskAssignment, VerificationAssignment, \
    EstimationAssignment, TrainData, VerificationData
from tatau_core.models.estimation import EstimationData, EstimationResult
from tatau_core.models.task import ListTaskAssignments, ListVerificationAssignments
from tatau_core.node.node import Node
from tatau_core.node.producer.estimator import Estimator
from tatau_core.node.producer.whitelist import WhiteList
from tatau_core.utils.ipfs import Directory

logger = getLogger('tatau_core')


# noinspection PyMethodMayBeStatic
class Producer(Node):

    asset_class = ProducerNode

    def _is_estimation_assignment_allowed(self,
                                          task_declaration: TaskDeclaration,
                                          estimation_assignment: EstimationAssignment):
        logger.info('{} requested {}'.format(estimation_assignment.estimator, estimation_assignment))

        if task_declaration.estimators_needed == 0:
            logger.info('Reject {} for {} (No more estimators needed)'.format(estimation_assignment, task_declaration))
            return False

        if estimation_assignment.state != EstimationAssignment.State.READY:
            logger.info('Reject {} for {} (Wrong state: {})'.format(
                estimation_assignment, task_declaration, estimation_assignment.state))
            return False

        if not WhiteList.is_allowed_estimator(estimation_assignment.estimator_id):
            logger.info('Reject {} for {} (Not whitelisted)'.format(estimation_assignment, task_declaration))
            return False

        logger.info('Accept {} for {}'.format(estimation_assignment, task_declaration))
        return True

    @use_async_commits
    def _assign_estimate_data(self, task_declaration: TaskDeclaration):
        estimation_assignments = task_declaration.get_estimation_assignments(
            states=(EstimationAssignment.State.ACCEPTED, EstimationAssignment.State.TIMEOUT)
        )

        # split accepted and overdue
        accepted_estimation_assignments = []
        timeout_estimation_assignments = []
        for ea in estimation_assignments:
            if ea.state == EstimationAssignment.State.ACCEPTED:
                accepted_estimation_assignments.append(ea)
                continue

            if ea.state == EstimationAssignment.State.TIMEOUT:
                timeout_estimation_assignments.append(ea)
                continue

            assert False and 'Check query!'

        if len(timeout_estimation_assignments):
            # its reassign
            assert len(timeout_estimation_assignments) == len(accepted_estimation_assignments)
            for index, ea in enumerate(accepted_estimation_assignments):
                timeout_ea = timeout_estimation_assignments[index]
                # reassign estimation data
                # retrieve data which producer is able to encrypt
                estimation_data = EstimationData.get_with_initial_data(
                    asset_id=timeout_ea.estimation_data_id,
                    db=self.db,
                    encryption=self.encryption
                )

                estimation_data.estimation_assignment_id = ea.asset_id
                # share data with new estimator
                estimation_data.set_encryption_key(ea.estimator.enc_key)
                estimation_data.save()

                ea.estimation_data_id = estimation_data.asset_id
                ea.state = EstimationAssignment.State.ESTIMATING
                ea.save()

                timeout_ea.state = EstimationAssignment.State.FORGOTTEN
                timeout_ea.save()
        else:
            estimation_data_params = Estimator.get_data_for_estimate(task_declaration)
            for ea in accepted_estimation_assignments:
                # create initial state with encrypted data which producer will be able to decrypt
                estimation_data = EstimationData.create(
                    db=self.db,
                    encryption=self.encryption,
                    **estimation_data_params
                )

                # share data with estimator
                estimation_data.estimation_assignment_id = ea.asset_id
                estimation_data.set_encryption_key(ea.estimator.enc_key)
                estimation_data.save()

                ea.estimation_data_id = estimation_data.asset_id
                ea.state = EstimationAssignment.State.ESTIMATING
                ea.save()

        task_declaration.state = TaskDeclaration.State.ESTIMATE_IS_IN_PROGRESS
        task_declaration.save()

    def _process_estimate_is_required(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.ESTIMATE_IS_REQUIRED

        with async_commit():

            save = False

            for ea in task_declaration.get_estimation_assignments(states=(EstimationAssignment.State.READY,)):
                if self._is_estimation_assignment_allowed(task_declaration, ea):
                    ea.state = EstimationAssignment.State.ACCEPTED
                    ea.save()
                    task_declaration.estimators_needed -= 1
                    save = True
                else:
                    ea.state = EstimationAssignment.State.REJECTED
                    ea.save()

            # save changes
            if save:
                task_declaration.save()

        if task_declaration.estimators_needed == 0:
            # in assign changes will be saved
            self._assign_estimate_data(task_declaration)

    @use_async_commits
    def _republish_for_estimation(self, task_declaration: TaskDeclaration):
        assert task_declaration.estimators_needed > 0

        task_declaration.state = TaskDeclaration.State.ESTIMATE_IS_REQUIRED
        task_declaration.save()

        for ei in task_declaration.get_estimation_assignments(states=(EstimationAssignment.State.REJECTED,)):
            ei.state = EstimationAssignment.State.REASSIGN
            # return back ownership
            ei.save(recipients=ei.estimator.address)

    def _process_estimate_is_in_progress(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.ESTIMATE_IS_IN_PROGRESS

        estimation_assignments = task_declaration.get_estimation_assignments(
            states=(
                EstimationAssignment.State.ESTIMATING,
                EstimationAssignment.State.FINISHED
            )
        )

        finished_assignments = []
        count_timeout = 0
        with async_commit():
            for ea in estimation_assignments:
                if ea.state == EstimationAssignment.State.ESTIMATING:
                    if ea.estimation_result.state == EstimationResult.State.FINISHED:
                        ea.state = EstimationAssignment.State.FINISHED
                        ea.save()
                    else:
                        estimate_timeout = settings.WAIT_ESTIMATE_TIMEOUT
                        now = datetime.datetime.utcnow().replace(tzinfo=ea.estimation_result.modified_at.tzinfo)
                        if (now - ea.estimation_result.modified_at).total_seconds() > estimate_timeout:
                            ea.state = EstimationAssignment.State.TIMEOUT
                            ea.save()

                            logger.info('Timeout of waiting for {}'.format(ea))
                            count_timeout += 1

                if ea.state == EstimationAssignment.State.FINISHED:
                    finished_assignments.append(ea)

        if count_timeout:
            task_declaration.estimators_needed += count_timeout
            self._republish_for_estimation(task_declaration)
            return

        if len(finished_assignments) == task_declaration.estimators_requested:
            task_declaration.state = TaskDeclaration.State.ESTIMATED
            task_declaration.estimated_tflops, failed = Estimator.estimate(task_declaration, finished_assignments)
            if failed:
                logger.info('{} is failed'.format(task_declaration))
                task_declaration.state = TaskDeclaration.State.FAILED
            task_declaration.save()
            return

        logger.info('Wait of finish for estimation {}, finished: {}, requested: {}'.format(
            task_declaration, len(finished_assignments), task_declaration.estimators_requested
        ))

    def _process_estimated(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.ESTIMATED
        # wait while job will be issued and will have enough balance
        if task_declaration.balance_in_wei >= task_declaration.train_cost_in_wei:
            task_declaration.state = TaskDeclaration.State.DEPLOYMENT
            task_declaration.save()
        else:
            if task_declaration.issued:
                logger.info('Deposit for {} is required, balance: {:.5f} train cost: {:.5f}'.format(
                    task_declaration, task_declaration.balance, task_declaration.train_cost))
            else:
                logger.info('Issue for {} is required, train cost: {:.5f}'.format(
                    task_declaration, task_declaration.train_cost))

    def _is_task_assignment_allowed(self, task_declaration: TaskDeclaration, task_assignment: TaskAssignment):
        logger.info('{} requested {}'.format(task_assignment.worker, task_assignment))
        if task_declaration.workers_needed == 0:
            logger.info('Reject {} for {} (No more workers needed)'.format(task_assignment, task_declaration))
            return False

        if task_assignment.state != TaskAssignment.State.READY:
            logger.info('Reject {} for {} (Wrong state: {})'.format(
                task_assignment, task_declaration, task_assignment.state))
            return False

        count = TaskAssignment.count(
            additional_match={
                'assets.data.worker_id': task_assignment.worker_id,
                'assets.data.task_declaration_id': task_declaration.asset_id
            },
            created_by_user=False,
            db=self.db
        )

        if count == 1:
            logger.info('Accept {} for {}'.format(task_assignment, task_declaration))
            return True

        logger.info('Reject {} for {} (Worker created {} assignment for this task)'.format(
            task_assignment, task_declaration, count))

        return False

    def _is_verification_assignment_allowed(self,
                                            task_declaration: TaskDeclaration,
                                            verification_assignment: VerificationAssignment):
        logger.info('{} requested {}'.format(verification_assignment.verifier, verification_assignment))
        if task_declaration.verifiers_needed == 0:
            logger.info('Reject {} for {} (No more verifiers needed)'.format(verification_assignment, task_declaration))
            return False

        if verification_assignment.state != VerificationAssignment.State.READY:
            logger.info('Reject {} for {} (Wrong state {})'.format(
                verification_assignment, task_declaration, verification_assignment.state))
            return False

        if not WhiteList.is_allowed_verifier(verification_assignment.verifier_id):
            logger.info('Reject {} for {} (Not whitelisted)'.format(verification_assignment, task_declaration))
            return False

        logger.info('Accept {} for {}'.format(verification_assignment, task_declaration))
        return True

    def _chunk_it(self, iterable, count):
        return [iterable[i::count] for i in range(count)]

    def _assign_initial_train_data(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.DEPLOYMENT
        # start of train
        task_declaration.current_iteration += 1
        task_declaration.current_iteration_retry = 0

        accepted_task_assignment = task_declaration.get_task_assignments(states=(TaskAssignment.State.ACCEPTED,))

        count_ta = 0

        train_dirs_ipfs, files = Directory(multihash=task_declaration.dataset.train_dir_ipfs).ls()
        test_dirs_ipfs, files = Directory(multihash=task_declaration.dataset.test_dir_ipfs).ls()

        all_train_chunks_ipfs = self._chunk_it(
            iterable=[x.multihash for x in train_dirs_ipfs],
            count=task_declaration.workers_requested
        )

        assert len(all_train_chunks_ipfs) == task_declaration.workers_requested

        all_test_chunks_ipfs = self._chunk_it(
            iterable=[x.multihash for x in test_dirs_ipfs],
            count=task_declaration.workers_requested
        )

        assert len(all_test_chunks_ipfs) == task_declaration.workers_requested

        list_td_ta = []
        with async_commit():
            # create TrainData
            for index, task_assignment in enumerate(accepted_task_assignment):
                train_chunks_ipfs = all_train_chunks_ipfs[index]
                test_chunks_ipfs = all_test_chunks_ipfs[index]

                train_data = TrainData.create(
                    model_code_ipfs=task_declaration.train_model.code_ipfs,
                    train_chunks_ipfs=train_chunks_ipfs,
                    test_chunks_ipfs=test_chunks_ipfs,
                    data_index=index,
                    db=self.db,
                    encryption=self.encryption
                )

                list_td_ta.append((train_data, task_assignment))
                logger.debug('Created {}, train chunks: {}, count:{}, test chunks: {}, count:{}'.format(
                    train_data, train_chunks_ipfs, len(train_chunks_ipfs), test_chunks_ipfs, len(test_chunks_ipfs)))
                count_ta += 1

        assert task_declaration.workers_requested == count_ta

        with async_commit():
            # share to worker
            for train_data, task_assignment in list_td_ta:
                train_data.task_assignment_id = task_assignment.asset_id
                train_data.set_encryption_key(task_assignment.worker.enc_key)
                train_data.save()

                task_assignment.train_data_id = train_data.asset_id
                task_assignment.state = TaskAssignment.State.TRAINING
                task_assignment.save()

            task_declaration.state = TaskDeclaration.State.EPOCH_IN_PROGRESS
            task_declaration.save()

    @use_async_commits
    def _update_train_data_for_next_iteration(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.VERIFY_IN_PROGRESS

        task_declaration.current_iteration += 1
        task_declaration.current_iteration_retry = 0

        task_declaration.progress = (
                task_declaration.current_iteration * task_declaration.epochs_in_iteration * 100
                / task_declaration.epochs)

        count_ta = 0
        for ta in task_declaration.get_task_assignments(states=(TaskAssignment.State.FINISHED,)):
            train_data = ta.train_data
            # share data to worker
            train_data.set_encryption_key(ta.worker.enc_key)
            train_data.save()

            ta.state = TaskAssignment.State.TRAINING
            ta.save()
            count_ta += 1

        assert task_declaration.workers_requested == count_ta
        task_declaration.state = TaskDeclaration.State.EPOCH_IN_PROGRESS
        task_declaration.save()

    @use_async_commits
    def _reassign_train_data(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.DEPLOYMENT_TRAIN

        task_assignments = task_declaration.get_task_assignments(
            states=(
                TaskAssignment.State.ACCEPTED,
                TaskAssignment.State.TIMEOUT,
                TaskAssignment.State.FAKE_RESULTS
            )
        )

        # split by state
        accepted_task_assignment = []
        failed_task_assignments = []
        for ta in task_assignments:
            if ta.state == TaskAssignment.State.ACCEPTED:
                accepted_task_assignment.append(ta)
                continue

            if ta.state == TaskAssignment.State.TIMEOUT:
                failed_task_assignments.append(ta)
                continue

            if ta.state == TaskAssignment.State.FAKE_RESULTS:
                failed_task_assignments.append(ta)
                continue

            assert False and 'Check query!'

        assert len(failed_task_assignments) == len(accepted_task_assignment)

        # assign data to new accepted task_assignments
        for index, ta in enumerate(accepted_task_assignment):
            failed_ta = failed_task_assignments[index]
            # reassign train data
            # retrieve data which producer is able to encrypt
            train_data = TrainData.get_with_initial_data(
                asset_id=failed_ta.train_data_id,
                db=self.db,
                encryption=self.encryption
            )
            train_data.task_assignment_id = ta.asset_id
            # share data with new worker
            train_data.set_encryption_key(ta.worker.enc_key)
            train_data.save()

            ta.train_data_id = train_data.asset_id
            ta.state = TaskAssignment.State.TRAINING
            ta.save()

            failed_ta.state = TaskAssignment.State.FORGOTTEN
            failed_ta.save()

        task_declaration.state = TaskDeclaration.State.EPOCH_IN_PROGRESS
        task_declaration.save()

    def _process_deployment(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.DEPLOYMENT

        with async_commit():
            save = False
            for ta in task_declaration.get_task_assignments(states=(TaskAssignment.State.READY,)):
                if self._is_task_assignment_allowed(task_declaration, ta):
                    ta.state = TaskAssignment.State.ACCEPTED
                    ta.save()

                    task_declaration.workers_needed -= 1
                    save = True
                else:
                    ta.state = TaskAssignment.State.REJECTED
                    ta.save()

            for va in task_declaration.get_verification_assignments(states=(VerificationAssignment.State.READY,)):
                if self._is_verification_assignment_allowed(task_declaration, va):
                    va.state = VerificationAssignment.State.ACCEPTED
                    va.save()

                    task_declaration.verifiers_needed -= 1
                    save = True
                else:
                    va.state = VerificationAssignment.State.REJECTED
                    va.save()

            # save if were changes
            if save:
                task_declaration.save()

        ready_to_start = task_declaration.workers_needed == 0 and task_declaration.verifiers_needed == 0
        logger.info('{} ready: {} workers_needed: {} verifiers_needed: {}'.format(
            task_declaration, ready_to_start, task_declaration.workers_needed, task_declaration.verifiers_needed))

        if ready_to_start:
            self._assign_initial_train_data(task_declaration)
            return

        if not save:
            # recheck how many workers and verifiers really accepted
            accepted_workers_count = len(task_declaration.get_task_assignments(
                states=(TaskAssignment.State.ACCEPTED,)))

            accepted_verifiers_count = len(task_declaration.get_verification_assignments(
                states=(VerificationAssignment.State.ACCEPTED,)))

            if accepted_workers_count == task_declaration.workers_requested \
                    and accepted_verifiers_count == task_declaration.verifiers_requested:
                logger.info('All performers are accepted, start train')
                task_declaration.workers_needed = 0
                task_declaration.verifiers_needed = 0
                self._assign_initial_train_data(task_declaration)

    def _process_deployment_train(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.DEPLOYMENT_TRAIN

        with async_commit():
            save = False
            for ta in task_declaration.get_task_assignments(states=(TaskAssignment.State.READY,)):
                if self._is_task_assignment_allowed(task_declaration, ta):
                    ta.state = TaskAssignment.State.ACCEPTED
                    ta.save()

                    task_declaration.workers_needed -= 1
                    save = True
                else:
                    ta.state = TaskAssignment.State.REJECTED
                    ta.save()

            # save if were changes
            if save:
                task_declaration.save()

        ready_to_start = task_declaration.workers_needed == 0 and task_declaration.verifiers_needed == 0
        logger.info('{} ready: {} workers_needed: {} verifiers_needed: {}'.format(
            task_declaration, ready_to_start, task_declaration.workers_needed, task_declaration.verifiers_needed))

        if ready_to_start:
            self._reassign_train_data(task_declaration)
            return

        if not save:
            # recheck how many workers really accepted
            accepted_workers_count = len(task_declaration.get_task_assignments(
                states=(TaskAssignment.State.ACCEPTED, TaskAssignment.State.FINISHED)))

            if accepted_workers_count == task_declaration.workers_requested:
                logger.info('All performers are accepted, start train')
                task_declaration.workers_needed = 0
                self._reassign_train_data(task_declaration)

    def _process_deployment_verification(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.DEPLOYMENT_VERIFICATION

        with async_commit():
            save = False
            for va in task_declaration.get_verification_assignments(states=(VerificationAssignment.State.READY,)):
                if self._is_verification_assignment_allowed(task_declaration, va):
                    va.state = VerificationAssignment.State.ACCEPTED
                    va.save()

                    task_declaration.verifiers_needed -= 1
                    save = True
                else:
                    va.state = VerificationAssignment.State.REJECTED
                    va.save()

            # save if were changes
            if save:
                task_declaration.save()

        ready_to_verify = task_declaration.workers_needed == 0 and task_declaration.verifiers_needed == 0
        logger.info('{} ready for verification: {} workers_needed: {} verifiers_needed: {}'.format(
            task_declaration, ready_to_verify, task_declaration.workers_needed, task_declaration.verifiers_needed))

        if ready_to_verify:
            self._reassign_verification_data(task_declaration)
            return

        if not save:
            # recheck how many verifiers really accepted
            accepted_verifiers_count = len(task_declaration.get_verification_assignments(
                states=(VerificationAssignment.State.ACCEPTED, VerificationAssignment.State.FINISHED)))

            if accepted_verifiers_count == task_declaration.verifiers_requested:
                logger.info('All performers are accepted, start train')
                task_declaration.verifiers_needed = 0
                self._reassign_verification_data(task_declaration)

    @use_async_commits
    def _assign_verification_data(self, task_declaration: TaskDeclaration, task_assignments: ListTaskAssignments):
        train_results = []
        for ta in task_assignments:
            train_results.append({
                'worker_id': ta.worker_id,
                'result': ta.train_result.weights_ipfs
            })
            task_declaration.tflops += ta.train_result.tflops

        for verification_assignment in task_declaration.get_verification_assignments(
                states=(VerificationAssignment.State.ACCEPTED, VerificationAssignment.State.FINISHED)):

            if verification_assignment.state == VerificationAssignment.State.ACCEPTED:
                assert verification_assignment.verification_data_id is None
                verification_data = VerificationData.create(
                    verification_assignment_id=verification_assignment.asset_id,
                    # share data with verifier
                    public_key=verification_assignment.verifier.enc_key,
                    test_dir_ipfs=task_declaration.dataset.test_dir_ipfs,
                    model_code_ipfs=task_declaration.train_model.code_ipfs,
                    train_results=train_results,
                    db=self.db,
                    encryption=self.encryption
                )

                verification_assignment.verification_data_id = verification_data.asset_id
                verification_assignment.state = VerificationAssignment.State.VERIFYING
                verification_assignment.save()
                continue

            if verification_assignment.state == VerificationAssignment.State.FINISHED:
                verification_data = verification_assignment.verification_data
                verification_data.train_results = train_results
                verification_data.save()

                verification_assignment.state = VerificationAssignment.State.VERIFYING
                verification_assignment.save()
                continue

        task_declaration.state = TaskDeclaration.State.VERIFY_IN_PROGRESS
        task_declaration.save()

    @use_async_commits
    def _reassign_verification_data(self, task_declaration: TaskDeclaration):
        verification_assignments = task_declaration.get_verification_assignments(
            states=(VerificationAssignment.State.ACCEPTED, VerificationAssignment.State.TIMEOUT)
        )

        # split accepted and overdue
        accepted_verification_assignments = []
        timeout_verification_assignments = []
        for va in verification_assignments:
            if va.state == VerificationAssignment.State.ACCEPTED:
                accepted_verification_assignments.append(va)
                continue

            if va.state == VerificationAssignment.State.TIMEOUT:
                timeout_verification_assignments.append(va)
                continue

            assert False and 'Check query!'

        assert len(accepted_verification_assignments) == len(timeout_verification_assignments)

        train_results = [
            {
                'worker_id': ta.worker_id,
                'result': ta.train_result.weights_ipfs
            }
            for ta in task_declaration.get_task_assignments(states=(TaskAssignment.State.FINISHED,))
        ]

        for index, va in enumerate(accepted_verification_assignments):
            assert va.verification_data_id is None
            verification_data = VerificationData.create(
                verification_assignment_id=va.asset_id,
                # share data with verifier
                public_key=va.verifier.enc_key,
                test_dir_ipfs=task_declaration.dataset.test_dir_ipfs,
                model_code_ipfs=task_declaration.train_model.code_ipfs,
                train_results=train_results,
                db=self.db,
                encryption=self.encryption
            )

            va.verification_data_id = verification_data.asset_id
            va.state = VerificationAssignment.State.VERIFYING
            va.save()

            failed_va = timeout_verification_assignments[index]
            failed_va.state = VerificationAssignment.State.FORGOTTEN
            failed_va.save()

        task_declaration.state = TaskDeclaration.State.VERIFY_IN_PROGRESS
        task_declaration.save()

    @use_async_commits
    def _republish_for_train(self, task_declaration: TaskDeclaration):
        assert task_declaration.workers_needed > 0

        task_declaration.state = TaskDeclaration.State.DEPLOYMENT_TRAIN
        task_declaration.current_iteration_retry += 1
        task_declaration.save()

        task_assignments = task_declaration.get_task_assignments(
            states=(TaskAssignment.State.REJECTED,)
        )

        for ta in task_assignments:
            ta.state = TaskAssignment.State.REASSIGN
            # return back ownership
            ta.save(recipients=ta.worker.address)

    @use_async_commits
    def _reject_fake_workers(self, task_declaration: TaskDeclaration, fake_worker_ids):
        for worker_id in fake_worker_ids:
            task_assignments = TaskAssignment.list(
                additional_match={
                    'assets.data.worker_id': worker_id,
                    'assets.data.task_declaration_id': task_declaration.asset_id
                },
                created_by_user=False,
                db=self.db,
                encryption=self.encryption
            )
            assert len(task_assignments) == 1

            task_assignment = task_assignments[0]
            task_assignment.state = TaskAssignment.State.FAKE_RESULTS
            task_assignment.save()

            task_declaration.workers_needed += 1

    def _save_loss_and_accuracy(self, task_declaration: TaskDeclaration, finished_task_assignments):
        assert task_declaration.current_iteration > 1
        loss = []
        accuracy = []

        # collect loss and accuracy for prev iteration
        iteration = str(task_declaration.current_iteration - 1)
        for ta in finished_task_assignments:
            if ta.train_result.eval_results is not None and ta.train_result.eval_results.get(iteration):
                loss.append(ta.train_result.eval_results[iteration]['loss'])
                accuracy.append(ta.train_result.eval_results[iteration]['accuracy'])

        assert len(loss) and len(accuracy)
        task_declaration.loss = sum(loss)/len(loss)
        task_declaration.accuracy = sum(accuracy)/len(accuracy)
        logger.info('Save avr iteration: {} loss: {} and accuracy: {}'.format(
            iteration, task_declaration.loss, task_declaration.accuracy))

    def _process_epoch_in_progress(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS
        task_assignments = task_declaration.get_task_assignments(
            states=(
                TaskAssignment.State.TRAINING,
                TaskAssignment.State.FINISHED,
            )
        )

        failed = False
        finished_task_assignments = []
        count_timeout = 0
        with async_commit():
            for ta in task_assignments:
                if ta.state == TaskAssignment.State.TRAINING:
                    if ta.iteration_is_finished:
                        ta.state = TaskAssignment.State.FINISHED
                        ta.save()

                if ta.state == TaskAssignment.State.FINISHED:
                    if ta.train_result.error:
                        failed = True
                    else:
                        finished_task_assignments.append(ta)
                    continue

                train_timeout = settings.WAIT_TRAIN_TIMEOUT
                now = datetime.datetime.utcnow().replace(tzinfo=ta.train_result.modified_at.tzinfo)
                if (now - ta.train_result.modified_at).total_seconds() > train_timeout:
                    ta.state = TaskAssignment.State.TIMEOUT
                    ta.save()

                    logger.info('Timeout of waiting for {}'.format(ta))
                    count_timeout += 1

        if failed:
            logger.info('{} is failed'.format(task_declaration))
            task_declaration.state = TaskDeclaration.State.FAILED
            task_declaration.save()
            return

        if count_timeout:
            task_declaration.workers_needed += count_timeout
            self._republish_for_train(task_declaration)
            return

        if len(finished_task_assignments) < task_declaration.workers_requested:
            logger.info('Wait for finish of training for {} iteration {}'.format(
                task_declaration, task_declaration.current_iteration))
            return

        if task_declaration.current_iteration > 1:
            self._save_loss_and_accuracy(task_declaration, finished_task_assignments)

        self._assign_verification_data(task_declaration, finished_task_assignments)

    @use_async_commits
    def _republish_for_verify(self, task_declaration: TaskDeclaration):
        assert task_declaration.verifiers_needed > 0

        task_declaration.state = TaskDeclaration.State.DEPLOYMENT_VERIFICATION
        task_declaration.save()

        verification_assignment = task_declaration.get_verification_assignments(
            states=(VerificationAssignment.State.REJECTED,)
        )

        for va in verification_assignment:
            va.state = VerificationAssignment.State.REASSIGN
            # return back ownership
            va.save(recipients=va.verifier.address)

    def _parse_verification_results(self, task_declaration: TaskDeclaration,
                                    finished_verification_assignments: ListVerificationAssignments):
        fake_workers = {}
        for va in finished_verification_assignments:
            assert va.verification_result.error is None
            for result in va.verification_result.result:
                if result['is_fake']:
                    try:
                        fake_workers[result['worker_id']] += 1
                    except KeyError:
                        fake_workers[result['worker_id']] = 1

            task_declaration.tflops += va.verification_result.tflops

            # what to do if many verifiers ?
            if va.verification_result.weights:
                # if weights is None than fake workers are present
                task_declaration.weights_ipfs = va.verification_result.weights
                if task_declaration.last_iteration:
                    task_declaration.loss = va.verification_result.loss
                    task_declaration.accuracy = va.verification_result.accuracy

                    logger.info('Copy summarization for {}, loss: {}, accuracy: {}'.format(
                        task_declaration, task_declaration.loss, task_declaration.accuracy))
                else:
                    logger.info('Copy summarization for {}'.format(task_declaration))

        return fake_workers

    def _process_verify_in_progress(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.VERIFY_IN_PROGRESS
        verification_assignments = task_declaration.get_verification_assignments(
            states=(
                VerificationAssignment.State.VERIFYING,
                VerificationAssignment.State.FINISHED
            )
        )

        failed = False
        finished_verification_assignments = []
        count_timeout = 0
        with async_commit():
            for va in verification_assignments:
                if va.state == VerificationAssignment.State.VERIFYING:
                    if va.iteration_is_finished:
                        va.state = VerificationAssignment.State.FINISHED
                        va.save()

                if va.state == VerificationAssignment.State.FINISHED:
                    if va.verification_result.error:
                        failed = True
                    else:
                        finished_verification_assignments.append(va)
                    continue

                verify_timeout = settings.WAIT_VERIFY_TIMEOUT
                now = datetime.datetime.utcnow().replace(tzinfo=va.verification_result.modified_at.tzinfo)
                if (now - va.verification_result.modified_at).total_seconds() > verify_timeout:
                    va.state = VerificationAssignment.State.TIMEOUT
                    va.save()

                    logger.info('Timeout of waiting for {}'.format(va))
                    count_timeout += 1

        if count_timeout:
            task_declaration.verifiers_needed += count_timeout
            self._republish_for_verify(task_declaration)
            return

        if failed:
            logger.info('{} is failed'.format(task_declaration))
            task_declaration.state = TaskDeclaration.State.FAILED
            task_declaration.save()
            return

        if len(finished_verification_assignments) < task_declaration.verifiers_requested:
            # verification is not ready
            logger.info('Wait for finish of verification for {} iteration {}'.format(
                task_declaration, task_declaration.current_iteration))
            return

        fake_workers = self._parse_verification_results(
            task_declaration, finished_verification_assignments)

        if fake_workers:
            logger.info('Fake workers detected')
            fake_worker_ids = []
            for worker_id, count_detections in fake_workers.items():
                logger.info('Fake worker_id: {}, count detections: {}'.format(worker_id, count_detections))
                fake_worker_ids.append(worker_id)
            self._reject_fake_workers(task_declaration, fake_worker_ids)
            self._republish_for_train(task_declaration)
            return

        if not task_declaration.last_iteration:
            self._update_train_data_for_next_iteration(task_declaration)
            return

        task_declaration.progress = 100.0
        task_declaration.state = TaskDeclaration.State.COMPLETED
        task_declaration.save()
        logger.info('{} is finished tflops: {} estimated: {}'.format(
            task_declaration, task_declaration.tflops, task_declaration.estimated_tflops))

    def _process_task_declaration(self, task_declaration: TaskDeclaration):
        if task_declaration.in_finished_state:
            return

        if task_declaration.state == TaskDeclaration.State.ESTIMATE_IS_REQUIRED:
            self._process_estimate_is_required(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.ESTIMATE_IS_IN_PROGRESS:
            self._process_estimate_is_in_progress(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.ESTIMATED:
            self._process_estimated(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.DEPLOYMENT:
            self._process_deployment(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.DEPLOYMENT_TRAIN:
            self._process_deployment_train(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.DEPLOYMENT_VERIFICATION:
            self._process_deployment_verification(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
            self._process_epoch_in_progress(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.VERIFY_IN_PROGRESS:
            self._process_verify_in_progress(task_declaration)
            return

    def train_task(self, asset_id):
        while True:
            task_declaration = TaskDeclaration.get(asset_id, db=self.db, encryption=self.encryption)
            if task_declaration.in_finished_state:
                break

            self._process_task_declaration(task_declaration)
            time.sleep(settings.PRODUCER_PROCESS_INTERVAL)

    def process_tasks(self):
        while True:
            try:
                for task_declaration in TaskDeclaration.enumerate(db=self.db, encryption=self.encryption):
                    if task_declaration.in_finished_state:
                        continue

                    self._process_task_declaration(task_declaration)

                time.sleep(settings.PRODUCER_PROCESS_INTERVAL)
            except Exception as e:
                logger.exception(e)
