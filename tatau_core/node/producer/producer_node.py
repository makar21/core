import datetime
import re
import time
from logging import getLogger

from tatau_core import settings
from tatau_core.models import ProducerNode, TaskDeclaration, TaskAssignment, VerificationAssignment, \
    EstimationAssignment
from tatau_core.models.estimation import EstimationData, EstimationResult
from tatau_core.models.task import ListTaskAssignments
from tatau_core.models.train import TrainData, TrainResult
from tatau_core.models.verification import VerificationData, VerificationResult
from tatau_core.node.node import Node
from tatau_core.node.producer.estimator import Estimator
from tatau_core.node.producer.whitelist import WhiteList
from tatau_core.utils.ipfs import Directory

logger = getLogger()


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

        assert len(accepted_estimation_assignments) != 0

        if len(timeout_estimation_assignments):
            # its reassign
            for ea in accepted_estimation_assignments:
                # if pop will throw exception, then something went wrong
                timeout_ea = timeout_estimation_assignments.pop(-1)
                # reassign estimation data
                estimation_data = timeout_ea.estimation_data
                estimation_data.estimation_assignment_id = ea.asset_id
                # share data with new estimator
                estimation_data.set_encryption_key(ea.estimator.enc_key)
                estimation_data.save()

                ea.estimation_data_id = estimation_data.asset_id
                ea.state = EstimationAssignment.State.ESTIMATING
                ea.save()
        else:
            estimation_data_params = Estimator.get_data_for_estimate(task_declaration)
            for ea in accepted_estimation_assignments:
                estimation_data = EstimationData.create(
                    estimation_assignment_id=ea.asset_id,
                    # share data with estimator
                    public_key=ea.estimator.enc_key,
                    db=self.db,
                    encryption=self.encryption,
                    **estimation_data_params
                )
                ea.estimation_data_id = estimation_data.asset_id
                ea.state = EstimationAssignment.State.ESTIMATING
                ea.save()

        task_declaration.state = TaskDeclaration.State.ESTIMATE_IS_IN_PROGRESS
        task_declaration.save()

    def _process_estimate_is_required(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.ESTIMATE_IS_REQUIRED

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

        if task_declaration.estimators_needed == 0:
            # in assign changes will be saved
            self._assign_estimate_data(task_declaration)
            return

        # save changes
        if save:
            task_declaration.save()

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
                task_declaration.state = TaskDeclaration.State.FAILED
            task_declaration.save()

    def _process_estimated(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.ESTIMATED
        # wait while job will be issued
        if task_declaration.job_has_enough_balance():
            task_declaration.state = TaskDeclaration.State.DEPLOYMENT
            task_declaration.save()

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

    def _get_file_indexes(self, worker_index, train_files_count, workers_requested):
        files_count_for_worker = int(train_files_count / (2 * workers_requested))
        return [x + files_count_for_worker * worker_index for x in range(files_count_for_worker)]

    def _create_train_data(self, worker_index, ipfs_files, task_assignment: TaskAssignment):
        task_declaration = task_assignment.task_declaration

        file_indexes = self._get_file_indexes(
            worker_index=worker_index,
            train_files_count=len(ipfs_files),
            workers_requested=task_declaration.workers_requested
        )

        x_train_ipfs = []
        y_train_ipfs = []
        for ipfs_file in ipfs_files:
            index = int(re.findall('\d+', ipfs_file.name)[0])
            if index in file_indexes:
                if ipfs_file.name[0] == 'x':
                    x_train_ipfs.append(ipfs_file.multihash)
                elif ipfs_file.name[0] == 'y':
                    y_train_ipfs.append(ipfs_file.multihash)

        return TrainData.create(
            model_code=task_declaration.train_model.code_ipfs,
            x_train=x_train_ipfs,
            y_train=y_train_ipfs,
            x_test=task_declaration.dataset.x_test_ipfs,
            y_test=task_declaration.dataset.y_test_ipfs,
            data_index=worker_index,
            batch_size=task_declaration.batch_size,
            initial_weights=task_declaration.weights,
            epochs=task_declaration.epochs_in_current_iteration,
            task_assignment_id=task_declaration.asset_id,
            # share with worker
            public_key=task_assignment.worker.enc_key,
            db=self.db,
            encryption=self.encryption
        )

    def _assign_train_data_to_worker(self, task_assignment, worker_index, ipfs_files):
        train_data = self._create_train_data(
            worker_index=worker_index,
            ipfs_files=ipfs_files,
            task_assignment=task_assignment
        )

        task_assignment.train_data_id = train_data.asset_id
        task_assignment.state = TaskAssignment.State.TRAINING
        task_assignment.save()

    def _assign_train_data(self, task_declaration: TaskDeclaration):
        ipfs_dir = Directory(multihash=task_declaration.dataset.train_dir_ipfs)
        dirs, files = ipfs_dir.ls()

        task_declaration.progress = (
                task_declaration.current_iteration * task_declaration.epochs_in_iteration * 100
                / task_declaration.epochs)

        task_declaration.current_iteration += 1
        if task_declaration.current_iteration == 1:
            # start of train
            for index, ta in enumerate(task_declaration.get_task_assignments(states=(TaskAssignment.State.ACCEPTED,))):
                self._assign_train_data_to_worker(
                    task_assignment=ta,
                    worker_index=index,
                    ipfs_files=files
                )
        else:
            for ta in task_declaration.get_task_assignments(states=(TaskAssignment.State.FINISHED,)):
                train_data = ta.train_data
                train_data.current_iteration = task_declaration.current_iteration
                train_data.epochs = task_declaration.epochs_in_current_iteration
                train_data.initial_weights = task_declaration.weights
                train_data.set_encryption_key(ta.worker.enc_key)
                train_data.save()

                ta.state = TaskAssignment.State.TRAINING
                ta.save()

        task_declaration.state = TaskDeclaration.State.EPOCH_IN_PROGRESS
        task_declaration.save()

    def _process_deployment(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.DEPLOYMENT

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

        ready_to_start = task_declaration.workers_needed == 0 and task_declaration.verifiers_needed == 0
        logger.info('{} ready: {} workers_needed: {} verifiers_needed: {}'.format(
            task_declaration, ready_to_start, task_declaration.workers_needed, task_declaration.verifiers_needed))

        if ready_to_start:
            self._assign_train_data(task_declaration)
            return

        # save if were changes
        if save:
            task_declaration.save()

    def _assign_verification_data(self, task_declaration: TaskDeclaration, task_assignments: ListTaskAssignments):
        train_results = []
        for ta in task_assignments:
            train_results.append({
                'worker_id': ta.worker_id,
                'result': ta.train_result.weights
            })
            task_declaration.tflops += ta.train_result.tflops

        if task_declaration.current_iteration == 1:
            for verification_assignment in task_declaration.get_verification_assignments(
                    states=(VerificationAssignment.State.ACCEPTED,)):

                verification_data = VerificationData.create(
                    verification_assignment_id=verification_assignment.asset_id,
                    # share data with verifier
                    public_key=verification_assignment.verifier.enc_key,
                    x_test=task_declaration.dataset.x_test_ipfs,
                    y_test=task_declaration.dataset.y_test_ipfs,
                    model_code=task_declaration.train_model.code_ipfs,
                    train_results=train_results,
                    db=self.db,
                    encryption=self.encryption
                )

                verification_assignment.verification_data_id = verification_data.asset_id
                verification_assignment.state = VerificationAssignment.State.VERIFYING
                verification_assignment.save()
        else:
            for verification_assignment in task_declaration.get_verification_assignments(
                    states=(VerificationAssignment.State.FINISHED,)):

                verification_data = verification_assignment.verification_data
                verification_data.train_results = train_results
                verification_data.current_iteration = task_declaration.current_iteration
                verification_data.save()

                verification_assignment.state = VerificationAssignment.State.VERIFYING
                verification_assignment.save()

        task_declaration.state = TaskDeclaration.State.VERIFY_IN_PROGRESS
        task_declaration.save()

    def _process_epoch_in_progress(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS
        task_assignments = task_declaration.get_task_assignments(
            states=(
                TaskAssignment.State.TRAINING,
                TaskAssignment.State.FINISHED,
            )
        )

        finished_task_assignments = []
        failed = False
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

            # TODO: handle timeout

        if failed:
            task_declaration.state = TaskDeclaration.State.FAILED
            task_declaration.save()
            return

        if len(finished_task_assignments) < task_declaration.workers_requested:
            logger.info('Wait for finish of training for {} iteration {}'.format(
                task_declaration, task_declaration.current_iteration))
            # epoch is not ready
            # self._assign_partial_verification_data(task_declaration)
            return

        # if len(task_declaration.get_verification_assignments(
        #         states=(VerificationAssignment.State.PARTIAL_DATA_IS_READY,))):
        #     # wait verifiers
        #     return

        self._assign_verification_data(task_declaration, finished_task_assignments)

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

            # TODO: handle timeout

        if failed:
            task_declaration.state = TaskDeclaration.State.FAILED
            task_declaration.save()
            return

        if len(finished_verification_assignments) < task_declaration.verifiers_requested:
            # verification is not ready
            logger.info('Wait for finish of verification for {} iteration {}'.format(
                task_declaration, task_declaration.current_iteration))
            return

        for va in finished_verification_assignments:
            task_declaration.weights = va.verification_result.weights
            task_declaration.loss = va.verification_result.loss
            task_declaration.accuracy = va.verification_result.accuracy
            break

        logger.info('Copy summarization for {}, loss: {}, accuracy: {}'.format(
            task_declaration, task_declaration.loss, task_declaration.accuracy))

        if not task_declaration.is_last_epoch():
            self._assign_train_data(task_declaration)
            return

        task_declaration.progress = 100.0
        task_declaration.state = TaskDeclaration.State.COMPLETED
        task_declaration.save()
        logger.info('{} is finished tflops: {} estimated: {}'.format(
            task_declaration, task_declaration.tflops, task_declaration.estimated_tflops))

        # if task_declaration.verification_is_ready():
        #     logger.info('{} verification iteration {} is ready'.format(
        #         task_declaration, task_declaration.current_iteration))
        #
        #     can_continue, failed = self._process_verification_results(task_declaration)
        #     if failed:
        #         logger.info('{} is failed'.format(task_declaration))
        #         task_declaration.state = TaskDeclaration.State.FAILED
        #         task_declaration.save()
        #         return
        #
        #     if not can_continue:
        #         # set RETRY to REJECTED task_assignments
        #         rejected_task_declarations = task_declaration.get_task_assignments(
        #             states=(TaskAssignment.State.REJECTED,)
        #         )
        #
        #         for task_assignment in rejected_task_declarations:
        #             task_assignment.state = TaskAssignment.State.RETRY
        #             task_assignment.save(recipients=task_assignment.worker.address)
        #         return
        #
        #     self._copy_summarize_epoch_results(task_declaration)
        #     logger.info('Copy summarization for {}, loss: {}, accuracy: {}'.format(
        #         task_declaration, task_declaration.loss, task_declaration.accuracy))
        #
        #     if task_declaration.all_done():
        #         task_declaration.progress = 100.0
        #         task_declaration.state = TaskDeclaration.State.COMPLETED
        #         task_declaration.save()
        #         logger.info('{} is finished tflops: {} estimated: {}'.format(
        #             task_declaration, task_declaration.tflops, task_declaration.estimated_tflops))
        #     else:
        #         self._assign_train_data(task_declaration)
        # else:
        #     logger.info('{} verification for iteration {} is not ready'.format(
        #         task_declaration, task_declaration.current_iteration))

    def _process_task_declaration(self, task_declaration: TaskDeclaration):
        if task_declaration.is_in_finished_state():
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

        if task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
            self._process_epoch_in_progress(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.VERIFY_IN_PROGRESS:
            self._process_verify_in_progress(task_declaration)
            return

    def _process_verification_results(self, task_declaration):
        verification_assignments = task_declaration.get_verification_assignments(
            states=(VerificationAssignment.State.FINISHED,)
        )

        fake_workers = {}
        failed = False
        for verification_assignment in verification_assignments:
            if verification_assignment.error is not None:
                failed = True
                can_continue = False
                return can_continue, failed

            task_declaration.tflops += verification_assignment.tflops
            for result in verification_assignment.result:
                if result['is_fake']:
                    try:
                        fake_workers[result['worker_id']] += 1
                    except KeyError:
                        fake_workers[result['worker_id']] = 1

        if fake_workers:
            logger.info('Found fake workers')
            for worker_id in fake_workers.keys():
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

                logger.info('{} did fake results for {}'.format(task_assignment.worker, task_declaration))

                task_declaration.workers_needed += 1

            task_declaration.state = TaskDeclaration.State.DEPLOYMENT
            task_declaration.save()
            can_continue = False
            return can_continue, failed

        can_continue = True
        return can_continue, failed

    # def _assign_partial_verification_data(self, task_declaration):
    #     task_assignments = task_declaration.get_task_assignments(states=(TaskAssignment.State.FINISHED,))
    #
    #     current_train_results = []
    #     for task_assignment in task_assignments:
    #         current_train_results.append({
    #             'worker_id': task_assignment.worker_id,
    #             'result': task_assignment.result,
    #             'error': task_assignment.error
    #         })
    #
    #     if len(current_train_results):
    #         verification_assignments = task_declaration.get_verification_assignments(
    #             states=(
    #                 VerificationAssignment.State.ACCEPTED,
    #                 VerificationAssignment.State.PARTIAL_DATA_IS_DOWNLOADED,
    #                 VerificationAssignment.State.FINISHED
    #             )
    #         )
    #
    #         for verification_assignment in verification_assignments:
    #             verification_assignment.train_results = current_train_results
    #             verification_assignment.current_iteration = task_declaration.current_iteration
    #             verification_assignment.result = None
    #             verification_assignment.state = VerificationAssignment.State.PARTIAL_DATA_IS_READY
    #             verification_assignment.set_encryption_key(verification_assignment.verifier.enc_key)
    #             verification_assignment.save(recipients=verification_assignment.verifier.address)

    # def _copy_summarize_epoch_results(self, task_declaration):
    #     verification_assignments = task_declaration.get_verification_assignments(
    #         states=(VerificationAssignment.State.FINISHED,)
    #     )
    #
    #     for verification_assignment in verification_assignments:
    #         task_declaration.weights = verification_assignment.weights
    #         task_declaration.loss = verification_assignment.loss
    #         task_declaration.accuracy = verification_assignment.accuracy
    #         break

    def train_task(self, asset_id):
        while True:
            task_declaration = TaskDeclaration.get(asset_id, db=self.db, encryption=self.encryption)
            if task_declaration.is_in_finished_state():
                break

            self._process_task_declaration(task_declaration)
            time.sleep(settings.PRODUCER_PROCESS_INTERVAL)

    def process_tasks(self):
        while True:
            try:
                for task_declaration in TaskDeclaration.enumerate(db=self.db, encryption=self.encryption):
                    if task_declaration.is_in_finished_state():
                        continue

                    self._process_task_declaration(task_declaration)

                time.sleep(settings.PRODUCER_PROCESS_INTERVAL)
            except Exception as e:
                logger.exception(e)


