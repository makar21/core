import datetime
import re
import time
from logging import getLogger

from tatau_core import settings
from tatau_core.models import ProducerNode, TaskDeclaration, TaskAssignment, VerificationAssignment, \
    EstimationAssignment
from tatau_core.models.estimation import EstimationData
from tatau_core.models.task import ListTaskAssignments
from tatau_core.models.train import TrainData
from tatau_core.node.node import Node
from tatau_core.node.producer.estimator import Estimator
from tatau_core.node.producer.whitelist import WhiteList
from tatau_core.utils.ipfs import Directory

logger = getLogger()


class Producer(Node):

    asset_class = ProducerNode

    def _is_estimation_assignment_allowed(self, task_declaration: TaskDeclaration,
                                          estimation_assignment: EstimationAssignment):

        if task_declaration.estimators_needed == 0:
            return False

        if estimation_assignment.state != EstimationAssignment.State.INITIAL:
            return False

        if not WhiteList.is_allowed_estimator(estimation_assignment.estimator_id):
            return False

        logger.info('{} allowed for {}'.format(estimation_assignment, task_declaration))
        return True

    def _assign_estimate_data(self, task_declaration: TaskDeclaration):
        estimation_assignments = task_declaration.get_estimation_assignments(
            states=(EstimationAssignment.State.ACCEPTED,)
        )

        assert len(estimation_assignments) != 0

        estimation_data_params = Estimator.get_data_for_estimate(task_declaration)
        for ea in estimation_assignments:
            estimation_data = EstimationData.create(
                estimation_assignment_id=ea.asset_id,
                public_key=ea.estimator.enc_key,
                db=self.db,
                encryption=self.encryption,
                **estimation_data_params
            )
            ea.estimation_data_id = estimation_data.asset_id
            ea.state = EstimationAssignment.State.DATA_IS_READY
            ea.set_encryption_key(ea.estimator.enc_key)
            ea.save()

        task_declaration.state = TaskDeclaration.State.ESTIMATE_IS_IN_PROGRESS
        task_declaration.save()

    def _process_estimate_is_required(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.ESTIMATE_IS_REQUIRED

        save = False
        for ea in task_declaration.get_estimation_assignments(states=(EstimationAssignment.State.INITIAL,)):
            logger.info('{} requested {}'.format(ea.estimator, ea))
            if self._is_estimation_assignment_allowed(task_declaration, ea):
                ea.state = EstimationAssignment.State.ACCEPTED
                ea.save()

                logger.info('Accept {} for {}'.format(ea, task_declaration))
                task_declaration.estimators_needed -= 1
                save = True
            else:
                ea.state = EstimationAssignment.State.REJECTED
                ea.save()
                logger.info('Reject {} for {} (No more estimators needed)'.format(ea, task_declaration))

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

        for ei in task_declaration.get_estimation_assignments(
                states=(EstimationAssignment.State.REJECTED,)
        ):
            ei.state = EstimationAssignment.State.REASSIGN
            # give back access
            ei.save(recipients=ei.estimator.address)

    def _process_estimate_is_in_progress(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.ESTIMATE_IS_IN_PROGRESS

        estimation_assignments = task_declaration.get_estimation_assignments(
            states=(
                EstimationAssignment.State.DATA_IS_READY,
                EstimationAssignment.State.FINISHED
            )
        )

        finished_assignments = []
        count_timeout = 0
        for ea in estimation_assignments:
            if ea.state == EstimationAssignment.State.DATA_IS_READY:
                # check is train result is present
                if ea.estimation_result:
                    ea.state = EstimationAssignment.State.FINISHED
                    ea.save()
                else:
                    estimate_timeout = settings.WAIT_ESTIMATE_TIMEOUT
                    now = datetime.datetime.utcnow().replace(tzinfo=ea.created_at.tzinfo)
                    if (now - ea.created_at).total_seconds() > estimate_timeout:
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
        if task_declaration.workers_needed == 0:
            return False

        if task_assignment.state != TaskAssignment.State.INITIAL:
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
            logger.info('{} allowed for {}'.format(task_assignment, task_declaration))
            return True

        logger.info('{} not allowed for {}, worker created {} assignment for this task'.format(
            task_assignment, task_declaration, count))
        return False

    def _is_verification_assignment_allowed(self, task_declaration: TaskDeclaration,
                                            verification_assignment: VerificationAssignment):
        if task_declaration.verifiers_needed == 0:
            return False

        if verification_assignment.state != VerificationAssignment.State.INITIAL:
            return False

        count = VerificationAssignment.count(
            additional_match={
                'assets.data.verifier_id': verification_assignment.verifier_id,
                'assets.data.task_declaration_id': task_declaration.asset_id
            },
            created_by_user=False,
            db=self.db
        )

        if count == 1:
            logger.info('{} allowed for {}'.format(verification_assignment, task_declaration))
            return True

        logger.info('{} not allowed for {}, verifier created {} assignment for this task'.format(
            verification_assignment, task_declaration, count))
        return False

    def _get_file_indexes(self, worker_index, train_files_count, workers_requested):
        files_count_for_worker = int(train_files_count / (2 * workers_requested))
        return [x + files_count_for_worker * worker_index for x in range(files_count_for_worker)]

    def _create_train_data_params(self, worker_index, ipfs_files, task_declaration: TaskDeclaration):
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

        return dict(
            model_code=task_declaration.train_model.code_ipfs,
            x_train=x_train_ipfs,
            y_train=y_train_ipfs,
            x_test=task_declaration.dataset.x_test_ipfs,
            y_test=task_declaration.dataset.y_test_ipfs,
            data_index=worker_index,
            batch_size=task_declaration.batch_size,
            initial_weights=task_declaration.weights,
            epochs=task_declaration.epochs_in_current_iteration(),
            train_iteration=task_declaration.current_iteration
        )

    def _assign_train_data_to_worker(self, task_assignment, task_declaration, worker_index, ipfs_files):
        train_data_params = self._create_train_data(
            worker_index=worker_index,
            ipfs_files=ipfs_files,
            task_declaration=task_declaration
        )

        train_data = TrainData.create(
            task_assignment_id=task_declaration.asset_id,
            public_key=task_assignment.worker.enc_key,
            db=self.db,
            encryption=self.encryption,
            **train_data_params
        )

        task_assignment.train_data_id = train_data.asset_id
        task_assignment.state = TaskAssignment.State.DATA_IS_READY
        task_assignment.save()

    def _assign_train_data(self, task_declaration: TaskDeclaration):
        ipfs_dir = Directory(multihash=task_declaration.dataset.train_dir_ipfs)
        dirs, files = ipfs_dir.ls()

        task_declaration.progress = (
                task_declaration.current_iteration * task_declaration.epochs_in_iteration * 100
                / task_declaration.epochs)

        task_declaration.current_iteration += 1

        for index, ta in enumerate(task_declaration.get_task_assignments(states=(TaskAssignment.State.ACCEPTED,))):
            self._assign_train_data_to_worker(
                task_assignment=ta,
                task_declaration=task_declaration,
                worker_index=index,
                ipfs_files=files
            )

        task_declaration.state = TaskDeclaration.State.EPOCH_IN_PROGRESS
        task_declaration.save()

    def _process_deployment(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.DEPLOYMENT

        save = False
        for ta in task_declaration.get_task_assignments(states=(TaskAssignment.State.INITIAL,)):
            logger.info('{} requested {}'.format(ta.worker, ta))
            if self._is_task_assignment_allowed(task_declaration, ta):
                ta.state = TaskAssignment.State.ACCEPTED
                ta.save()
                logger.info('Accept {} for {}'.format(ta, task_declaration))

                task_declaration.workers_needed -= 1
                save = True
            else:
                ta.state = TaskAssignment.State.REJECTED
                ta.save()

                logger.info('Reject {} for {} (No more workers needed)'.format(ta, task_declaration))

        for va in task_declaration.get_verification_assignments(states=(VerificationAssignment.State.INITIAL,)):
            logger.info('{} requested {}'.format(va.verifier, va))
            if self._is_verification_assignment_allowed(task_declaration, va) \
                    and WhiteList.is_allowed_verifier(va.verifier_id):

                va.state = VerificationAssignment.State.ACCEPTED
                va.save()
                logger.info('Accept {} for {}'.format(va, task_declaration))

                task_declaration.verifiers_needed -= 1
                save = True
            else:
                va.state = VerificationAssignment.State.REJECTED
                va.save()
                logger.info('Reject {} for {} (No more verifiers needed)'.format(
                    va, task_declaration))

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

        verification_assignments = task_declaration.get_verification_assignments(
            states=(
                VerificationAssignment.State.ACCEPTED,
                VerificationAssignment.State.PARTIAL_DATA_IS_DOWNLOADED,
                VerificationAssignment.State.FINISHED
            )
        )
        train_results = []
        for ta in task_assignments:
            train_results.append({
                'worker_id': ta.worker_id,
                'weights': ta.train_result.weights,

                'result': ta.train_result.weights
            })
            task_declaration.tflops += ta.train_result.tflops

        for verification_assignment in verification_assignments:
            verification_assignment.train_results = train_results
            verification_assignment.current_iteration = task_declaration.current_iteration
            verification_assignment.x_test_ipfs = task_declaration.dataset.x_test_ipfs
            verification_assignment.y_test_ipfs = task_declaration.dataset.y_test_ipfs
            verification_assignment.model_code_ipfs = task_declaration.train_model.code_ipfs
            verification_assignment.clean()

            verification_assignment.state = VerificationAssignment.State.DATA_IS_READY
            verification_assignment.set_encryption_key(verification_assignment.verifier.enc_key)
            verification_assignment.save(recipients=verification_assignment.verifier.address)

        task_declaration.state = TaskDeclaration.State.VERIFY_IN_PROGRESS
        task_declaration.save()

    def _process_epoch_in_progress(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS
        task_assignments = task_declaration.get_task_assignments(
            states=(
                TaskAssignment.State.DATA_IS_READY,
                TaskAssignment.State.IN_PROGRESS,
                TaskAssignment.State.FINISHED,
            )
        )

        finished_task_assignments = []
        failed = False
        for ta in task_assignments:
            if ta.state == TaskAssignment.State.DATA_IS_READY:
                if ta.train_result:
                    ta.state = TaskAssignment.State.IN_PROGRESS
                    ta.save()

            if ta.state == TaskAssignment.State.IN_PROGRESS:
                if ta.train_result.finished:
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
            # epoch is not ready
            # self._assign_partial_verification_data(task_declaration)
            return

        if len(task_declaration.get_verification_assignments(
                states=(VerificationAssignment.State.PARTIAL_DATA_IS_READY,))):
            # wait verifiers
            return

        self._assign_verification_data(task_declaration, finished_task_assignments)

    def _process_verify_in_progress(self, task_declaration: TaskDeclaration):
        assert task_declaration.state == TaskDeclaration.State.VERIFY_IN_PROGRESS
        if task_declaration.verification_is_ready():
            logger.info('{} verification iteration {} is ready'.format(
                task_declaration, task_declaration.current_iteration))

            can_continue, failed = self._process_verification_results(task_declaration)
            if failed:
                logger.info('{} is failed'.format(task_declaration))
                task_declaration.state = TaskDeclaration.State.FAILED
                task_declaration.save()
                return

            if not can_continue:
                # set RETRY to REJECTED task_assignments
                rejected_task_declarations = task_declaration.get_task_assignments(
                    states=(TaskAssignment.State.REJECTED,)
                )

                for task_assignment in rejected_task_declarations:
                    task_assignment.state = TaskAssignment.State.RETRY
                    task_assignment.save(recipients=task_assignment.worker.address)
                return

            self._copy_summarize_epoch_results(task_declaration)
            logger.info('Copy summarization for {}, loss: {}, accuracy: {}'.format(
                task_declaration, task_declaration.loss, task_declaration.accuracy))

            if task_declaration.all_done():
                task_declaration.progress = 100.0
                task_declaration.state = TaskDeclaration.State.COMPLETED
                task_declaration.save()
                logger.info('{} is finished tflops: {} estimated: {}'.format(
                    task_declaration, task_declaration.tflops, task_declaration.estimated_tflops))
            else:
                self._assign_train_data(task_declaration)
        else:
            logger.info('{} verification for iteration {} is not ready'.format(
                task_declaration, task_declaration.current_iteration))

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

    def _create_train_data(self, worker_index, ipfs_files, task_declaration):
        epochs = task_declaration.epochs_in_iteration
        if task_declaration.current_iteration * task_declaration.epochs_in_iteration > task_declaration.epochs:
            epochs = task_declaration.epochs - \
                     (task_declaration.current_iteration - 1) * task_declaration.epochs_in_iteration

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

        return dict(
            model_code=task_declaration.train_model.code_ipfs,
            x_train=x_train_ipfs,
            y_train=y_train_ipfs,
            data_index=worker_index,
            initial_weights=task_declaration.weights,
            batch_size=task_declaration.batch_size,
            epochs=epochs,
            train_iteration=task_declaration.current_iteration
        )

    def _assign_train_data_old(self, task_declaration: TaskDeclaration):
        task_assignments = task_declaration.get_task_assignments(
            states=(
                TaskAssignment.State.ACCEPTED,
                TaskAssignment.State.FINISHED,
                TaskAssignment.State.FAKE_RESULTS
            )
        )

        # clean results from previous train_iteration
        task_declaration.results = []

        ipfs_dir = Directory(multihash=task_declaration.dataset.train_dir_ipfs)
        dirs, files = ipfs_dir.ls()

        # collect fake worker's indexes
        fake_worker_indexes = []
        for task_assignment in task_assignments:
            if task_assignment.state == TaskAssignment.State.FAKE_RESULTS:
                fake_worker_indexes.append(task_assignment.train_data['worker_index'])

        if len(fake_worker_indexes):
            # train_iteration is not finished if task assignments with "accepted" and "fake results" states are present
            reassign_performed = False
            for task_assignment in task_assignments:
                if task_assignment.state == TaskAssignment.State.ACCEPTED:
                    self._assign_train_data_to_worker(
                        task_assignment=task_assignment,
                        task_declaration=task_declaration,
                        worker_index=fake_worker_indexes.pop(0),
                        ipfs_files=files
                    )
                    reassign_performed = True

                if len(fake_worker_indexes) == 0:
                    break

            # if reassign train data was not performed to another worker, then continue do next train_iteration
            if reassign_performed:
                task_declaration.state = TaskDeclaration.State.EPOCH_IN_PROGRESS
                task_declaration.save()
                return

        task_declaration.progress = int(task_declaration.current_iteration * task_declaration.epochs_in_iteration * 100
                                        / task_declaration.epochs)

        task_declaration.current_iteration += 1
        worker_index = 0

        for task_assignment in task_assignments:
            # do not assign data to FAKE_RESULTS
            if task_assignment.state == TaskAssignment.State.FAKE_RESULTS:
                continue

            self._assign_train_data_to_worker(
                task_assignment=task_assignment,
                task_declaration=task_declaration,
                worker_index=worker_index,
                ipfs_files=files
            )

            worker_index += 1

        task_declaration.state = TaskDeclaration.State.EPOCH_IN_PROGRESS
        task_declaration.save()


    def _assign_partial_verification_data(self, task_declaration):
        task_assignments = task_declaration.get_task_assignments(states=(TaskAssignment.State.FINISHED,))

        current_train_results = []
        for task_assignment in task_assignments:
            current_train_results.append({
                'worker_id': task_assignment.worker_id,
                'result': task_assignment.result,
                'error': task_assignment.error
            })

        if len(current_train_results):
            verification_assignments = task_declaration.get_verification_assignments(
                states=(
                    VerificationAssignment.State.ACCEPTED,
                    VerificationAssignment.State.PARTIAL_DATA_IS_DOWNLOADED,
                    VerificationAssignment.State.FINISHED
                )
            )

            for verification_assignment in verification_assignments:
                verification_assignment.train_results = current_train_results
                verification_assignment.current_iteration = task_declaration.current_iteration
                verification_assignment.result = None
                verification_assignment.state = VerificationAssignment.State.PARTIAL_DATA_IS_READY
                verification_assignment.set_encryption_key(verification_assignment.verifier.enc_key)
                verification_assignment.save(recipients=verification_assignment.verifier.address)

    def _copy_summarize_epoch_results(self, task_declaration):
        verification_assignments = task_declaration.get_verification_assignments(
            states=(VerificationAssignment.State.FINISHED,)
        )

        for verification_assignment in verification_assignments:
            task_declaration.weights = verification_assignment.weights
            task_declaration.loss = verification_assignment.loss
            task_declaration.accuracy = verification_assignment.accuracy
            break

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


