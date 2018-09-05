import datetime
import re
import time
from logging import getLogger

from tatau_core import settings
from tatau_core.models import ProducerNode, TaskDeclaration, TaskAssignment, VerificationAssignment, \
    EstimationAssignment
from tatau_core.models.estimation import EstimationData
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




    def _process_task_assignment(self, task_assignment, task_declaration, save=True):
        if task_assignment.state == TaskAssignment.State.REJECTED:
            return

        logger.info('Process: {}, state: {}'.format(task_assignment, task_assignment.state))
        if task_assignment.state == TaskAssignment.State.INITIAL:
            logger.info('{} requested {}'.format(task_assignment.worker, task_assignment))
            if task_declaration.is_task_assignment_allowed(task_assignment):
                task_assignment.state = TaskAssignment.State.ACCEPTED
                task_assignment.save()
                logger.info('Accept {} for {}'.format(task_assignment, task_declaration))

                task_declaration.workers_needed -= 1
                if save:
                    task_declaration.save()
            else:
                task_assignment.state = TaskAssignment.State.REJECTED
                task_assignment.save()
                logger.info('Reject {} for {} (No more workers needed)'.format(task_assignment, task_declaration))
            return

        if task_assignment.state == TaskAssignment.State.IN_PROGRESS:
            # check timeout
            pass

        if task_assignment.state == TaskAssignment.State.FINISHED:
            self._process_task_declaration(task_declaration)

    def _process_verification_assignment(self, verification_assignment, task_declaration, save=True):
        if verification_assignment.state == VerificationAssignment.State.REJECTED:
            return

        logger.info('Process: {}, state: {}'.format(verification_assignment, verification_assignment.state))
        if verification_assignment.state == VerificationAssignment.State.INITIAL:
            logger.info('{} requested {}'.format(verification_assignment.verifier, verification_assignment))
            if task_declaration.is_verification_assignment_allowed(verification_assignment) \
                    and WhiteList.is_allowed_verifier(verification_assignment.verifier_id):

                verification_assignment.state = VerificationAssignment.State.ACCEPTED
                verification_assignment.save()
                logger.info('Accept {} for {}'.format(verification_assignment, task_declaration))

                task_declaration.verifiers_needed -= 1
                if save:
                    task_declaration.save()
            else:
                verification_assignment.state = VerificationAssignment.State.REJECTED
                verification_assignment.save()
                logger.info('Reject {} for {} (No more verifiers needed)'.format(
                    verification_assignment, task_declaration))
            return

        if verification_assignment.state == VerificationAssignment.State.IN_PROGRESS:
            # check timeout
            pass

        if verification_assignment.state == VerificationAssignment.State.PARTIAL_DATA_IS_DOWNLOADED:
            self._process_task_declaration(task_declaration)

        if verification_assignment.state == VerificationAssignment.State.FINISHED:
            self._process_task_declaration(task_declaration)

    def _epoch_is_ready(self, task_declaration):
        task_assignments = task_declaration.get_task_assignments(
            states=(
                TaskAssignment.State.DATA_IS_READY,
                TaskAssignment.State.IN_PROGRESS
            )
        )

        for task_assignment in task_assignments:
            if task_assignment.state != TaskAssignment.State.FINISHED:
                return False

        return True

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
            # wait while job will be issued
            if task_declaration.job_has_enough_balance():
                task_declaration.state = TaskDeclaration.State.DEPLOYMENT
                task_declaration.save()
            return

        if not task_declaration.ready_for_start():
            return

        if task_declaration.state == TaskDeclaration.State.DEPLOYMENT:
            self._assign_train_data(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
            if self._epoch_is_ready(task_declaration):
                # are all verifiers are ready for verify
                if len(task_declaration.get_verification_assignments(
                        states=(VerificationAssignment.State.PARTIAL_DATA_IS_READY,))):
                    # wait verifiers
                    return

                logger.info('{} train iteration {} is ready'.format(task_declaration, task_declaration.current_iteration))
                # collect results from train_iteration
                task_assignments = task_declaration.get_task_assignments(
                    states=(TaskAssignment.State.FINISHED,)
                )

                for task_assignment in task_assignments:
                    if task_assignment.error is not None:
                        logger.info('{} is failed'.format(task_declaration))
                        task_declaration.state = TaskDeclaration.State.FAILED
                        task_declaration.save()
                        return

                    task_declaration.results.append({
                        'worker_id': task_assignment.worker_id,
                        'result': task_assignment.result,
                        'error': task_assignment.error
                    })
                    task_declaration.tflops += task_assignment.tflops

                self._assign_verification_data(task_declaration)
            else:
                self._assign_partial_verification_data(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.VERIFY_IN_PROGRESS:
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

    def _get_file_indexes(self, worker_index, train_files_count, workers_requested):
        files_count_for_worker = int(train_files_count / (2 * workers_requested))
        return [x + files_count_for_worker * worker_index for x in range(files_count_for_worker)]

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
            x_train_ipfs=x_train_ipfs,
            y_train_ipfs=y_train_ipfs,
            x_test_ipfs=task_declaration.dataset.x_test_ipfs,
            y_test_ipfs=task_declaration.dataset.y_test_ipfs,
            initial_weights=task_declaration.weights,
            batch_size=task_declaration.batch_size,
            epochs=epochs,
            worker_index=worker_index
        )

    def _assign_train_data(self, task_declaration):
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

    def _assign_train_data_to_worker(self, task_assignment, task_declaration, worker_index, ipfs_files):
        task_assignment.train_data = self._create_train_data(
            worker_index=worker_index,
            ipfs_files=ipfs_files,
            task_declaration=task_declaration
        )

        task_assignment.current_iteration = task_declaration.current_iteration
        task_assignment.clean()
        task_assignment.state = TaskAssignment.State.DATA_IS_READY
        # encrypt inner data using worker's public key
        task_assignment.set_encryption_key(task_assignment.worker.enc_key)
        task_assignment.save(recipients=task_assignment.worker.address)

    def _assign_verification_data(self, task_declaration):
        verification_assignments = task_declaration.get_verification_assignments(
            states=(
                VerificationAssignment.State.ACCEPTED,
                VerificationAssignment.State.PARTIAL_DATA_IS_DOWNLOADED,
                VerificationAssignment.State.FINISHED
            )
        )

        for verification_assignment in verification_assignments:
            verification_assignment.train_results = task_declaration.results
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

    def _process_performers(self, task_declaration):
        worker_needed = task_declaration.workers_needed
        verifiers_needed = task_declaration.verifiers_needed

        task_assignments = task_declaration.get_task_assignments(
            states=(
                TaskAssignment.State.INITIAL,
                TaskAssignment.State.IN_PROGRESS,
                TaskAssignment.State.FINISHED
            )
        )

        for task_assignment in task_assignments:
            try:
                self._process_task_assignment(task_assignment, task_declaration, save=False)
            except Exception as ex:
                logger.exception(ex)

        verification_assignments = task_declaration.get_verification_assignments(
            states=(
                VerificationAssignment.State.INITIAL,
                VerificationAssignment.State.IN_PROGRESS,
                VerificationAssignment.State.FINISHED
            )
        )

        for verification_assignment in verification_assignments:
            try:
                self._process_verification_assignment(verification_assignment, task_declaration, save=False)
            except Exception as ex:
                logger.exception(ex)

        # save if were changes
        if task_declaration.workers_needed != worker_needed \
                or task_declaration.verifiers_needed != verifiers_needed:
            task_declaration.save()

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


