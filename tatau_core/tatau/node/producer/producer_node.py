import re
import time
from logging import getLogger

from tatau_core import settings
from tatau_core.tatau.models import ProducerNode, TaskDeclaration, TaskAssignment, VerificationAssignment
from tatau_core.tatau.node.node import Node
from tatau_core.tatau.node.producer import poa_wrapper
from tatau_core.tatau.node.producer.sumarize_weights import summarize_weights
from tatau_core.utils.ipfs import Directory

logger = getLogger()


# noinspection PyMethodMayBeStatic
class Producer(Node):
    node_type = Node.NodeType.PRODUCER
    asset_class = ProducerNode

    def __init__(self, rsa_pk_fs_name=None, rsa_pk=None, exit_on_task_completion=None, task_declaration_asset_id=None,
                 *args, **kwargs):
        super(Producer, self).__init__(rsa_pk_fs_name, rsa_pk, *args, **kwargs)
        self.exit_on_task_completion = exit_on_task_completion
        # if 2 instances of producers with websocket will be started, then without filtering will be shit
        self.task_declaration_asset_id = task_declaration_asset_id

    def ignore_task_declaration(self, task_declaration_asset_id):
        return self.task_declaration_asset_id is not None \
               and self.task_declaration_asset_id != task_declaration_asset_id

    def get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self._process_task_declaration_transaction,
            TaskAssignment.get_asset_name(): self._process_task_assignment_transaction,
            VerificationAssignment.get_asset_name(): self._process_verification_assignment_transaction
        }

    def _process_task_assignment_transaction(self, asset_id, transaction):
        task_assignment = TaskAssignment.get(asset_id)
        if task_assignment.producer_id != self.asset_id:
            return

        if self.ignore_task_declaration(task_assignment.task_declaration_id):
            return

        self._process_task_assignment(task_assignment, task_assignment.task_declaration)

    def _process_task_assignment(self, task_assignment, task_declaration, save=True):
        if task_assignment.state == TaskAssignment.State.REJECTED:
            return

        logger.info('Process: {}, state: {}'.format(task_assignment, task_assignment.state))
        if task_assignment.state == TaskAssignment.State.INITIAL:
            logger.info('{} requested {}'.format(task_assignment.worker, task_assignment))
            if task_declaration.is_task_assignment_allowed(task_assignment):
                task_assignment.state = TaskAssignment.State.ACCEPTED
                task_assignment.save()

                task_declaration.workers_needed -= 1
                if save:
                    task_declaration.save()
            else:
                task_assignment.state = TaskAssignment.State.REJECTED
                task_assignment.save()
            return

        if task_assignment.state == TaskAssignment.State.IN_PROGRESS:
            pass

        if task_assignment.state == TaskAssignment.State.FINISHED:
            self._process_task_declaration(task_declaration)

    def _process_verification_assignment_transaction(self, asset_id, transaction):
        verification_assignment = VerificationAssignment.get(asset_id)
        if verification_assignment.producer_id != self.asset_id:
            return

        if self.ignore_task_declaration(verification_assignment.task_declaration_id):
            return

        self._process_verification_assignment(verification_assignment, verification_assignment.task_declaration)

    def _process_verification_assignment(self, verification_assignment, task_declaration, save=True):
        if verification_assignment.state == VerificationAssignment.State.REJECTED:
            return

        logger.info('Process: {}, state: {}'.format(verification_assignment, verification_assignment.state))
        if verification_assignment.state == VerificationAssignment.State.INITIAL:
            logger.info('{} requested {}'.format(verification_assignment.verifier, verification_assignment))
            if task_declaration.is_verification_assignment_allowed(verification_assignment):
                verification_assignment.state = VerificationAssignment.State.ACCEPTED
                verification_assignment.save()

                task_declaration.verifiers_needed -= 1
                if save:
                    task_declaration.save()
            else:
                verification_assignment.state = VerificationAssignment.State.REJECTED
                verification_assignment.save()
            return

        if verification_assignment.state == VerificationAssignment.State.IN_PROGRESS:
            pass

        if verification_assignment.state == VerificationAssignment.State.FINISHED:
            self._process_task_declaration(task_declaration)

    def _process_task_declaration_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        if self.ignore_task_declaration(asset_id):
            return

        task_declaration = TaskDeclaration.get(asset_id)
        if task_declaration.producer_id != self.asset_id or task_declaration.state == TaskDeclaration.State.COMPLETED:
            return

        self._process_task_declaration(task_declaration)

    def _process_task_declaration(self, task_declaration):
        if not task_declaration.ready_for_start():
            return

        if task_declaration.state == TaskDeclaration.State.DEPLOYMENT:
            poa_wrapper.issue_job(task_declaration)
            self._assign_train_data(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
            if task_declaration.epoch_is_ready():
                logger.info('{} train epoch {} is ready'.format(task_declaration, task_declaration.current_epoch))
                # collect results from epoch
                task_assignments = task_declaration.get_task_assignments(
                    exclude_states=(TaskAssignment.State.REJECTED, TaskAssignment.State.INITIAL)
                )
                for task_assignment in task_assignments:
                    task_declaration.results.append({
                        'worker_id': task_assignment.worker_id,
                        'result': task_assignment.result,
                        'error': task_assignment.error
                    })
                    assert task_assignment.state == TaskAssignment.State.FINISHED
                    task_declaration.tflops += task_assignment.tflops

                self.assign_verification_data(task_declaration)
            return

        if task_declaration.state == TaskDeclaration.State.VERIFY_IN_PROGRESS:
            if task_declaration.verification_is_ready():
                logger.info('{} verification epoch {} is ready'.format(
                    task_declaration, task_declaration.current_epoch))

                can_continue = self._process_verification_results(task_declaration)
                if not can_continue:
                    return

                self.summarize_epoch_resuts(task_declaration)
                if task_declaration.all_done():
                    task_declaration.state = TaskDeclaration.State.COMPLETED
                    task_declaration.save()
                    logger.info('{} is finished'.format(task_declaration))
                else:
                    self._assign_train_data(task_declaration)
            return

    def _process_verification_results(self, task_declaration):
        verification_assignments = task_declaration.get_verification_assignments(
            exclude_states=(VerificationAssignment.State.REJECTED, VerificationAssignment.State.INITIAL)
        )

        fake_workers = {}
        for verification_assignment in verification_assignments:
            assert verification_assignment.state == VerificationAssignment.State.FINISHED
            task_declaration.tflops += verification_assignment.tflops
            for result in verification_assignment.result:
                if result['is_fake']:
                    try:
                        fake_workers[result['worker_id']] += 1
                    except KeyError:
                        fake_workers[result['worker_id']] = 1

        if fake_workers:
            for worker_id in fake_workers.keys():
                task_assignments = TaskAssignment.list(
                    additional_match={
                        'assets.data.worker_id': worker_id,
                        'assets.data.task_declaration_id': task_declaration.asset_id
                    }
                )
                assert len(task_assignments) == 1

                task_assignment = task_assignments[0]
                task_assignment.state = TaskAssignment.State.REJECTED_FAKE
                task_assignment.save()

                task_declaration.workers_needed += 1

            task_declaration.state = TaskDeclaration.State.DEPLOYMENT
            task_declaration.save()
            return False

        return True

    def _get_file_indexes(self, worker_index, train_files_count, workers_requested):
        files_count_for_worker = int(train_files_count / (2 * workers_requested))
        return [x + files_count_for_worker * worker_index for x in range(files_count_for_worker)]

    def _create_train_data(self, worker_index, ipfs_files, task_declaration):
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
            epochs=task_declaration.epochs,
            worker_index=worker_index
        )

    def _assign_train_data(self, task_declaration):
        task_assignments = task_declaration.get_task_assignments(
            exclude_states=(TaskAssignment.State.INITIAL, TaskAssignment.State.REJECTED)
        )

        ipfs_dir = Directory(multihash=task_declaration.dataset.train_dir_ipfs)
        dirs, files = ipfs_dir.ls()

        # collect fake worker's indexes
        fake_worker_indexes = []
        for task_assignment in task_assignments:
            if task_assignment.state == TaskAssignment.State.REJECTED_FAKE:
                fake_worker_indexes.append(task_assignment.train_data['worker_index'])

        if len(fake_worker_indexes):
            # epoch is not finished
            for task_assignment in task_assignments:
                if task_assignment.state == TaskAssignment.State.ACCEPTED:
                    self._assign_train_data_to_worker(
                        task_assignment=task_assignment,
                        task_declaration=task_declaration,
                        worker_index=fake_worker_indexes.pop(0),
                        ipfs_files=files
                    )

                if len(fake_worker_indexes) == 0:
                    break

            task_declaration.state = TaskDeclaration.State.EPOCH_IN_PROGRESS
            task_declaration.save()
            return

        task_declaration.progress = int(task_declaration.current_epoch * 100 / task_declaration.epochs)
        task_declaration.current_epoch += 1
        task_declaration.results = []

        worker_index = 0

        for task_assignment in task_assignments:
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

        task_assignment.current_epoch = task_declaration.current_epoch
        task_assignment.state = TaskAssignment.State.DATA_IS_READY
        # encrypt inner data using worker's public key
        task_assignment.set_encryption_key(task_assignment.worker.enc_key)
        task_assignment.save(recipients=task_assignment.worker.address)

    def assign_verification_data(self, task_declaration):
        verification_assignments = task_declaration.get_verification_assignments(
            exclude_states=(VerificationAssignment.State.REJECTED, VerificationAssignment.State.INITIAL)
        )

        for verification_assignment in verification_assignments:
            verification_assignment.train_results = task_declaration.results
            verification_assignment.state = VerificationAssignment.State.DATA_IS_READY
            verification_assignment.set_encryption_key(verification_assignment.verifier.enc_key)
            verification_assignment.save(recipients=verification_assignment.verifier.address)

        task_declaration.state = TaskDeclaration.State.VERIFY_IN_PROGRESS
        task_declaration.save()

    def summarize_epoch_resuts(self, task_declaration):
        weights_ipfs, loss, acc = summarize_weights(
            train_results=task_declaration.results,
            x_test_ipfs=task_declaration.dataset.x_test_ipfs,
            y_test_ipfs=task_declaration.dataset.y_test_ipfs,
            model_code_ipfs=task_declaration.train_model.code_ipfs
        )

        task_declaration.weights = weights_ipfs
        task_declaration.loss = loss
        task_declaration.accuracy = acc

    def _process_performers(self, task_declaration):
        worker_needed = task_declaration.workers_needed
        verifiers_needed = task_declaration.verifiers_needed

        task_assignments = task_declaration.get_task_assignments(
            exclude_states=(TaskAssignment.State.ACCEPTED,)
        )

        for task_assignment in task_assignments:
            self._process_task_assignment(task_assignment, task_declaration, save=False)

        verification_assignments = task_declaration.get_verification_assignments(
            exclude_states=(VerificationAssignment.State.ACCEPTED,)
        )

        for verification_assignment in verification_assignments:
            self._process_verification_assignment(verification_assignment, task_declaration, save=False)

        # save if were changes
        if task_declaration.workers_needed != worker_needed or task_declaration.verifiers_needed != verifiers_needed:
            task_declaration.save()

    def train_task(self, asset_id):
        counter = 0
        while True:
            try:
                task_declaration = TaskDeclaration.get(asset_id)
                if task_declaration.state == TaskDeclaration.State.COMPLETED:
                    break

                self._process_performers(task_declaration)

                if task_declaration.state == TaskDeclaration.State.DEPLOYMENT:
                    self._process_task_declaration(task_declaration)

                time.sleep(settings.PRODUCER_PROCESS_INTERVAL)

                # TODO: add state failed
                # remove infinity loop
                counter += 1
                if counter == 1000:
                    task_declaration.state = TaskDeclaration.State.COMPLETED
                    task_declaration.save()
                    break

            except Exception as e:
                logger.exception(e)

    def process_tasks(self):
        while True:
            try:
                for task_declaration in TaskDeclaration.list():
                    if task_declaration.state == TaskDeclaration.State.COMPLETED:
                        continue

                    self._process_performers(task_declaration)
                    self._process_task_declaration(task_declaration)

                time.sleep(settings.PRODUCER_PROCESS_INTERVAL)
            except Exception as e:
                logger.exception(e)


