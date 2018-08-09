from logging import getLogger
import os
import shutil
import tempfile
import numpy as np

from tatau_core import settings, web3
from tatau_core.contract import poa_wrapper
from tatau_core.db import models, fields
from tatau_core.utils import cached_property
from tatau_core.utils.ipfs import IPFS

logger = getLogger()


class TrainModel(models.Model):
    name = fields.CharField()
    code_ipfs = fields.EncryptedCharField()

    @classmethod
    def upload_and_create(cls, code_path, **kwargs):
        code_ipfs = IPFS().add_file(code_path).multihash
        return cls.create(code_ipfs=code_ipfs, **kwargs)


class Dataset(models.Model):
    name = fields.CharField()
    train_dir_ipfs = fields.EncryptedCharField()
    x_test_ipfs = fields.EncryptedCharField()
    y_test_ipfs = fields.EncryptedCharField()

    @classmethod
    def upload_and_create(cls, x_train_path, y_train_path, x_test_path, y_test_path, minibatch_size, **kwargs):
        logger.info('Creating dataset')
        ipfs = IPFS()

        kwargs['x_test_ipfs'] = ipfs.add_file(x_test_path).multihash
        kwargs['y_test_ipfs'] = ipfs.add_file(y_test_path).multihash

        directory = tempfile.mkdtemp()
        try:

            # TODO: determine files_count
            # file_size = os.path.getsize(x_train_ds_path)
            # files_count = int(file_size / 4096)
            x_train = np.load(x_train_path)
            y_train = np.load(y_train_path)
            batches = int(len(x_train) / minibatch_size)
            logger.info('Split dataset to {} batches'.format(batches))
            for batch_idx in range(0, batches):
                start_idx = batch_idx * minibatch_size
                end_idx = start_idx + minibatch_size
                x_batch = x_train[start_idx: end_idx]
                y_batch = y_train[start_idx: end_idx]
                x_path = os.path.join(directory, 'x_{:04d}'.format(batch_idx))
                np.save(x_path, x_batch)
                y_path = os.path.join(directory, 'y_{:04d}'.format(batch_idx))
                np.save(y_path, y_batch)
            logger.info('Upload dataset to IPFS')
            kwargs['train_dir_ipfs'] = ipfs.add_dir(directory).multihash
            logger.info('Dataset was uploaded')
        finally:
            logger.debug('Cleanup dataset tmp dir')
            shutil.rmtree(directory)

        return cls.create(**kwargs)


class NodeType:
    PRODUCER = 'producer'
    WORKER = 'worker'
    VERIFIER = 'verifier'


class ProducerNode(models.Model):
    node_type = fields.CharField(immutable=True, initial=NodeType.PRODUCER)
    enc_key = fields.CharField(immutable=True)
    account_address = fields.CharField(immutable=True)


class WorkerNode(models.Model):
    node_type = fields.CharField(immutable=True, initial=NodeType.WORKER)
    enc_key = fields.CharField(immutable=True)
    account_address = fields.CharField(immutable=True)


class VerifierNode(models.Model):
    node_type = fields.CharField(immutable=True, initial=NodeType.VERIFIER)
    enc_key = fields.CharField(immutable=True)
    account_address = fields.CharField(immutable=True)


class TaskDeclaration(models.Model):
    class State:
        ESTIMATE_IS_REQUIRED = 'estimate is required'
        ESTIMATE_IN_PROGRESS = 'estimate in progress'
        ESTIMATED = 'estimated'
        DEPLOYMENT = 'deployment'
        EPOCH_IN_PROGRESS = 'training'
        VERIFY_IN_PROGRESS = 'verifying'
        COMPLETED = 'completed'
        FAILED = 'failed'

    producer_id = fields.CharField(immutable=True)
    # TODO: make copy of dataset and train model data instead reference on assets
    dataset_id = fields.CharField(immutable=True)
    train_model_id = fields.CharField(immutable=True)
    weights = fields.EncryptedCharField(required=False)
    loss = fields.FloatField(required=False)
    accuracy = fields.FloatField(required=False)

    batch_size = fields.IntegerField(immutable=True)
    epochs = fields.IntegerField(immutable=True)

    workers_requested = fields.IntegerField(immutable=True)
    verifiers_requested = fields.IntegerField(immutable=True)
    estimators_requested = fields.IntegerField(immutable=True)

    workers_needed = fields.IntegerField()
    verifiers_needed = fields.IntegerField()
    estimators_needed = fields.IntegerField()

    state = fields.CharField(initial=State.ESTIMATE_IS_REQUIRED)
    current_epoch = fields.IntegerField(initial=0)
    progress = fields.FloatField(initial=0.0)
    tflops = fields.FloatField(initial=0.0)
    estimated_tflops = fields.FloatField(initial=0.0)
    results = fields.EncryptedJsonField(initial=[])

    @cached_property
    def producer(self):
        return ProducerNode.get(self.producer_id)

    @cached_property
    def dataset(self):
        return Dataset.get(self.dataset_id)

    @cached_property
    def train_model(self):
        return TrainModel.get(self.train_model_id)

    @classmethod
    def create(cls, **kwargs):
        kwargs['workers_requested'] = kwargs['workers_needed']
        kwargs['verifiers_requested'] = kwargs['verifiers_needed']

        if 'estimators_needed' not in kwargs:
            kwargs['estimators_needed'] = 1

        kwargs['estimators_requested'] = kwargs['estimators_needed']
        return super(TaskDeclaration, cls).create(**kwargs)

    def ready_for_start(self):
        ready = self.workers_needed == 0 and self.verifiers_needed == 0
        logger.info('{} ready:{} workers_needed:{} verifiers_needed:{}'.format(
            self, ready, self.workers_needed, self.verifiers_needed))
        return ready

    def job_has_enough_balance(self):
        balance = poa_wrapper.get_job_balance(self)

        if self.current_epoch == 0:
            epoch_cost = self.estimated_tflops / self.epochs * settings.TFLOPS_COST
        elif self.current_epoch == 1:
            epoch_cost = self.estimated_tflops / self.epochs * settings.TFLOPS_COST
        else:
            epoch_cost = self.tflops / (self.current_epoch - 1) * settings.TFLOPS_COST
        epoch_cost = web3.toWei(str(epoch_cost), 'ether')

        if int(balance) > int(epoch_cost):
            logger.info('{} balance: {}, epoch cost: {}'.format(self, balance, epoch_cost))
            return True
        else:
            if poa_wrapper.does_job_exist(self):
                logger.info('{} balance: {}, epoch cost: {} Deposit is required!!!'.format(self, balance, epoch_cost))
            else:
                estimated_cost = self.estimated_tflops * settings.TFLOPS_COST
                logger.info('{} Issue job is required!!! Estimated cost: {:.20f} ETH'.format(
                    self, web3.fromWei(estimated_cost, 'ether')))
            return False

    def get_task_assignments(self, states=None):
        task_assignments = TaskAssignment.enumerate(
            additional_match={
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False
        )

        ret = []
        for task_assignment in task_assignments:
            if states is None or task_assignment.state in states:
                ret.append(task_assignment)
        return ret

    def get_estimation_assignments(self, states=None):
        estimation_assignments = EstimationAssignment.enumerate(
            additional_match={
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False
        )

        ret = []
        for estimation_assignment in estimation_assignments:
            if states is None or estimation_assignment.state in states:
                ret.append(estimation_assignment)
        return ret

    def is_last_epoch(self):
        return self.current_epoch == self.epochs

    @property
    def task_assignments(self):
        return self.get_task_assignments()

    def get_verification_assignments(self, states=None):
        verification_assignments = VerificationAssignment.enumerate(
            additional_match={
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False
        )

        ret = []
        for verification_assignment in verification_assignments:
            if states is None or verification_assignment.state in states:
                ret.append(verification_assignment)
        return ret

    @property
    def verification_assignments(self):
        return self.get_verification_assignments()

    def is_task_assignment_allowed(self, task_assignment):
        if self.workers_needed == 0:
            return False

        if task_assignment.state != TaskAssignment.State.INITIAL:
            return False

        count = TaskAssignment.count(
            additional_match={
                'assets.data.worker_id': task_assignment.worker_id,
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False
        )

        if count == 1:
            logger.info('{} allowed for {}'.format(task_assignment, self))
            return True

        logger.info('{} not allowed for {}, worker created {} assignment for this task'.format(
            task_assignment, self, count))
        return False

    def is_verification_assignment_allowed(self, verification_assignment):
        if self.verifiers_needed == 0:
            return False

        if verification_assignment.state != VerificationAssignment.State.INITIAL:
            return False

        count = VerificationAssignment.count(
            additional_match={
                'assets.data.verifier_id': verification_assignment.verifier_id,
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False
        )

        if count == 1:
            logger.info('{} allowed for {}'.format(verification_assignment, self))
            return True

        logger.info('{} not allowed for {}, verifier created {} assignment for this task'.format(
            verification_assignment, self, count))
        return False

    def is_estimation_assignment_allowed(self, estimation_assignment):
        if self.estimators_needed == 0:
            return False

        if estimation_assignment.state != EstimationAssignment.State.INITIAL:
            return False

        count = EstimationAssignment.count(
            additional_match={
                'assets.data.worker_id': estimation_assignment.worker_id,
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False
        )

        if count == 1:
            logger.info('{} allowed for {}'.format(estimation_assignment, self))
            return True

        logger.info('{} not allowed for {}, verifier created {} assignment for this task'.format(
            estimation_assignment, self, count))
        return False

    def verification_is_ready(self):
        verification_assignments = self.get_verification_assignments(
            states=(
                VerificationAssignment.State.DATA_IS_READY,
                VerificationAssignment.State.IN_PROGRESS,
                VerificationAssignment.State.FINISHED
            )
        )

        for va in verification_assignments:
            if va.state != VerificationAssignment.State.FINISHED:
                return False
        return True

    def all_done(self):
        return self.epochs == self.current_epoch and self.verification_is_ready()


class EstimationAssignment(models.Model):
    class State:
        INITIAL = 'initial'
        RETRY = 'retry'
        REJECTED = 'rejected'
        ACCEPTED = 'accepted'
        DATA_IS_READY = 'data is ready'
        IN_PROGRESS = 'in progress'
        FINISHED = 'finished'

    producer_id = fields.CharField(immutable=True)
    worker_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)

    estimation_data = fields.EncryptedJsonField(required=False)
    tflops = fields.FloatField(initial=0.0)
    error = fields.EncryptedCharField(required=False)

    @cached_property
    def producer(self):
        return ProducerNode.get(self.producer_id)

    @cached_property
    def worker(self):
        return WorkerNode.get(self.worker_id)

    @cached_property
    def task_declaration(self):
        return TaskDeclaration.get(self.task_declaration_id)


class TaskAssignment(models.Model):
    class State:
        INITIAL = 'initial'
        RETRY = 'retry'
        REJECTED = 'rejected'
        ACCEPTED = 'accepted'
        DATA_IS_READY = 'data is ready'
        IN_PROGRESS = 'in progress'
        FINISHED = 'finished'
        FAKE_RESULTS = 'fake results'

    producer_id = fields.CharField(immutable=True)
    worker_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)

    train_data = fields.EncryptedJsonField(required=False)
    current_epoch = fields.IntegerField(initial=0)

    progress = fields.FloatField(initial=0.0)
    tflops = fields.FloatField(initial=0.0)

    result = fields.EncryptedCharField(required=False)
    loss = fields.FloatField(required=False)
    accuracy = fields.FloatField(required=False)
    train_history = fields.JsonField(required=False)

    error = fields.EncryptedCharField(required=False)

    @cached_property
    def producer(self):
        return ProducerNode.get(self.producer_id)

    @cached_property
    def worker(self):
        return WorkerNode.get(self.worker_id)

    @cached_property
    def task_declaration(self):
        return TaskDeclaration.get(self.task_declaration_id)


class VerificationAssignment(models.Model):
    class State:
        INITIAL = 'initial'
        REJECTED = 'rejected'
        ACCEPTED = 'accepted'
        PARTIAL_DATA_IS_READY = 'partial data is ready'
        PARTIAL_DATA_IS_DOWNLOADED = 'partial data is downloaded'
        DATA_IS_READY = 'data is ready'
        IN_PROGRESS = 'in progress'
        FINISHED = 'finished'

    producer_id = fields.CharField(immutable=True)
    verifier_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)
    model_code_ipfs = fields.EncryptedCharField(required=False)
    train_results = fields.EncryptedJsonField(required=False)

    progress = fields.FloatField(initial=0.0)
    tflops = fields.FloatField(initial=0.0)
    result = fields.EncryptedJsonField(required=False)
    error = fields.EncryptedCharField(required=False)

    @cached_property
    def producer(self):
        return ProducerNode.get(self.producer_id)

    @cached_property
    def verifier(self):
        return VerifierNode.get(self.verifier_id)

    @cached_property
    def task_declaration(self):
        return TaskDeclaration.get(self.task_declaration_id)
