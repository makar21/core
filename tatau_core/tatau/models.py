import logging
import os
import shutil
import tempfile

import numpy as np

from tatau_core.db import models, fields
from tatau_core.utils import cached_property
from tatau_core.utils.ipfs import IPFS

log = logging.getLogger()


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
    def upload_and_create(cls, x_train_path, y_train_path, x_test_path, y_test_path, files_count, **kwargs):
        ipfs = IPFS()

        kwargs['x_test_ipfs'] = ipfs.add_file(x_test_path).multihash
        kwargs['y_test_ipfs'] = ipfs.add_file(y_test_path).multihash

        directory = tempfile.mkdtemp()
        try:
            # TODO: determine files_count
            # file_size = os.path.getsize(x_train_ds_path)
            # files_count = int(file_size / 4096)

            with np.load(x_train_path) as fx, np.load(y_train_path) as fy:
                split_x = np.split(fx[fx.files[0]], files_count)
                split_y = np.split(fy[fy.files[0]], files_count)
                for i in range(files_count):
                    np.savez(os.path.join(directory, 'x_{}'.format(i)), split_x[i])
                    np.savez(os.path.join(directory, 'y_{}'.format(i)), split_y[i])

                kwargs['train_dir_ipfs'] = ipfs.add_dir(directory).multihash
        finally:
            shutil.rmtree(directory)

        return cls.create(**kwargs)


class NodeType:
    PRODUCER = 'producer'
    WORKER = 'worker'
    VERIFIER = 'verifier'


class ProducerNode(models.Model):
    node_type = fields.CharField(immutable=True, initial=NodeType.PRODUCER)
    enc_key = fields.CharField(immutable=True)


class WorkerNode(models.Model):
    node_type = fields.CharField(immutable=True, initial=NodeType.WORKER)
    enc_key = fields.CharField(immutable=True)


class VerifierNode(models.Model):
    node_type = fields.CharField(immutable=True, initial=NodeType.WORKER)
    enc_key = fields.CharField(immutable=True)


class TaskDeclaration(models.Model):
    class State:
        DEPLOYMENT = 'deployment'
        EPOCH_IN_PROGRESS = 'training'
        VERIFY_IN_PROGRESS = 'verifying'
        COMPLETED = 'completed'

    producer_id = fields.CharField(immutable=True)
    dataset_id = fields.CharField(immutable=True)
    train_model_id = fields.CharField(immutable=True)
    weights = fields.EncryptedCharField(required=False)
    batch_size = fields.IntegerField(immutable=True)
    epochs = fields.IntegerField(immutable=True)

    workers_requested = fields.IntegerField(immutable=True)
    verifiers_requested = fields.IntegerField(immutable=True)

    workers_needed = fields.IntegerField()
    verifiers_needed = fields.IntegerField()

    state = fields.CharField(initial=State.DEPLOYMENT)
    current_epoch = fields.IntegerField(initial=0)
    progress = fields.IntegerField(initial=0)
    tflops = fields.IntegerField(initial=0)
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
        return super(TaskDeclaration, cls).create(**kwargs)

    def ready_for_start(self):
        ready = self.workers_needed == 0 and self.verifiers_needed == 0
        log.info('{} ready:{} workers_needed:{} verifiers_needed:{}'.format(
            self, ready, self.workers_needed, self.verifiers_needed))
        return ready

    def get_task_assignments(self, exclude_states=None):
        ret = []
        task_assignments = TaskAssignment.list(
            additional_match={
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False
        )
        for ta in task_assignments:
            if exclude_states is None or ta.state not in exclude_states:
                ret.append(ta)
        return ret

    @property
    def task_assignments(self):
        return self.get_task_assignments()

    def get_verification_assignments(self, exclude_states=None):
        ret = []
        task_assignments = VerificationAssignment.list(
            additional_match={
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False
        )
        for ta in task_assignments:
            if exclude_states is None or ta.state not in exclude_states:
                ret.append(ta)
        return ret

    @property
    def verification_assignments(self):
        return self.get_verification_assignments()

    def is_task_assignment_allowed(self, task_assignment):
        if self.workers_needed == 0:
            return False

        match = {
            'assets.data.worker_id': task_assignment.worker_id,
            'assets.data.task_declaration_id': self.asset_id
        }

        count = TaskAssignment.count(additional_match=match, created_by_user=False)
        if count == 1:
            log.info('{} allowed for {}'.format(task_assignment, self))
            return True

        log.info('{} not allowed for {}, worker created {} assignment for this task'.format(
            task_assignment, self, count))
        return False

    def is_verification_assignment_allowed(self, verification_assignment):
        if self.verifiers_needed == 0:
            return False

        match = {
            'assets.data.verifier_id': verification_assignment.verifier_id,
            'assets.data.task_declaration_id': self.asset_id
        }

        count = VerificationAssignment.count(additional_match=match, created_by_user=False)
        if count == 1:
            log.info('{} allowed for {}'.format(verification_assignment, self))
            return True

        log.info('{} not allowed for {}, verifier created {} assignment for this task'.format(
            verification_assignment, self, count))
        return False

    def epoch_is_ready(self):
        task_assignments = self.get_task_assignments(
            exclude_states=(TaskAssignment.State.REJECTED, TaskAssignment.State.INITIAL)
        )

        for ta in task_assignments:
            if ta.state != TaskAssignment.State.FINISHED:
                return False
        return True

    def verification_is_ready(self):
        verification_assignments = self.get_verification_assignments(
            exclude_states=(VerificationAssignment.State.REJECTED, VerificationAssignment.State.INITIAL)
        )

        for va in verification_assignments:
            if va.state != VerificationAssignment.State.FINISHED:
                return False
        return True

    def all_done(self):
        return self.epochs == self.current_epoch and self.verification_is_ready()


class TaskAssignment(models.Model):
    class State:
        INITIAL = 'initial'
        REJECTED = 'rejected'
        ACCEPTED = 'accepted'
        DATA_IS_READY = 'data is ready'
        IN_PROGRESS = 'in progress'
        FINISHED = 'finished'

    producer_id = fields.CharField(immutable=True)
    worker_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)

    train_data = fields.EncryptedJsonField(required=False)
    current_epoch = fields.IntegerField(initial=0)

    progress = fields.IntegerField(initial=0)
    tflops = fields.IntegerField(initial=0)

    result = fields.EncryptedCharField(required=False)
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
        DATA_IS_READY = 'data is ready'
        IN_PROGRESS = 'in progress'
        FINISHED = 'finished'

    producer_id = fields.CharField(immutable=True)
    verifier_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)
    train_results = fields.EncryptedJsonField(required=False)

    progress = fields.IntegerField(initial=0)
    tflops = fields.IntegerField(initial=0)
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
