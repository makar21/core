import logging
import os
import re
import shutil
import tempfile

import numpy as np

from tatau_core.db import models, fields
from tatau_core.ipfs import IPFS, Directory
from tatau_core.utils import cached_property

log = logging.getLogger()


class TrainModel(models.Model):
    name = fields.CharField()
    code_ipfs = fields.EncryptedCharField()

    @classmethod
    def create(cls, **kwargs):
        code_path = kwargs.pop('code_path')
        kwargs['code_ipfs'] = IPFS().add_file(code_path).multihash
        return super(TrainModel, cls).create(**kwargs)


class Dataset(models.Model):
    name = fields.CharField()
    train_dir_ipfs = fields.EncryptedCharField()
    x_test_ipfs = fields.EncryptedCharField()
    y_test_ipfs = fields.EncryptedCharField()

    @classmethod
    def create(cls, **kwargs):
        x_train_path = kwargs.pop('x_train_path')
        y_train_path = kwargs.pop('y_train_path')
        x_test_path = kwargs.pop('x_test_path')
        y_test_path = kwargs.pop('y_test_path')
        files_count = kwargs.pop('files_count')

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

        return super(Dataset, cls).create(**kwargs)


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
    results = fields.JsonField(initial=[])

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
        return self.workers_needed == 0 and self.verifiers_needed == 0

    def get_task_assignments(self):
        ret = []
        task_assignments = TaskAssignment.list(
            additional_match={
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False
        )
        for ta in task_assignments:
            if ta.state not in (TaskAssignment.State.REJECTED, TaskAssignment.State.INITIAL):
                ret.append(ta)
        return ret

    def get_verification_assignments(self):
        ret = []
        task_assignments = VerificationAssignment.list(
            additional_match={
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False
        )
        for ta in task_assignments:
            if ta.state not in (VerificationAssignment.State.REJECTED, VerificationAssignment.State.INITIAL):
                ret.append(ta)
        return ret

    def assign_train_data(self):
        # check is it last epoch?

        task_assignments = self.get_task_assignments()
        self.current_epoch += 1
        self.results = []

        worker_index = 0
        for task_assignment in task_assignments:
            ipfs_dir = Directory(multihash=self.dataset.train_dir_ipfs)
            dirs, files = ipfs_dir.ls()

            # TODO: optimize this shit
            files_count_for_worker = int(len(files) / (2 * self.workers_requested))
            file_indexes = [x + files_count_for_worker * worker_index for x in range(files_count_for_worker)]

            x_train_ipfs = []
            y_train_ipfs = []
            for f in files:
                index = int(re.findall('\d+', f.name)[0])
                if index in file_indexes:
                    if f.name[0] == 'x':
                        x_train_ipfs.append(f.multihash)
                    elif f.name[0] == 'y':
                        y_train_ipfs.append(f.multihash)

            task_assignment.train_data = dict(
                model_code=self.train_model.code_ipfs,
                x_train_ipfs=x_train_ipfs,
                y_train_ipfs=y_train_ipfs,
                x_test_ipfs=self.dataset.x_test_ipfs,
                y_test_ipfs=self.dataset.y_test_ipfs,
                batch_size=self.batch_size,
                epochs=self.epochs
            )

            task_assignment.current_epoch = self.current_epoch
            task_assignment.state = TaskAssignment.State.DATA_IS_READY
            # encrypt inner data using worker's public key
            task_assignment.set_encryption_key(task_assignment.worker.enc_key)
            task_assignment.save(recipients=task_assignment.worker.address)

            worker_index += 1

        self.state = TaskDeclaration.State.EPOCH_IN_PROGRESS
        self.save()

    def assign_verification_data(self):
        for va in self.get_verification_assignments():
            va.train_results = self.results
            va.state = VerificationAssignment.State.DATA_IS_READY
            va.save(recipients=va.verifier.address)
        self.state = TaskDeclaration.State.VERIFY_IN_PROGRESS
        self.save()

    def is_task_assignment_allowed(self, task_assignment):
        if self.workers_needed == 0:
            return False

        match = {
            'assets.data.worker_id': task_assignment.worker_id,
            'assets.data.task_declaration_id': self.asset_id
        }

        if TaskAssignment.count(additional_match=match, created_by_user=False) == 1:
            return True

        return False

    def is_verification_assignment_allowed(self, verification_assignment):
        if self.verifiers_needed == 0:
            return False

        match = {
            'assets.data.verifier_id': verification_assignment.verifier_id,
            'assets.data.task_declaration_id': self.asset_id
        }

        if VerificationAssignment.count(additional_match=match, created_by_user=False) == 1:
            return True

        return False

    def epoch_is_ready(self):
        for ta in self.get_task_assignments():
            if ta.state != TaskAssignment.State.FINISHED:
                return False
        return True

    def verification_is_ready(self):
        for ta in self.get_verification_assignments():
            if ta.state != VerificationAssignment.State.FINISHED:
                return False
        return True

    def all_done(self):
        return self.epochs == self.current_epoch


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

    train_data = fields.JsonField(required=False)
    current_epoch = fields.IntegerField(initial=0)

    progress = fields.IntegerField(initial=0)
    tflops = fields.IntegerField(initial=0)

    result = fields.CharField(required=False)
    error = fields.CharField(required=False)

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
    train_results = fields.JsonField(required=False)

    progress = fields.IntegerField(initial=0)
    tflops = fields.IntegerField(initial=0)
    result = fields.JsonField(required=False)
    error = fields.CharField(required=False)

    @cached_property
    def producer(self):
        return ProducerNode.get(self.producer_id)

    @cached_property
    def verifier(self):
        return VerifierNode.get(self.verifier_id)

    @cached_property
    def task_declaration(self):
        return TaskDeclaration.get(self.task_declaration_id)
