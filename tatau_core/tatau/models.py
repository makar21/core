import logging
import os
import shutil
import tempfile

import numpy as np

from tatau_core.db import models, fields
from tatau_core.ipfs import IPFS
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
    class Status:
        DEPLOYMENT = 'deployment'
        RUN = 'run'
        COMPLETED = 'completed'

    producer_id = fields.CharField(immutable=True)
    dataset_id = fields.CharField(immutable=True)
    train_model_id = fields.CharField(immutable=True)
    workers_requested = fields.IntegerField(immutable=True)
    workers_needed = fields.IntegerField()
    verifiers_requested = fields.IntegerField(immutable=True)
    verifiers_needed = fields.IntegerField()
    batch_size = fields.IntegerField(immutable=True)
    epochs = fields.IntegerField(immutable=True)
    status = fields.CharField(initial=Status.DEPLOYMENT)
    progress = fields.IntegerField(initial=0)
    tflops = fields.IntegerField(initial=0)
    results = fields.JsonField(initial=[])
    errors = fields.JsonField(initial=[])

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


class TaskAssignment(models.Model):
    producer_id = fields.CharField(immutable=True)
    worker_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)
    train_data = fields.JsonField(required=False)
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


class VerificationDeclaration(models.Model):
    class Status:
        PUBLISHED = 'published'
        RUN = 'run'
        COMPLETED = 'completed'

    producer_id = fields.CharField(immutable=True)
    verifiers_requested = fields.IntegerField(immutable=True)
    verifiers_needed = fields.IntegerField()
    task_declaration_id = fields.CharField(immutable=True)
    status = fields.CharField(initial=Status.PUBLISHED)
    progress = fields.IntegerField(initial=0)

    @cached_property
    def producer(self):
        return ProducerNode.get(self.producer_id)

    @cached_property
    def task_declaration(self):
        return TaskDeclaration.get(self.task_declaration_id)


class VerificationAssignment(models.Model):
    producer_id = fields.CharField(immutable=True)
    verifier_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)
    verification_declaration_id = fields.CharField(immutable=True)
    train_results = fields.JsonField(required=False)
    progress = fields.IntegerField(initial=0)
    tflops = fields.IntegerField(initial=0)
    result = fields.CharField(required=False)
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
