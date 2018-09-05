from logging import getLogger

from tatau_core.db import models, fields
from tatau_core.models.nodes import ProducerNode, WorkerNode
from tatau_core.utils import cached_property

logger = getLogger()


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
        TIMEOUT = 'timeout'

    producer_id = fields.CharField(immutable=True)
    worker_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)

    train_data_id = fields.CharField(null=True, initial=None)
    train_progress_id = fields.CharField(null=True, initial=None)
    train_result_id = fields.CharField(null=True, initial=None)

    @cached_property
    def producer(self):
        return ProducerNode.get(self.producer_id, db=self.db, encryption=self.encryption)

    @cached_property
    def worker(self):
        return WorkerNode.get(self.worker_id, db=self.db, encryption=self.encryption)

    @cached_property
    def task_declaration(self):
        from tatau_core.models import TaskDeclaration
        return TaskDeclaration.get(self.task_declaration_id, db=self.db, encryption=self.encryption)

    @cached_property
    def train_data(self):
        return TrainData.get(self.train_data_id, db=self.db, encryption=self.encryption)

    @cached_property
    def train_result(self):
        if self.train_result_id:
            return TrainResult.get(self.train_result_id, db=self.db, encryption=self.encryption)

        for train_result in TrainResult.enumerate(
                additional_match={
                    'assets.data.task_assignment_id': self.asset_id
                },
                created_by_user=False,
                db=self.db,
                encryption=self.encryption
        ):
            self.train_result_id = train_result.asset_id
            self.save()
            return train_result


class TrainData(models.Model):
    model_code = fields.EncryptedCharField(immutable=True)
    x_train = fields.EncryptedJsonField(immutable=True)
    y_train = fields.EncryptedJsonField(immutable=True)
    data_index = fields.IntegerField(immutable=True)
    batch_size = fields.IntegerField(immutable=True)

    task_assignment_id = fields.CharField()
    initial_weights = fields.EncryptedCharField()
    epochs = fields.IntegerField()
    train_iteration = fields.IntegerField()


class TrainResult(models.Model):
    task_assignment_id = fields.CharField(immutable=True)

    progress = fields.FloatField(initial=0.0)
    tflops = fields.FloatField(initial=0.0)

    weights = fields.EncryptedCharField(required=False)
    error = fields.EncryptedCharField(required=False)

    loss = fields.FloatField(required=False)
    accuracy = fields.FloatField(required=False)
    train_history = fields.JsonField(required=False)

    def clean(self):
        self.progress = 0.0
        self.tflops = 0.0
        self.weights = None
        self.error = None
        self.loss = 0.0
        self.accuracy = 0.0
        self.train_history = None

    @property
    def finished(self):
        return self.progress == 100.0
