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

    producer_id = fields.CharField(immutable=True)
    worker_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)

    train_data = fields.EncryptedJsonField(required=False)
    current_iteration = fields.IntegerField(initial=0)

    progress = fields.FloatField(initial=0.0)
    tflops = fields.FloatField(initial=0.0)

    result = fields.EncryptedCharField(required=False)
    loss = fields.FloatField(required=False)
    accuracy = fields.FloatField(required=False)
    train_history = fields.JsonField(required=False)

    error = fields.EncryptedCharField(required=False)

    def clean(self):
        self.progress = 0.0
        self.tflops = 0.0
        self.result = None
        self.error = None
        self.loss = 0.0
        self.accuracy = 0.0
        self.train_history = None

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
