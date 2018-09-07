from logging import getLogger

from tatau_core.db import models, fields
from tatau_core.models.nodes import ProducerNode, WorkerNode
from tatau_core.utils import cached_property

logger = getLogger()


class TrainData(models.Model):
    # owner only producer, share data with workers
    data_index = fields.IntegerField(immutable=True)
    batch_size = fields.IntegerField(immutable=True)

    # this data may be encrypted for different workers
    model_code = fields.EncryptedCharField()
    x_train = fields.EncryptedJsonField()
    y_train = fields.EncryptedJsonField()

    task_assignment_id = fields.CharField(null=True, initial=None)
    initial_weights = fields.EncryptedCharField()
    epochs = fields.IntegerField()
    # train data will be always created for 1st iteration
    current_iteration = fields.IntegerField(initial=1)


class TrainResult(models.Model):
    # owner only worker, share data with producer
    class State:
        INITIAL = 'initial'
        IN_PROGRESS = 'in progress'
        FINISHED = 'finished'

    task_assignment_id = fields.CharField(immutable=True)
    state = fields.CharField(initial=State.INITIAL)

    progress = fields.FloatField(initial=0.0)
    tflops = fields.FloatField(initial=0.0)
    current_iteration = fields.IntegerField(initial=0)

    weights = fields.EncryptedCharField(required=False)
    error = fields.EncryptedCharField(required=False)

    loss = fields.FloatField(required=False)
    accuracy = fields.FloatField(required=False)
    train_history = fields.JsonField(required=False)

    @cached_property
    def task_assignment(self):
        return TaskAssignment.get(self.task_assignment_id, db=self.db, encryption=self.encryption)

    def clean(self):
        self.progress = 0.0
        self.tflops = 0.0
        self.weights = None
        self.error = None
        self.loss = 0.0
        self.accuracy = 0.0
        self.train_history = None


class TaskAssignment(models.Model):
    class State:
        INITIAL = 'initial'
        READY = 'ready'
        REASSIGN = 'reassign'
        REJECTED = 'rejected'
        ACCEPTED = 'accepted'
        TRAINING = 'training'
        FINISHED = 'finished'
        FAKE_RESULTS = 'fake results'
        TIMEOUT = 'timeout'
        FORGOTTEN = 'forgotten'

    producer_id = fields.CharField(immutable=True)
    worker_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)

    train_data_id = fields.CharField(null=True, initial=None)
    train_result_id = fields.CharField(null=True, initial=None)

    @cached_property
    def producer(self) -> ProducerNode:
        return ProducerNode.get(self.producer_id, db=self.db, encryption=self.encryption)

    @cached_property
    def worker(self) -> WorkerNode:
        return WorkerNode.get(self.worker_id, db=self.db, encryption=self.encryption)

    @cached_property
    def task_declaration(self):
        from tatau_core.models import TaskDeclaration
        return TaskDeclaration.get(self.task_declaration_id, db=self.db, encryption=self.encryption)

    @cached_property
    def train_data(self) -> TrainData:
        td = TrainData.get(self.train_data_id, db=self.db, encryption=self.encryption)
        # creator and owner must be producer, share data with worker
        td.set_encryption_key(self.worker.enc_key)
        return td

    @cached_property
    def train_result(self) -> TrainResult:
        tr = TrainResult.get(self.train_result_id, db=self.db, encryption=self.encryption)
        # creator and owner must be worker, share data with producer
        tr.set_encryption_key(self.producer.enc_key)
        return tr

    @property
    def iteration_is_finished(self):
        return self.train_result.current_iteration == self.train_data.current_iteration \
               and self.train_result.state == TrainResult.State.FINISHED
