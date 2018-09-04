from logging import getLogger

from tatau_core.db import models, fields, exceptions
from tatau_core.models.nodes import ProducerNode, VerifierNode, WorkerNode
from tatau_core.utils import cached_property

logger = getLogger()


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
    estimator_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)

    estimation_data = fields.EncryptedJsonField(required=False)
    tflops = fields.FloatField(initial=0.0)
    error = fields.EncryptedCharField(required=False)

    @cached_property
    def producer(self):
        return ProducerNode.get(self.producer_id, db=self.db, encryption=self.encryption)

    @cached_property
    def estimator(self):
        try:
            return VerifierNode.get(self.estimator_id, db=self.db, encryption=self.encryption)
        except exceptions.Asset.WrongType:
            return WorkerNode.get(self.estimator_id, db=self.db, encryption=self.encryption)

    @cached_property
    def task_declaration(self):
        from .task_declaration import TaskDeclaration
        return TaskDeclaration.get(self.task_declaration_id, db=self.db, encryption=self.encryption)
