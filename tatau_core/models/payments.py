from logging import getLogger

from tatau_core.db import models, fields
from tatau_core.models.task import TaskDeclaration
from tatau_core.models.nodes import ProducerNode, WorkerNode, VerifierNode
from tatau_core.utils import cached_property

logger = getLogger('tatau_core')


class WorkerPayment(models.Model):
    producer_id = fields.CharField(immutable=True)
    worker_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)
    train_iteration = fields.IntegerField(immutable=True)
    train_iteration_retry = fields.IntegerField(immutable=True)
    tflops = fields.FloatField(immutable=True)
    tokens = fields.FloatField(immutable=True)

    @cached_property
    def producer(self) -> ProducerNode:
        return ProducerNode.get(self.producer_id, db=self.db, encryption=self.encryption)

    @cached_property
    def worker(self) -> WorkerNode:
        return WorkerNode.get(self.worker_id, db=self.db, encryption=self.encryption)

    @cached_property
    def task_declaration(self) -> TaskDeclaration:
        return TaskDeclaration.get(self.task_declaration_id, db=self.db, encryption=self.encryption)


class VerifierPayment(models.Model):
    producer_id = fields.CharField(immutable=True)
    verifier_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)
    train_iteration = fields.IntegerField(immutable=True)
    tflops = fields.FloatField(immutable=True)
    tokens = fields.FloatField(immutable=True)

    @cached_property
    def producer(self) -> ProducerNode:
        return ProducerNode.get(self.producer_id, db=self.db, encryption=self.encryption)

    @cached_property
    def verifier(self) -> VerifierNode:
        return VerifierNode.get(self.verifier_id, db=self.db, encryption=self.encryption)

    @cached_property
    def task_declaration(self) -> TaskDeclaration:
        return TaskDeclaration.get(self.task_declaration_id, db=self.db, encryption=self.encryption)

