from logging import getLogger

from tatau_core.db import models, fields, exceptions
from tatau_core.models.nodes import ProducerNode, VerifierNode, WorkerNode
from tatau_core.utils import cached_property

logger = getLogger()


class EstimationData(models.Model):
    # this data may be encrypted for different estimators
    chunk_ipfs = fields.EncryptedCharField()
    model_code_ipfs = fields.EncryptedCharField()

    estimation_assignment_id = fields.CharField(null=True, initial=None)

    @cached_property
    def estimation_assignment(self):
        return EstimationAssignment.get(self.estimation_assignment_id, db=self.db, encryption=self.encryption)

    @cached_property
    def weights_ipfs(self):
        return self.estimation_assignment.task_declaration.weights_ipfs

    @cached_property
    def batch_size(self):
        return self.estimation_assignment.task_declaration.batch_size


class EstimationResult(models.Model):
    # owner only estimator, share data with producer
    class State:
        INITIAL = 'initial'
        IN_PROGRESS = 'in progress'
        FINISHED = 'finished'

    estimation_assignment_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)
    tflops = fields.FloatField(initial=0.0)
    progress = fields.FloatField(initial=0.0)
    error = fields.EncryptedCharField(null=True, initial=None)


class EstimationAssignment(models.Model):
    class State:
        INITIAL = 'initial'
        READY = 'ready'
        REASSIGN = 'reassign'
        REJECTED = 'rejected'
        ACCEPTED = 'accepted'
        ESTIMATING = 'estimating'
        FINISHED = 'finished'
        TIMEOUT = 'timeout'
        FORGOTTEN = 'forgotten'

    producer_id = fields.CharField(immutable=True)
    estimator_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)
    estimation_data_id = fields.CharField(required=False)
    estimation_result_id = fields.CharField(required=False)

    @cached_property
    def producer(self) -> ProducerNode:
        return ProducerNode.get(self.producer_id, db=self.db, encryption=self.encryption)

    @cached_property
    def estimator(self):
        try:
            return VerifierNode.get(self.estimator_id, db=self.db, encryption=self.encryption)
        except exceptions.Asset.WrongType:
            return WorkerNode.get(self.estimator_id, db=self.db, encryption=self.encryption)

    @cached_property
    def task_declaration(self):
        from .task import TaskDeclaration
        return TaskDeclaration.get(self.task_declaration_id, db=self.db, encryption=self.encryption)

    @cached_property
    def estimation_data(self) -> EstimationData:
        ed = EstimationData.get(self.estimation_data_id, db=self.db, encryption=self.encryption)
        # creator and owner must be producer, share data with estimator
        ed.set_encryption_key(self.estimator.enc_key)
        return ed

    @cached_property
    def estimation_result(self) -> EstimationResult:
        er = EstimationResult.get(self.estimation_result_id, db=self.db, encryption=self.encryption)
        # creator and owner must be estimator, share data with producer
        er.set_encryption_key(self.producer.enc_key)
        return er

