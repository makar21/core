from logging import getLogger

from tatau_core.db import models, fields, exceptions
from tatau_core.models.nodes import ProducerNode, VerifierNode, WorkerNode
from tatau_core.utils import cached_property

logger = getLogger()


class EstimationAssignment(models.Model):
    class State:
        INITIAL = 'initial'
        REASSIGN = 'reassign'
        REJECTED = 'rejected'
        ACCEPTED = 'accepted'
        DATA_IS_READY = 'data is ready'
        FINISHED = 'finished'
        TIMEOUT = 'timeout'

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
    def estimation_data(self):
        return EstimationData.get(self.estimation_data_id, db=self.db, encryption=self.encryption)

    @cached_property
    def estimation_result(self):
        if self.estimation_result_id:
            return EstimationResult.get(self.estimation_result_id, db=self.db, encryption=self.encryption)

        for estimation_result in EstimationResult.enumerate(
            additional_match={
                'assets.data.estimation_assignment_id': self.asset_id
            },
            created_by_user=False,
            db=self.db,
            encryption=self.encryption
        ):
            self.estimation_result_id = estimation_result.asset_id
            self.save()
            return estimation_result


class EstimationData(models.Model):
    estimation_assignment_id = fields.CharField(immutable=True)
    x_train = fields.EncryptedCharField(immutable=True)
    y_train = fields.EncryptedCharField(immutable=True)
    model_code = fields.EncryptedCharField(immutable=True)
    initial_weights = fields.EncryptedCharField(immutable=True)
    batch_size = fields.IntegerField(immutable=True)

    @cached_property
    def estimation_assignment(self) -> EstimationAssignment:
        return EstimationAssignment.get(self.estimation_assignment_id, db=self.db, encryption=self.encryption)


class EstimationResult(models.Model):
    estimation_assignment_id = fields.CharField(immutable=True)
    tflops = fields.FloatField(immutable=True)
    error = fields.EncryptedCharField(immutable=True, null=True)

    @cached_property
    def estimation_assignment(self) -> EstimationAssignment:
        return EstimationAssignment.get(self.estimation_assignment_id, db=self.db, encryption=self.encryption)
