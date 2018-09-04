from logging import getLogger

from bigchaindb_driver.exceptions import MissingPrivateKeyError

from tatau_core.db import models, fields
from tatau_core.models.nodes import ProducerNode, VerifierNode
from tatau_core.utils import cached_property

logger = getLogger()


class VerificationAssignment(models.Model):
    class State:
        INITIAL = 'initial'
        REJECTED = 'rejected'
        ACCEPTED = 'accepted'
        PARTIAL_DATA_IS_READY = 'partial data is ready'
        PARTIAL_DATA_IS_DOWNLOADED = 'partial data is downloaded'
        DATA_IS_READY = 'data is ready'
        IN_PROGRESS = 'in progress'
        VERIFICATION_FINISHED = 'verification is finished'
        FINISHED = 'finished'

    producer_id = fields.CharField(immutable=True)
    verifier_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)
    x_test_ipfs = fields.EncryptedCharField(required=False)
    y_test_ipfs = fields.EncryptedCharField(required=False)
    model_code_ipfs = fields.EncryptedCharField(required=False)

    train_results = fields.EncryptedJsonField(required=False)
    current_iteration = fields.IntegerField(initial=0)

    progress = fields.FloatField(initial=0.0)
    tflops = fields.FloatField(initial=0.0)
    result = fields.EncryptedJsonField(required=False)

    weights = fields.EncryptedCharField(required=False)
    loss = fields.FloatField(required=False)
    accuracy = fields.FloatField(required=False)

    error = fields.EncryptedCharField(required=False)
    distribute_history_id = fields.CharField(null=True, initial=None)

    def clean(self):
        self.progress = 0.0
        self.tflops = 0.0
        self.result = None
        self.weights = None
        self.loss = 0.0
        self.accuracy = 0.0

    @cached_property
    def producer(self):
        return ProducerNode.get(self.producer_id, db=self.db, encryption=self.encryption)

    @cached_property
    def verifier(self):
        return VerifierNode.get(self.verifier_id, db=self.db, encryption=self.encryption)

    @cached_property
    def task_declaration(self):
        from tatau_core.models import TaskDeclaration
        return TaskDeclaration.get(self.task_declaration_id, db=self.db, encryption=self.encryption)

    @cached_property
    def distribute_history(self):
        if self.distribute_history_id:
            return DistributeHistory.get(self.distribute_history_id, db=self.db, encryption=self.encryption)

        # try to load exist if it present
        distribute_histories = DistributeHistory.list(
            additional_match={
                'assets.data.task_declaration_id': self.task_declaration_id
            },
            created_by_user=True,
            db=self.db,
            encryption=self.encryption
        )

        distribute_history = None
        if len(distribute_histories):
            for dh in distribute_histories:
                if dh.verification_assignment_id == self.asset_id:
                    distribute_history = dh
                    break

        if not distribute_history:
            distribute_history = DistributeHistory(
                task_declaration_id=self.task_declaration_id,
                verification_assignment_id=self.asset_id,
                distribute_transactions={},
                db=self.db,
                encryption=self.encryption
            )
            distribute_history.save()

        try:
            self.distribute_history_id = distribute_history.asset_id
            self.save()
        except MissingPrivateKeyError:
            pass

        return distribute_history


class DistributeHistory(models.Model):
    task_declaration_id = fields.CharField(immutable=True)
    verification_assignment_id = fields.CharField(immutable=True)
    distribute_transactions = fields.EncryptedJsonField()

    @cached_property
    def task_declaration(self):
        from tatau_core.models import TaskDeclaration
        return TaskDeclaration.get(self.task_declaration_id, db=self.db, encryption=self.encryption)

    @cached_property
    def verification_assignment(self):
        return VerificationAssignment.get(self.verification_assignment_id, db=self.db, encryption=self.encryption)

