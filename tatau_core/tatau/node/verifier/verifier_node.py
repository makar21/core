import time
from logging import getLogger

from tatau_core import settings
from tatau_core.tatau.models import VerifierNode, TaskDeclaration, VerificationAssignment
from tatau_core.tatau.node.node import Node
from tatau_core.tatau.node.verifier.verify_weights import verify_train_results

logger = getLogger()


class Verifier(Node):
    node_type = Node.NodeType.VERIFIER
    asset_class = VerifierNode

    def get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self.process_task_declaration_transaction,
            VerificationAssignment.get_asset_name(): self.process_verification_assignment_transaction,
        }

    def process_task_declaration_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'TRANSFER':
            return

        task_declaration = TaskDeclaration.get(asset_id)
        if task_declaration.verifiers_needed == 0:
            return

        self.process_task_declaration(task_declaration)

    def process_task_declaration(self, task_declaration):
        exists = VerificationAssignment.exists(
            additional_match={
                'assets.data.verifier_id': self.asset_id,
                'assets.data.task_declaration_id': task_declaration.asset_id,
            },
            created_by_user=False
        )

        if exists:
            logger.debug('{} has already created verification assignment to {}'.format(self, task_declaration))
            return

        verification_assignment = VerificationAssignment.create(
            producer_id=task_declaration.producer_id,
            verifier_id=self.asset_id,
            task_declaration_id=task_declaration.asset_id,
            recipients=task_declaration.producer.address
        )
        logger.info('{} added {}'.format(self, verification_assignment))

    def process_verification_assignment_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        # skip another assignment
        verification_assignment = VerificationAssignment.get(asset_id)
        if verification_assignment.verifier_id != self.asset_id:
            return

        self.process_verification_assignment(verification_assignment)

    def process_verification_assignment(self, verification_assignment):
        if verification_assignment.state == VerificationAssignment.State.DATA_IS_READY:
            logger.info('{} start verify {}'.format(self, verification_assignment))

            verification_assignment.result = verify_train_results(verification_assignment.train_results)
            verification_assignment.progress = 100
            verification_assignment.tflops = 99
            verification_assignment.state = VerificationAssignment.State.FINISHED
            verification_assignment.set_encryption_key(verification_assignment.producer.enc_key)
            verification_assignment.save(recipients=verification_assignment.producer.address)

            logger.info('{} finish verify {} results: {}'.format(
                self, verification_assignment, verification_assignment.result)
            )

    def process_task_declarations(self):
        for task_declaration in TaskDeclaration.list(created_by_user=False):
            if task_declaration.state == TaskDeclaration.State.DEPLOYMENT and task_declaration.verifiers_needed > 0:
                self.process_task_declaration(task_declaration)

    def process_verification_assignments(self):
        for verification_assignment in VerificationAssignment.list():
            self.process_verification_assignment(verification_assignment)

    def search_tasks(self):
        while True:
            try:
                self.process_task_declarations()
                self.process_verification_assignments()
                time.sleep(settings.VERIFIER_PROCESS_INTERVAL)
            except Exception as e:
                logger.exception(e)
