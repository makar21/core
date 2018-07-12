import json
import logging
import re

from tatau_core import ipfs
from tatau_core import settings
from tatau_core.db.exceptions import StopWSClient
from ..tasks import Task, TaskDeclaration, TaskAssignment, VerificationDeclaration, VerificationAssignment
from .node import Node

logger = logging.getLogger()


class Producer(Node):
    node_type = Node.NodeType.PRODUCER

    key_name = 'producer'
    asset_name = 'Producer info'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exit_on_task_completion = kwargs.get('exit_on_task_completion')

    def get_tx_methods(self):
        return {
            Task.TaskType.TASK_ASSIGNMENT: self.process_task_assignment,
            Task.TaskType.VERIFICATION_ASSIGNMENT: self.process_verification_assignment,
        }

    def ignore_operation(self, operation):
        return False

    def process_task_assignment(self, asset_id, transaction):
        task_assignment = TaskAssignment.get(self, asset_id)
        if task_assignment.owner_producer_id != self.asset_id:
            return

        logger.info('Process: {}'.format(task_assignment))

        if transaction['operation'] == 'CREATE':
            logger.info('Worker: {} requested task: {}'.format(task_assignment.worker_id, asset_id))
            self.assign_task(task_assignment)
            return

        task_declaration = TaskDeclaration.get(self, task_assignment.task_declaration_id)
        task_declaration.status = TaskDeclaration.Status.RUN
        task_declaration.tflops += task_assignment.tflops

        if not task_assignment.result and not task_assignment.error:
            task_declaration.save(self.db)
            return

        logger.info('Task assignment is finished worker: {}'.format(task_assignment.worker_id))

        if task_assignment.result:
            task_declaration.results.append({
                'worker_id': task_assignment.worker_id,
                'result': self.decrypt_text(task_assignment.result)
            })

        if task_assignment.error:
            task_declaration.errors.append({
                'worker_id': task_assignment.worker_id,
                'error': self.decrypt_text(task_assignment.error)
            })

        task_declaration.encrypted_text = self.encrypt_text(json.dumps(task_declaration.results))
        task_declaration.encrypted_text_errors = self.encrypt_text(json.dumps(task_declaration.errors))

        if len(task_declaration.results) + len(task_declaration.errors) == task_declaration.workers_requested:
            task_declaration.progress = 100

        publish_verification_declaration = False
        if task_declaration.progress == 100:
            task_declaration.status = TaskDeclaration.Status.COMPLETED
            publish_verification_declaration = True

        # save Task Declaration before create Verification Declaration to be sure all results are saved
        task_declaration.save(self.db)

        if publish_verification_declaration:
            VerificationDeclaration.add(
                node=self,
                verifiers_needed=task_declaration.verifiers_needed,
                task_declaration_id=task_declaration.asset_id,
            )

    def process_verification_assignment(self, asset_id, transaction):
        verification_assignment = VerificationAssignment.get(self, asset_id)
        if verification_assignment.owner_producer_id != self.asset_id:
            return

        if transaction['operation'] == 'CREATE':
            logger.info('Verifier: {} requested verification: {}'.format(verification_assignment.verifier_id, asset_id))
            self.assign_verification(verification_assignment)
            return

        vd = VerificationDeclaration.get(self, verification_assignment.verification_declaration_id)
        vd.status = VerificationDeclaration.Status.COMPLETED

        if verification_assignment.verified == None:
            return

        if verification_assignment.verified:
            logger.info('Task result is verified')
        else:
            logger.info('Task result is not verified')

        if self.exit_on_task_completion:
            raise StopWSClient

    def assign_task(self, task_assignment):
        task_declaration = TaskDeclaration.get(
            self,
            task_assignment.task_declaration_id
        )
        if task_declaration.workers_needed == 0:
            logger.info('No more workers needed')
            return

        worker_index = task_declaration.workers_requested - task_declaration.workers_needed
        if worker_index < 0:
            return

        ipfs_dir = ipfs.Directory(multihash=task_declaration.dataset.train_dir_ipfs)
        dirs, files = ipfs_dir.ls()

        # TODO: optimize this shit
        files_count_for_worker = int(len(files) / (2 * task_declaration.workers_requested))
        file_indexes = [x + files_count_for_worker * worker_index for x in range(files_count_for_worker)]

        x_train_ipfs = []
        y_train_ipfs = []
        for f in files:
            index = int(re.findall('\d+', f.name)[0])
            if index in file_indexes:
                if f.name[0] == 'x':
                    x_train_ipfs.append(f.multihash)
                elif f.name[0] == 'y':
                    y_train_ipfs.append(f.multihash)

        task_assignment.assign(
            node=self,
            model_code=task_declaration.train_model.code_ipfs,
            x_train_ipfs=x_train_ipfs,
            y_train_ipfs=y_train_ipfs,
            x_test_ipfs=task_declaration.dataset.x_test_ipfs,
            y_test_ipfs=task_declaration.dataset.y_test_ipfs,
            batch_size=task_declaration.batch_size,
            epochs=task_declaration.epochs,
        )

        task_declaration.workers_needed -= 1
        task_declaration.save(self.db)

    def assign_verification(self, verification_assignment):
        verification_declaration = VerificationDeclaration.get(
            self,
            verification_assignment.verification_declaration_id
        )
        if verification_declaration.verifiers_needed == 0:
            logger.info('No more verifiers needed')
            return

        task_declaration = TaskDeclaration.get(self, verification_declaration.task_declaration_id)
        verification_assignment.assign(
            node=self,
            train_results=task_declaration.results,
        )

        verification_declaration.verifiers_needed -= 1
        verification_declaration.save(self.db)
