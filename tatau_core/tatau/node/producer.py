import json
import logging
import re

from tatau_core import ipfs
from tatau_core import settings
from ..tasks import Task, TaskDeclaration, TaskAssignment, VerificationDeclaration, VerificationAssignment
from .node import Node

logger = logging.getLogger()


class Producer(Node):
    node_type = Node.NodeType.PRODUCER

    key_name = 'producer'
    asset_name = 'Producer info'

    def get_node_info(self):
        return {
            'enc_key': self.encryption.get_public_key().decode(),
            'producer_api_url': self.producer_api_url,
        }

    @property
    def producer_api_url(self):
        return 'http://{}:{}'.format(settings.PRODUCER_HOST, settings.PRODUCER_PORT)

    def get_tx_methods(self):
        return {
            Task.TaskType.TASK_ASSIGNMENT: self.process_task_assignment,
            Task.TaskType.VERIFICATION_ASSIGNMENT: self.process_verification_assignment,
        }

    def ignore_operation(self, operation):
        return operation in ['CREATE']

    def process_task_assignment(self, asset_id, transaction):
        task_assignment = TaskAssignment.get(self, asset_id)
        if task_assignment.owner_producer_id != self.asset_id:
            return

        logger.info('Process: {}'.format(task_assignment))

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

        vd = VerificationDeclaration.get(self, verification_assignment.verification_declaration_id)
        vd.status = VerificationDeclaration.Status.COMPLETED

        if verification_assignment.verified:
            logger.info('Task result is verified')
        else:
            logger.info('Task result is not verified')

    def on_worker_ping(self, task_asset_id, worker_asset_id):
        logger.info('Worker: {} requested task: {}'.format(worker_asset_id, task_asset_id))

        task_declaration = TaskDeclaration.get(self, task_asset_id)
        if task_declaration.workers_needed == 0:
            logger.info('No more workers needed')
            return

        worker_index = task_declaration.workers_requested - task_declaration.workers_needed
        if worker_index < 0:
            return

        exists = TaskAssignment.exists(
            node=self,
            additional_match={
                'assets.data.worker_id': worker_asset_id,
                'assets.data.task_declaration_id': task_asset_id
            }
        )

        if exists:
            logger.info('Worker: {} have already worked on this task: {}'.format(worker_asset_id, task_asset_id))
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

        TaskAssignment.add(
            node=self,
            worker_id=worker_asset_id,
            model_code=task_declaration.train_model.code_ipfs,
            x_train_ipfs=x_train_ipfs,
            y_train_ipfs=y_train_ipfs,
            x_test_ipfs=task_declaration.dataset.x_test_ipfs,
            y_test_ipfs=task_declaration.dataset.y_test_ipfs,
            batch_size=task_declaration.batch_size,
            epochs=task_declaration.epochs,
            task_declaration_id=task_declaration.asset_id
        )

        task_declaration.workers_needed -= 1
        task_declaration.save(self.db)

    def on_verifier_ping(self, task_asset_id, verifier_asset_id):
        verification_declaration = VerificationDeclaration.get(self, task_asset_id)
        if verification_declaration.verifiers_needed == 0:
            logger.info('No more verifiers needed')
            return

        exists = VerificationAssignment.exists(
            node=self,
            additional_match={
                'assets.data.verifier_id': verifier_asset_id,
                'assets.data.task_declaration_id': verification_declaration.task_declaration_id
            }
        )

        if exists:
            logger.info('Verifier: {} have already worked on this task: {}'.format(verifier_asset_id, task_asset_id))
            return

        task_declaration = TaskDeclaration.get(self, verification_declaration.task_declaration_id)
        VerificationAssignment.add(
            node=self,
            verifier_id=verifier_asset_id,
            verification_declaration_id=verification_declaration.asset_id,
            train_results=task_declaration.results,
            task_declaration_id=task_declaration.asset_id
        )

        verification_declaration.verifiers_needed -= 1
        verification_declaration.save(self.db)
