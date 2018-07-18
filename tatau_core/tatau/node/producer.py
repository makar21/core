import logging

from tatau_core.tatau.models import ProducerNode, TaskDeclaration, TaskAssignment, VerificationAssignment
from .node import Node

log = logging.getLogger()


class Producer(Node):
    node_type = Node.NodeType.PRODUCER
    asset_class = ProducerNode

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exit_on_task_completion = kwargs.get('exit_on_task_completion')

    def get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self.process_task_declaration,
            TaskAssignment.get_asset_name(): self.process_task_assignment,
            VerificationAssignment.get_asset_name(): self.process_verification_assignment,
        }

    def process_task_assignment(self, asset_id, transaction):
        task_assignment = TaskAssignment.get(asset_id)
        if task_assignment.producer_id != self.asset_id:
            return

        if task_assignment.state == TaskAssignment.State.REJECTED:
            return

        log.info('Process: {}, state: {}'.format(task_assignment, task_assignment.state))

        task_declaration = task_assignment.task_declaration
        if transaction['operation'] == 'CREATE':
            log.info('{} requested {}'.format(task_assignment.worker, task_assignment))
            if task_declaration.is_task_assignment_allowed(task_assignment):
                task_assignment.state = TaskAssignment.State.ACCEPTED
                task_assignment.save()

                task_declaration.workers_needed -= 1
                task_declaration.save()
            else:
                task_assignment.state = TaskAssignment.State.REJECTED
                task_assignment.save()
            return

        if task_assignment.state == TaskAssignment.State.IN_PROGRESS:
            task_declaration.tflops += task_assignment.tflops
            task_declaration.save()
            return

    def process_verification_assignment(self, asset_id, transaction):
        verification_assignment = VerificationAssignment.get(asset_id)
        if verification_assignment.producer_id != self.asset_id:
            return

        if verification_assignment.state == VerificationAssignment.State.REJECTED:
            return

        log.info('Process: {}, state: {}'.format(verification_assignment, verification_assignment.state))

        task_declaration = verification_assignment.task_declaration
        if transaction['operation'] == 'CREATE':
            log.info('{} requested {}'.format(verification_assignment.verifier, verification_assignment))
            if task_declaration.is_verification_assignment_allowed(verification_assignment):
                verification_assignment.state = VerificationAssignment.State.ACCEPTED
                verification_assignment.save()

                task_declaration.verifiers_needed -= 1
                task_declaration.save()
            else:
                verification_assignment.state = VerificationAssignment.State.REJECTED
                verification_assignment.save()
            return

        if verification_assignment.state == VerificationAssignment.State.IN_PROGRESS:
            task_declaration.tflops += verification_assignment.tflops
            task_declaration.save()

        if verification_assignment.state == VerificationAssignment.State.FINISHED:
            # process results
            task_declaration.save()

    def process_task_declaration(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        task_declaration = TaskDeclaration.get(asset_id)
        if task_declaration.producer_id != self.asset_id or task_declaration.state == TaskDeclaration.State.COMPLETED:
            return

        if not task_declaration.ready_for_start():
            return

        if task_declaration.state == TaskDeclaration.State.DEPLOYMENT:
            task_declaration.assign_train_data()
            return

        if task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
            if task_declaration.epoch_is_ready():
                # collect results from epoch
                for task_assignment in task_declaration.get_task_assignments():
                    task_declaration.results.append({
                        'worker_id': task_assignment.worker_id,
                        'result': task_assignment.result,
                        'error': task_assignment.error
                    })
                task_declaration.assign_verification_data()
            return

        if task_declaration.state == TaskDeclaration.State.VERIFY_IN_PROGRESS:
            if task_declaration.verification_is_ready():
                if task_declaration.all_done():
                    # TODO: summarize and save final result
                    task_declaration.state = TaskDeclaration.State.COMPLETED
                    task_declaration.save()
                    log.info('{} is finished'.format(task_declaration))
                else:
                    # TODO: summarize and update train data
                    task_declaration.assign_train_data()
            return
