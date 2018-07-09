from ..node.node import Node
from .task import Task


class VerificationDeclaration(Task):
    task_type = Task.TaskType.VERIFICATION_DECLARATION

    class Status:
        PUBLISHED = 'published'
        RUN = 'run'
        COMPLETED = 'completed'

    def __init__(self, owner_producer_id, verifiers_needed, verifiers_requested, asset_id, task_declaration_id,
                 *args, **kwargs):
        super().__init__(asset_id, *args, **kwargs)
        self.owner_producer_id = owner_producer_id
        self.verifiers_needed = verifiers_needed
        self.verifiers_requested = verifiers_requested
        self.task_declaration_id = task_declaration_id
        self.status = kwargs.get('status', VerificationDeclaration.Status.PUBLISHED)
        self.progress = kwargs.get('progress', 0)

    def get_data(self):
        data = super(VerificationDeclaration, self).get_data()
        data.update({
            'owner_producer_id': self.owner_producer_id,
            'task_declaration_id': self.task_declaration_id,
            'verifiers_requested': self.verifiers_requested
        })
        return data

    def get_metadata(self):
        return {
            'verifiers_needed': self.verifiers_needed,
            'status': self.status,
            'progress': self.progress
        }

    # noinspection PyMethodOverriding
    @classmethod
    def add(cls, node, verifiers_needed, task_declaration_id, *args, **kwargs):
        if node.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can create task declaration')

        producer = node
        verification_declaration = cls(
            owner_producer_id=producer.asset_id,
            verifiers_needed=verifiers_needed,
            verifiers_requested=verifiers_needed,
            task_declaration_id=task_declaration_id,
            asset_id=None
        )

        asset_id, created = producer.db.create_asset(
            data=verification_declaration.get_data(),
            metadata=verification_declaration.get_metadata()
        )

        verification_declaration.asset_id = asset_id
        return verification_declaration

    @classmethod
    def get(cls, node, asset_id):
        asset = node.db.retrieve_asset(asset_id)

        return cls(
            owner_producer_id=asset.data['owner_producer_id'],
            verifiers_needed=asset.metadata['verifiers_needed'],
            verifiers_requested=asset.data['verifiers_requested'],
            task_declaration_id=asset.data['task_declaration_id'],
            asset_id=asset_id,
            status=asset.metadata['status'],
            progress=asset.metadata['progress']
        )

