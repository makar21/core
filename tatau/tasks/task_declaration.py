import json

from tatau.dataset import DataSet
from tatau.node import Node
from tatau.train_model import TrainModel
from .task import Task


class TaskDeclaration(Task):
    task_type = Task.TaskType.TASK_DECLARATION

    class Status:
        DEPLOYMENT = 'deployment'
        RUN = 'run'
        COMPLETED = 'completed'

    def __init__(self, owner_producer_id, dataset, train_model, workers_needed, workers_requested, verifiers_needed,
                 epochs, asset_id, *args, **kwargs):
        super().__init__(asset_id, *args, **kwargs)
        self.owner_producer_id = owner_producer_id
        self.dataset = dataset
        self.train_model = train_model
        self.workers_needed = workers_needed
        self.workers_requested = workers_requested
        self.verifiers_needed = verifiers_needed
        self.epochs = epochs
        self.status = kwargs.get('status', TaskDeclaration.Status.DEPLOYMENT)
        self.progress = kwargs.get('progress', 0)
        self.results = kwargs.get('results', [])
        self.encrypted_text = kwargs.get('encrypted_text', None)

    def get_data(self):
        return {
            'owner_producer_id': self.owner_producer_id,
            'train_model_id': self.train_model.asset_id,
            'dataset_id': self.dataset.asset_id,
            'workers_needed': self.workers_needed,
            'workers_requested': self.workers_requested,
            'verifiers_needed': self.verifiers_needed,
            'epochs': self.epochs,
            'status': self.status,
            'progress': self.progress,
            'results': self.encrypted_text or self.results,
        }

    # noinspection PyMethodOverriding
    @classmethod
    def add(cls, node, dataset, train_model, workers_needed, verifiers_needed, epochs, *args, **kwargs):
        if node.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can create task declaration')

        producer = node
        task_declaration = cls(
            owner_producer_id=producer.asset_id,
            dataset=dataset,
            train_model=train_model,
            workers_needed=workers_needed,
            workers_requested=workers_needed,
            verifiers_needed=verifiers_needed,
            results=producer.encrypt_text(json.dumps([])),
            epochs=epochs,
            asset_id=None
        )

        asset_id = producer.db.create_asset(
            data={'name': cls.task_type},
            metadata=task_declaration.get_data()
        )[0]

        task_declaration.asset_id = asset_id
        return task_declaration

    @classmethod
    def get(cls, node, asset_id):
        asset = node.db.retrieve_asset(asset_id)
        train_model = TrainModel.get(node, asset.metadata['train_model_id'])
        dataset = DataSet.get(node, asset.metadata['dataset_id'])

        encrypted_text = asset.metadata['results']
        results = None
        if encrypted_text is not None:
            try:
                results = json.loads(node.decrypt_text(encrypted_text))
            except json.JSONDecodeError:
                results = encrypted_text

        return cls(
            owner_producer_id=asset.metadata['owner_producer_id'],
            dataset=dataset,
            train_model=train_model,
            workers_needed=asset.metadata['workers_needed'],
            workers_requested=asset.metadata['workers_requested'],
            verifiers_needed=asset.metadata['verifiers_needed'],
            epochs=asset.metadata['epochs'],
            asset_id=asset_id,
            status=asset.metadata['status'],
            progress=asset.metadata['progress'],
            results=results,
            encrypted_text=encrypted_text
        )

    @classmethod
    def list(cls, node):
        # TODO: implement list of producer's task declarations
        raise NotImplemented
