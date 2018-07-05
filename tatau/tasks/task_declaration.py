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
        self.tflops = kwargs.get('tflops', 0)
        self.results = kwargs.get('results', [])
        self.errors = kwargs.get('errors', [])
        self.encrypted_text = kwargs.get('encrypted_text', None)
        self.encrypted_text_errors = kwargs.get('encrypted_text_errors', None)

    def get_data(self):
        data = super(TaskDeclaration, self).get_data()
        data.update({
            'owner_producer_id': self.owner_producer_id,
            'train_model_id': self.train_model.asset_id,
            'dataset_id': self.dataset.asset_id,
            'workers_requested': self.workers_requested,
            'epochs': self.epochs,
            'verifiers_needed': self.verifiers_needed,
        })
        return data

    def get_metadata(self):
        return {
            'workers_needed': self.workers_needed,
            'status': self.status,
            'progress': self.progress,
            'tflops': self.tflops,
            'results': self.encrypted_text or self.results,
            'errors': self.encrypted_text_errors or self.errors,
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
            errors=producer.encrypt_text(json.dumps([])),
            epochs=epochs,
            asset_id=None
        )

        asset_id, created = producer.db.create_asset(
            data=task_declaration.get_data(),
            metadata=task_declaration.get_metadata()
        )

        task_declaration.asset_id = asset_id
        return task_declaration

    @classmethod
    def get(cls, node, asset_id):
        asset = node.db.retrieve_asset(asset_id)
        train_model = TrainModel.get(node, asset.data['train_model_id'])
        dataset = DataSet.get(node, asset.data['dataset_id'])

        results = None
        encrypted_text = asset.metadata['results']
        if encrypted_text is not None:
            try:
                results = json.loads(node.decrypt_text(encrypted_text))
            except json.JSONDecodeError:
                results = encrypted_text

        errors = None
        encrypted_text_errors = asset.metadata['errors']
        if encrypted_text_errors is not None:
            try:
                errors = json.loads(node.decrypt_text(encrypted_text_errors))
            except json.JSONDecodeError:
                errors = encrypted_text_errors

        return cls(
            owner_producer_id=asset.data['owner_producer_id'],
            dataset=dataset,
            train_model=train_model,
            workers_needed=asset.metadata['workers_needed'],
            workers_requested=asset.data['workers_requested'],
            verifiers_needed=asset.data['verifiers_needed'],
            epochs=asset.data['epochs'],
            asset_id=asset_id,
            status=asset.metadata['status'],
            progress=asset.metadata['progress'],
            tflops=asset.metadata['tflops'],
            results=results,
            errors=errors,
            encrypted_text=encrypted_text,
            encrypted_text_errors=encrypted_text_errors
        )

