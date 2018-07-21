import logging

from tatau_core.tatau.models import TrainModel, Dataset, TaskDeclaration
from tatau_core.tatau.node.producer import Producer
from tatau_core.utils.logging import configure_logging

configure_logging('producer')

log = logging.getLogger()


if __name__ == '__main__':
    producer = Producer(rsa_pk_fs_name='producer')

    train_model = TrainModel.upload_and_create(
        name='mnist',
        code_path='test_data/models/mnist_train.py'
    )
    log.debug('Added {}'.format(train_model))

    dataset = Dataset.upload_and_create(
        db=producer.db,
        encryption=producer.encryption,
        name='mnist',
        x_train_path='test_data/datasets/mnist/X_train.npz',
        y_train_path='test_data/datasets/mnist/Y_train.npz',
        x_test_path='test_data/datasets/mnist/X_test.npz',
        y_test_path='test_data/datasets/mnist/Y_test.npz',
        files_count=10
    )

    log.debug('Added {}'.format(dataset))

    task = TaskDeclaration.create(
        producer_id=producer.asset_id,
        dataset_id=dataset.asset_id,
        train_model_id=train_model.asset_id,
        workers_needed=2,
        verifiers_needed=2,
        batch_size=124,
        epochs=3
    )

    log.debug('Added {}'.format(task))