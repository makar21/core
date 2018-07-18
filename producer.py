import logging

from tatau_core.tatau.models import TrainModel, Dataset, TaskDeclaration
from tatau_core.tatau.node.producer import Producer
from tatau_core.utils.logging import configure_logging

configure_logging('producer')

log = logging.getLogger()


if __name__ == '__main__':
    try:
        producer = Producer(rsa_pk_fs_name='producer')
        log.debug('Start {}'.format(producer))

        train_model = TrainModel.create(
            db=producer.db,
            encryption=producer.encryption,
            name='mnist',
            code_path='test_data/models/mnist_train.py'
        )

        dataset = Dataset.create(
            db=producer.db,
            encryption=producer.encryption,
            name='mnist',
            x_train_path='test_data/datasets/mnist/X_train.npz',
            y_train_path='test_data/datasets/mnist/Y_train.npz',
            x_test_path='test_data/datasets/mnist/X_test.npz',
            y_test_path='test_data/datasets/mnist/Y_test.npz',
            files_count=10
        )

        td = TaskDeclaration.create(
            db=producer.db,
            encryption=producer.encryption,
            producer_id=producer.asset_id,
            dataset_id=dataset.asset_id,
            train_model_id=train_model.asset_id,
            workers_needed=2,
            verifiers_needed=2,
            batch_size=124,
            epochs=2
        )

        producer.run_transaction_listener()
    except Exception as e:
        log.fatal(e)
