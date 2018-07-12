import logging
import signal
import sys
from multiprocessing import Process

from tatau_core.tatau.dataset import DataSet
from tatau_core.tatau.node.producer import Producer
from tatau_core.tatau.tasks import TaskDeclaration
from tatau_core.tatau.train_model import TrainModel
from tatau_core.utils.logging import configure_logging

configure_logging('producer')

logger = logging.getLogger()


if __name__ == '__main__':
    try:
        producer = Producer(rsa_pk_fs_name='producer')

        logger.info('Start producer: {}'.format(producer.asset_id))

        train_model = TrainModel.add(
            producer=producer,
            name='mnist',
            code_path='test_data/models/mnist_train.py'
        )

        dataset = DataSet.add(
            producer=producer,
            name='mnist',
            x_train_path='test_data/datasets/mnist/X_train.npz',
            y_train_path='test_data/datasets/mnist/Y_train.npz',
            x_test_path='test_data/datasets/mnist/X_test.npz',
            y_test_path='test_data/datasets/mnist/Y_test.npz',
            files_count=10
        )

        td = TaskDeclaration.add(
            node=producer,
            dataset=dataset,
            train_model=train_model,
            workers_needed=2,
            verifiers_needed=1,
            batch_size=124,
            epochs=3
        )

        producer.run_transaction_listener()
    except Exception as e:
        logger.fatal(e)
