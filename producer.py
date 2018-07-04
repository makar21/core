import logging
import signal
import sys
from multiprocessing import Process

from bottle import Bottle, request, run

import settings
from tatau.dataset import DataSet
from tatau.node.producer import Producer
from tatau.tasks import TaskDeclaration
from tatau.train_model import TrainModel
from utils.logging import configure_logging

configure_logging('producer')

logger = logging.getLogger()


class ProducerServer:
    def __init__(self, producer_node):
        self.producer = producer_node

    def add_task(self, *args, **kwargs):
        pass

    def worker_ready(self, *args, **kwargs):
        worker_id = request.json['worker_id']
        task_id = request.json['task_id']

        self.producer.on_worker_ping(
            task_asset_id=task_id,
            worker_asset_id=worker_id
        )

    def verifier_ready(self, *args, **kwargs):
        worker_id = request.json['verifier_id']
        task_id = request.json['task_id']

        self.producer.on_verifier_ping(
            task_asset_id=task_id,
            verifier_asset_id=worker_id
        )


def web_server(producer):
    producer.db.connect_to_mongodb()
    producer_server = ProducerServer(producer)
    bottle = Bottle()

    bottle.post('/add_task/')(producer_server.add_task)
    bottle.post('/worker/ready/')(producer_server.worker_ready)
    bottle.post('/verifier/ready/')(producer_server.verifier_ready)

    run(bottle, host=settings.PRODUCER_HOST, port=settings.PRODUCER_PORT)


def tx_stream_client(producer):
    producer.run_transaction_listener()


def sigint_handler(signal, frame):
    logging.info('Exiting')
    sys.exit(0)


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
            workers_needed=1,
            verifiers_needed=1,
            epochs=3
        )

        process_class = Process
        if settings.DEBUG:
            import threading
            process_class = threading.Thread

        web_server_process = process_class(target=web_server, args=(producer,))
        web_server_process.start()

        tx_stream_client_process = process_class(target=tx_stream_client, args=(producer,))
        tx_stream_client_process.start()

        signal.signal(signal.SIGINT, sigint_handler)
    except Exception as e:
        logger.fatal(e)
