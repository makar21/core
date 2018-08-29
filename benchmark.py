import argparse
import os
import shutil
import tempfile
import time
import zipfile
from logging import getLogger

import numpy as np

from tatau_core.metrics import MetricsCollector
from tatau_core.nn.tatau.model import Model, TrainProgress
from tatau_core.utils.ipfs import IPFS
from tatau_core.utils.logging import configure_logging

configure_logging(__name__)

logger = getLogger(__name__)


def train(x_train_path, y_train_path, x_test_path, y_test_path, model_path, batch_size, epochs):
    model = Model.load_model(path=model_path)

    x_train = np.load(x_train_path)
    y_train = np.load(y_train_path)
    x_test = np.load(x_test_path)
    y_test = np.load(y_test_path)

    class LocalProgress(TrainProgress):
        def progress_callback(self, progress):
            logger.info("Progress: {:.2f}".format(progress))

    model.train(x=x_train, y=y_train, batch_size=batch_size, nb_epochs=epochs, train_progress=LocalProgress())
    loss, acc = model.eval(x=x_test, y=y_test)
    logger.info('loss({}):{}, acc({}):{}'.format(loss.__class__.__name__, loss, acc.__class__.__name__, acc))


def benchmark_train(working_dir, model_file_path, batch_size, epochs, ethalon_tflops):
    x_train_path = os.path.join(working_dir, 'x_train.npy')
    y_train_path = os.path.join(working_dir, 'y_train.npy')
    x_test_path = os.path.join(working_dir, 'x_test.npy')
    y_test_path = os.path.join(working_dir, 'y_test.npy')

    metrics = MetricsCollector()
    metrics.start_and_wait_signal()
    metrics.set_pid(os.getpid())
    with metrics:
        train(
            x_train_path=x_train_path,
            y_train_path=y_train_path,
            x_test_path=x_test_path,
            y_test_path=y_test_path,
            model_path=model_file_path,
            batch_size=batch_size,
            epochs=epochs
        )

    logger.info('Estimated TFLOP/S is {}'.format(ethalon_tflops / metrics.total_seconds))
    logger.info('Current TFLOP/S is {}'.format(metrics.get_tflops() / metrics.total_seconds))

    return ethalon_tflops / metrics.total_seconds


def benchmark_ipfs(args):
    multihash = args.multihash
    ipfs = IPFS()
    target_dir = tempfile.mkdtemp()
    try:
        time_start = time.time()
        file_path = ipfs.download(multihash, target_dir)
        time_end = time.time()
        file_size = os.path.getsize(file_path)
        logger.info('IPFS download speed: {} kb/s'.format(file_size/1024/(time_end - time_start)))
    finally:
        shutil.rmtree(target_dir)


class Timer:
    def __init__(self):
        self._time_start = None
        self._time_end = None

    def __enter__(self):
        self._time_start = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._time_end = time.time()

    @property
    def total_seconds(self):
        return self._time_end - self._time_start


def main():
    parser = argparse.ArgumentParser(description='Benchmark')
    parser.add_argument('-e', '--epochs', default=3, type=int, metavar='EPOCHS', help='epochs')
    parser.add_argument('-b', '--batch_size', default=128, type=int, metavar='BATCH_SIZE', help='batch size')

    # parser.add_argument('-d', '--dataset', default='benchmark/torch/cifar10', metavar='DATASET', help='dataset dir')
    # parser.add_argument('-mh', '--multihash',
    #                     default='QmRBz8W3K4xDBeu6sDn5kqE4ybndh6CEyFUq1gDEXbYR9V',
    #                     metavar='MULTIHASH', help='default cifar zip multihash')

    args = parser.parse_args()

    torch_cifar_dataset = 'QmRBz8W3K4xDBeu6sDn5kqE4ybndh6CEyFUq1gDEXbYR9V'
    torch_cifar_model_cnn = 'QmfH9CRJtj66rhQ5jznthamDBHJb37He6Hfy8XmVGG23pw'
    torch_cifar_model_resnet = 'Qmc7DvhbHtx4oM874dTuiJgYFU5j369gdH1d674TyzBe5n'
    ethalon_tflops_multihash = 'QmdDs3WEJV77pSgDfpLhM2R1kGkxuukmhLN3UPzaD86xRw'

    model_multihash = torch_cifar_model_resnet

    ipfs = IPFS()
    target_dir = tempfile.mkdtemp()
    try:
        timer_model = Timer()
        with timer_model:
            model_code = ipfs.read(model_multihash)

        model_file_path = os.path.join(target_dir, 'model.py')
        with open(model_file_path, 'wb') as model_file:
            model_file.write(model_code)

        timer_dataset = Timer()
        with timer_dataset:
            dataset_zip_file_path = ipfs.download(torch_cifar_dataset, target_dir)

        total_size = os.path.getsize(dataset_zip_file_path) + os.path.getsize(model_file_path)
        download_speed = total_size / (timer_dataset.total_seconds + timer_model.total_seconds)
        logger.info('IPFS download speed: {} MB/s'.format(download_speed / 1024 / 1024))

        zip_ref = zipfile.ZipFile(dataset_zip_file_path, 'r')
        with zip_ref:
            zip_ref.extractall(target_dir)

        ethalon_tflops = int(ipfs.read(ethalon_tflops_multihash))

        tflops = benchmark_train(target_dir, model_file_path, args.batch_size, args.epochs, ethalon_tflops)
        logger.info('TFLOPs per seconds: {}'.format(tflops))
    finally:
        shutil.rmtree(target_dir)


if __name__ == '__main__':
    main()
