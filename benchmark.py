import json
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


def benchmark_train(working_dir, model_file_path, batch_size, epochs, cost_tflops):
    x_train_path = os.path.join(working_dir, 'x_train.npy')
    y_train_path = os.path.join(working_dir, 'y_train.npy')
    x_test_path = os.path.join(working_dir, 'x_test.npy')
    y_test_path = os.path.join(working_dir, 'y_test.npy')

    metrics = MetricsCollector(collect_load=True, use_thread=True)
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

    logger.info('Ethalon TFLOPs cost {}'.format(cost_tflops))

    logger.info('Spent hardcoded total TFLOPs {}'.format(metrics.get_tflops()))
    logger.info('Spent time: {} s'.format(metrics.total_seconds))

    logger.info('Av CPU Load: {} %'.format(metrics.average_cpu_load()))
    logger.info('Spent hardcoded CPU TFLOPs {}'.format(metrics.get_cpu_tflops()))

    logger.info('Av GPU Load: {} %'.format(metrics.average_gpu_load()))
    logger.info('Spent hardcoded GPU TFLOPs: {}'.format(metrics.get_gpu_tflops()))

    logger.info('Estimated TFLOP/s is {}'.format(cost_tflops / metrics.total_seconds))
    logger.info('Hardcoded TFLOP/s is {}'.format(metrics.get_tflops() / metrics.total_seconds))

    return cost_tflops / metrics.total_seconds


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
    ipfs = IPFS()
    # read this from IPFS
    ipfs.api.repo_gc()

    benchmark_config = json.loads(ipfs.read('QmPCGNbGF3jdVXghXDj2jR6jgAAtiwjjy6ZAmE1zTPLCMG'))
    target_dir = tempfile.mkdtemp()
    try:
        timer_model = Timer()
        with timer_model:
            model_code = ipfs.read(benchmark_config['model'])

        model_file_path = os.path.join(target_dir, 'model.py')
        with open(model_file_path, 'wb') as model_file:
            model_file.write(model_code)

        timer_dataset = Timer()
        with timer_dataset:
            dataset_zip_file_path = ipfs.download(benchmark_config['dataset'], target_dir)

        total_size = os.path.getsize(dataset_zip_file_path) + os.path.getsize(model_file_path)
        download_speed = total_size / (timer_dataset.total_seconds + timer_model.total_seconds)

        zip_ref = zipfile.ZipFile(dataset_zip_file_path, 'r')
        with zip_ref:
            zip_ref.extractall(target_dir)

        tflops = benchmark_train(
            working_dir=target_dir,
            model_file_path=model_file_path,
            batch_size=benchmark_config['batch_size'],
            epochs=benchmark_config['epochs'],
            cost_tflops=benchmark_config['cost_tflops']
        )

        logger.info('Result: ipfs speed {} MB/s, performance: {} TFLOP/s'.format(
            download_speed / 1024 / 1024, tflops))

    finally:
        shutil.rmtree(target_dir)


if __name__ == '__main__':
    main()
