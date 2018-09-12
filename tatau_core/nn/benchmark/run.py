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


class DownloadSpeedBenchmarkResult:
    def __init__(self, downloaded_size, download_time):
        self.downloaded_size = downloaded_size
        self.download_time = download_time
        logger.info('Download benchmark result: ipfs speed {:.3f} MB/s'.format(
            self.downloaded_size / 1024 / 1024 / self.download_time))


class TrainSpeedBenchmarkResult:
    def __init__(self, model_train_tflops, train_time, av_cpu_load, av_gpu_load):
        self.model_train_tflops = model_train_tflops
        self.train_time = train_time
        self.av_cpu_load = av_cpu_load
        self.av_gpu_load = av_gpu_load

        logger.info(
            'Train speed benchmark result: performance: {:.5f} TFLOP/s, av cpu load: {:.2f}% av gpu load: {:.2f}%'.format(
                self.model_train_tflops / self.train_time, self.av_cpu_load, self.av_gpu_load)
        )


def train(x_train_path, y_train_path, x_test_path, y_test_path, model_path, batch_size, epochs):
    model = Model.load_model(path=model_path)

    x_train = np.load(x_train_path)
    y_train = np.load(y_train_path)
    x_test = np.load(x_test_path)
    y_test = np.load(y_test_path)

    class LocalProgress(TrainProgress):
        def progress_callback(self, progress):
            logger.info("Progress: {:.2f}".format(progress))

    model.train(
        x=x_train, y=y_train,
        batch_size=batch_size, nb_epochs=epochs, current_iteration=1,
        train_progress=LocalProgress()
    )

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

    logger.info('Av CPU Load: {} %'.format(metrics.average_cpu_load))
    logger.info('Spent hardcoded CPU TFLOPs {}'.format(metrics.get_cpu_tflops))

    logger.info('Av GPU Load: {} %'.format(metrics.average_gpu_load))
    logger.info('Spent hardcoded GPU TFLOPs: {}'.format(metrics.get_gpu_tflops()))

    logger.info('Estimated TFLOP/s is {}'.format(cost_tflops / metrics.total_seconds))
    logger.info('Hardcoded TFLOP/s is {}'.format(metrics.get_tflops() / metrics.total_seconds))

    return TrainSpeedBenchmarkResult(
        model_train_tflops=cost_tflops,
        train_time=metrics.total_seconds,
        av_cpu_load=metrics.average_cpu_load,
        av_gpu_load=metrics.average_gpu_load
    )


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


def run():
    ipfs = IPFS()
    # read this from IPFS
    ipfs.api.repo_gc()

    benchmark_info_ipfs = 'QmPCGNbGF3jdVXghXDj2jR6jgAAtiwjjy6ZAmE1zTPLCMG'
    benchmark_config = json.loads(ipfs.read(benchmark_info_ipfs))
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
        zip_ref = zipfile.ZipFile(dataset_zip_file_path, 'r')
        with zip_ref:
            zip_ref.extractall(target_dir)

        train_benchmark_result = benchmark_train(
            working_dir=target_dir,
            model_file_path=model_file_path,
            batch_size=benchmark_config['batch_size'],
            epochs=benchmark_config['epochs'],
            cost_tflops=benchmark_config['cost_tflops']
        )

        train_benchmark_result.info_ipfs = benchmark_info_ipfs

        download_benchmark_result = DownloadSpeedBenchmarkResult(
            downloaded_size=total_size,
            download_time=timer_dataset.total_seconds + timer_model.total_seconds
        )

        return download_benchmark_result, train_benchmark_result
    finally:
        shutil.rmtree(target_dir)

