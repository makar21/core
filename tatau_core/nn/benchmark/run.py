import os
import time
from logging import getLogger

from tatau_core.metrics import MetricsCollector
from tatau_core.nn.tatau.model import Model, TrainProgress
from tatau_core.utils.ipfs import IPFS, Downloader
from tatau_core.utils.logging import configure_logging
from tatau_core.utils.misc import get_dir_size

configure_logging(__name__)

logger = getLogger('tatau_core')


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


def train(train_dir, test_dir, model_path, batch_size, epochs):
    model = Model.load_model(path=model_path)

    class LocalProgress(TrainProgress):
        def progress_callback(self, progress):
            logger.info("Progress: {:.2f}".format(progress))

    model.train(
        chunk_dirs=[x[0] for x in os.walk(train_dir)][1:],
        batch_size=batch_size,
        nb_epochs=epochs,
        current_iteration=1,
        train_progress=LocalProgress()
    )

    # loss, acc = model.eval(chunk_dirs=[x[0] for x in os.walk(test_dir)][1:])
    # logger.info('loss({}):{}, acc({}):{}'.format(loss.__class__.__name__, loss, acc.__class__.__name__, acc))


def benchmark_train(train_dir, test_dir, model_path, batch_size, epochs, cost_tflops):

    metrics = MetricsCollector(collect_load=True, use_thread=True)
    metrics.start_and_wait_signal()
    metrics.set_pid(os.getpid())

    with metrics:
        train(
            train_dir=train_dir,
            test_dir=test_dir,
            model_path=model_path,
            batch_size=batch_size,
            epochs=epochs
        )

    logger.info('Ethalon TFLOPs cost {}'.format(cost_tflops))

    logger.info('Spent hardcoded total TFLOPs {}'.format(metrics.get_tflops()))
    logger.info('Spent time: {} s'.format(metrics.total_seconds))

    logger.info('Av CPU Load: {} %'.format(metrics.average_cpu_load))
    logger.info('Spent hardcoded CPU TFLOPs {}'.format(metrics.get_cpu_tflops()))

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


def get_benchmark_config(benchmark_info_ipfs):
    # return {
    #     'train_ipfs': 'QmP9KUr8Y6HxNoBNM8zakxC65diYWHG2VBRhPHYnT5uWZT',
    #     'test_ipfs': 'QmRL93gvYRypqWs1wpzR8S6kvoGPxeP12v8RbTAJWDsQaK',
    #     'model_ipfs': 'QmdJXSpeqF4RFW3oe3JmhffTSHhu6bPxcxXtGoUXzfnyC4',
    #     'batch_size': 32,
    #     'epochs': 3,
    #     'cost_tflops': 100
    # }

    # TODO: upload config to ipfs
    return {
        'train_ipfs': 'QmUd8UQ2pYyyWJvsFghqmrt43uB8aTsf47sYN15YfXEPGF',
        'test_ipfs': 'QmYcFFcFT6b1djLP66HAZ4pffKFudHZmRGoKTnHhfhR2WW',
        'model_ipfs': 'QmNqpfNqQxMSGhf1D7Hnygx6HRT5rsisfL3gbz8sRwVtxB',
        'batch_size': 32,
        'epochs': 3,
        'cost_tflops': 100
    }


def download_data(train_dir_ipfs, test_dir_ipfs, model_ipfs):
    downloader = Downloader('benchmark')

    train_dir_name = 'train_dir'
    test_dir_name = 'test_dir'
    model_file_name = 'model.py'

    downloader.add_to_download_list(train_dir_ipfs, train_dir_name)
    downloader.add_to_download_list(test_dir_ipfs, test_dir_name)
    downloader.add_to_download_list(model_ipfs, model_file_name)
    downloader.download_all()

    train_dir_path = downloader.resolve_path(train_dir_name)
    test_dir_path = downloader.resolve_path(test_dir_name)
    model_file_path = downloader.resolve_path(model_file_name)

    return train_dir_path, test_dir_path, model_file_path


def get_data_size(train_dir_path, test_dir_path, model_file_path):
    return get_dir_size(train_dir_path) + get_dir_size(test_dir_path) + os.path.getsize(model_file_path)


def run():
    ipfs = IPFS()
    # read this from IPFS
    ipfs.api.repo_gc()

    benchmark_info_ipfs = ''
    benchmark_config = get_benchmark_config(benchmark_info_ipfs)

    timer_download = Timer()
    with timer_download:
        train_dir_path, test_dir_path, model_file_path = download_data(
            train_dir_ipfs=benchmark_config['train_ipfs'],
            test_dir_ipfs=benchmark_config['test_ipfs'],
            model_ipfs=benchmark_config['model_ipfs']
        )

    total_size = get_data_size(train_dir_path, test_dir_path, model_file_path)

    download_benchmark_result = DownloadSpeedBenchmarkResult(
        downloaded_size=total_size,
        download_time=timer_download.total_seconds
    )

    train_benchmark_result = benchmark_train(
        train_dir=train_dir_path,
        test_dir=test_dir_path,
        model_path=model_file_path,
        batch_size=benchmark_config['batch_size'],
        epochs=benchmark_config['epochs'],
        cost_tflops=benchmark_config['cost_tflops']
    )

    train_benchmark_result.info_ipfs = benchmark_info_ipfs
    return download_benchmark_result, train_benchmark_result

