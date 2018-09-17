import time
from logging import getLogger
from multiprocessing import RLock, Event, Value, Process
from threading import Thread

from psutil import NoSuchProcess

from tatau_core.metrics import ProcessSnapshot

logger = getLogger()


class MetricsCollector:
    def __init__(self, interval=1, collect_load=False, use_thread=False):
        self._event_start_collect_metrics = Event()
        self._event_stop = Event()
        self._tflops = Value('d', 0.0)
        self._cpu_tflops = Value('d', 0.0)
        self._gpu_tflops = Value('d', 0.0)
        self._pid = Value('i', 0)
        self._tflops_lock = RLock()
        self.interval = interval
        self._process = None
        self._start_timestamp = None
        self._end_timestamp = None
        self._collect_load = collect_load
        self._use_thread = use_thread
        self._cpu_loads = []
        self._gpu_loads = []

    def get_tflops(self):
        with self._tflops_lock:
            return self._tflops.value

    def get_gpu_tflops(self):
        with self._tflops_lock:
            return self._gpu_tflops.value

    def get_cpu_tflops(self):
        with self._tflops_lock:
            return self._cpu_tflops.value

    @property
    def average_cpu_load(self):
        if len(self._cpu_loads):
            return sum(self._cpu_loads)/float(len(self._cpu_loads))
        return 0

    @property
    def average_gpu_load(self):
        if len(self._gpu_loads):
            return sum(self._gpu_loads)/float(len(self._gpu_loads))
        return 0

    def add_tflops(self, cpu_tflops, gpu_tflops):
        with self._tflops_lock:
            self._tflops.value += cpu_tflops + gpu_tflops
            self._gpu_tflops.value += gpu_tflops
            self._cpu_tflops.value += cpu_tflops

    def set_pid(self, pid):
        self._pid.value = pid

    def get_pid(self):
        return self._pid.value

    def _start_collect_metrics(self):
        self._start_timestamp = time.time()
        self._event_start_collect_metrics.set()

    def wait_for_start_collect_metrics(self):
        self._event_start_collect_metrics.wait()

    def should_stop_collect_metrics(self, wait):
        return self._event_stop.wait(wait)

    def _stop_collect_metrics(self):
        logger.info('Signal for stop collect metrics')
        self._event_start_collect_metrics.set()
        self._event_stop.set()
        self._end_timestamp = time.time()

    def __enter__(self):
        self._start_collect_metrics()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop_collect_metrics()

    def start_and_wait_signal(self):
        # be sure this instance will not start collect metrics more than once
        assert self._process is None
        if self._use_thread:
            self._process = Thread(target=self._collect_metrics)
        else:
            self._process = Process(target=self._collect_metrics)
        self._process.start()

    def clean(self):
        self._stop_collect_metrics()

        if self._process:
            logger.info('Wait for end of process collect metrics PID: {}'.format(self.get_pid()))
            self._process.join()

    def _collect_metrics(self):
        self.wait_for_start_collect_metrics()
        logger.info('Start collect metrics')
        try:
            snapshot = ProcessSnapshot(self.get_pid())
            while not self.should_stop_collect_metrics(self.interval):
                snapshot.update()
                self.add_tflops(
                    cpu_tflops=snapshot.get_cpu_tflops() * self.interval,
                    gpu_tflops=snapshot.get_gpu_tflops() * self.interval
                )

                if self._collect_load:
                    self._cpu_loads.append(snapshot.get_cpu_load())
                    self._gpu_loads.append(snapshot.get_gpu_load())

        except NoSuchProcess as ex:
            logger.exception(ex)

        logger.info('Stop collect metrics')

    @property
    def total_seconds(self):
        return self._end_timestamp - self._start_timestamp
