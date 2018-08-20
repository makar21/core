from logging import getLogger
from multiprocessing import RLock, Event, Value, Process

from psutil import NoSuchProcess

from tatau_core.metrics import ProcessSnapshot

logger = getLogger()


class MetricsCollector:
    def __init__(self, interval=1):
        self._event_start_collect_metrics = Event()
        self._event_stop = Event()
        self._tflops = Value('d', 0.0)
        self._pid = Value('i', 0)
        self._tflops_lock = RLock()
        self.interval = interval
        self._process = None

    def get_tflops(self):
        with self._tflops_lock:
            return self._tflops.value

    def add_tflops(self, tflops):
        with self._tflops_lock:
            self._tflops.value += tflops

    def set_pid(self, pid):
        self._pid.value = pid

    def get_pid(self):
        return self._pid.value

    def _start_collect_metrics(self):
        self._event_start_collect_metrics.set()

    def wait_for_start_collect_metrics(self):
        self._event_start_collect_metrics.wait()

    def should_stop_collect_metrics(self, wait):
        return self._event_stop.wait(wait)

    def _stop_collect_metrics(self):
        self._event_start_collect_metrics.set()
        self._event_stop.set()

    def __enter__(self):
        self._start_collect_metrics()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop_collect_metrics()

    def start_and_wait_signal(self):
        # be sure this instance will not start collect metrics more than once
        assert self._process is None
        self._process = Process(target=self._collect_metrics)
        self._process.start()

    def clean(self):
        self._stop_collect_metrics()

        if self._process:
            self._process.join()

    def _collect_metrics(self):
        self.wait_for_start_collect_metrics()
        logger.info('Start collect metrics')
        try:
            snapshot = ProcessSnapshot(self.get_pid())
            while not self.should_stop_collect_metrics(self.interval):
                snapshot.update()
                self.add_tflops(snapshot.get_total_tflops() * self.interval)
        except NoSuchProcess as ex:
            logger.exception(ex)

        logger.info('Stop collect metrics')
