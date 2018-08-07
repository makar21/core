from multiprocessing import RLock, Event, Value


class WorkerInterprocess:
    def __init__(self, interval=1):
        self._event_start_collect_metrics = Event()
        self._event_stop = Event()
        self._tflops = Value('d', 0.0)
        self._pid = Value('i', 0)
        self._tflops_lock = RLock()
        self.interval = interval

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
        self._event_stop.set()

    def __enter__(self):
        self._start_collect_metrics()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop_collect_metrics()
