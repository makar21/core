from multiprocessing import RLock, Event, Value


class WorkerInterprocess:
    def __init__(self):
        self._event_start_collect_metrics = Event()
        self._event_stop = Event()
        self._tflops = Value('i', 0)
        self._tflops_lock = RLock()

    def get_tflops(self):
        with self._tflops_lock:
            return self._tflops.value

    def add_tflops(self, tflops):
        with self._tflops_lock:
            self._tflops.value += tflops

    def start_collect_metrics(self):
        self._event_start_collect_metrics.set()

    def wait_for_start_collect_metrics(self):
        self._event_start_collect_metrics.wait()

    def should_stop_collect_metrics(self, wait):
        return self._event_stop.wait(wait)

    def stop_collect_metrics(self):
        self._event_stop.set()