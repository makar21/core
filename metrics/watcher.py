from __future__ import absolute_import, unicode_literals

# standard library
import threading

from .snapshot import Snapshot


class Watcher:
    # 30 seconds
    interval = 30

    stop = False

    def __init__(self):
        self.metrics_list_lock = threading.RLock()
        self.event_stop = threading.Event()
        self.metric_snapshots = []
        self.thread_collect = threading.Thread(target=Watcher.thread_collect_metrics, args=[self])
        self.thread_send = threading.Thread(target=Watcher.thread_send_metrics, args=[self])

    def __enter__(self):
        self.event_stop.clear()
        self.thread_collect.start()
        self.thread_send.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.event_stop.set()
        self.thread_collect.join()
        self.thread_send.join()

    def save_metrics(self):
        with self.metrics_list_lock:
            s = Snapshot()
            self.metric_snapshots.append(s)

    def send_metrics(self):
        data_to_send = []
        with self.metrics_list_lock:
            for snapshot in self.metric_snapshots:
                data_to_send.append({
                    'cpu_load': snapshot.average_cpu_load,
                    'ram_load': snapshot.ram_load,
                    'gpu_load': snapshot.average_gpu_load,
                    'gpu_memory_load': snapshot.average_gpu_memory_load,
                    'when': '{0}'.format(snapshot.timestamp),
                    'job': self.job_uuid,
                    'node': self.node_uuid
                })

            self.metric_snapshots.clear()

        self.tatau_api.send_metrics(data_to_send)

    @staticmethod
    def thread_collect_metrics(manager):
        while not manager.event_stop.wait(1):
            manager.save_metrics()

    @staticmethod
    def thread_send_metrics(manager):
        while not manager.event_stop.wait(Watcher.interval):
            manager.send_metrics()

        # send last metrics
        manager.send_metrics()
