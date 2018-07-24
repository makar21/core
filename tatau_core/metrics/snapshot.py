import json
from logging import getLogger
import os
from datetime import datetime

import psutil

logger = getLogger()


class GpuMetric(object):
    def __init__(self, uuid, gpu_load, memory_load):
        self.uuid = uuid
        self.gpu_load = gpu_load
        self.memory_load = memory_load


class CpuCoreMetric(object):
    def __init__(self, bus_id, core_load):
        self.bus_id = bus_id
        self.core_load = core_load


class Snapshot(object):
    def __init__(self):
        self._cpu_metrics = []
        self._gpu_metrics = []
        self._ram_load = psutil.virtual_memory().percent

        core_metrics = psutil.cpu_percent(percpu=True)
        for bus_id in range(len(core_metrics)):
            self._cpu_metrics.append(
                CpuCoreMetric(bus_id=bus_id, core_load=core_metrics[bus_id])
            )

        if os.name != 'nt':
            try:
                import gpustat
                for gpu in gpustat.GPUStatCollection.new_query():
                    memory_load = gpu.memory_used * 100.0 / gpu.memory_total
                    self._gpu_metrics.append(
                        GpuMetric(uuid=gpu.uuid, gpu_load=gpu.utilization, memory_load=memory_load)
                    )
            except Exception as ex:
                logger.error('Collect metrics error: {}'.format(ex))

        self.timestamp = datetime.utcnow()

    @property
    def cpu_metrics(self):
        return self._cpu_metrics

    @property
    def gpu_metrics(self):
        return self._gpu_metrics

    @property
    def ram_load(self):
        return self._ram_load

    @property
    def average_cpu_load(self):
        if len(self.cpu_metrics):
            return sum(c.core_load for c in self.cpu_metrics) / float(len(self.cpu_metrics))
        return 0

    @property
    def average_gpu_load(self):
        if len(self.gpu_metrics):
            return sum(g.gpu_load for g in self.gpu_metrics) / float(len(self.gpu_metrics))
        return 0

    @property
    def average_gpu_memory_load(self):
        if len(self.gpu_metrics):
            return sum(g.memory_load for g in self.gpu_metrics) / float(len(self.gpu_metrics))
        return 0

    def __str__(self):
        return 'CPU: {0:.2f}%, RAM: {1:.2f}%, GPU {2:.2f}%, GRAM: {3:.2f}%'.format(
            self.average_cpu_load, self.ram_load, self.average_gpu_load, self.average_gpu_memory_load
        )

    def to_dict(self):
        return {
            'cpu_load': self.average_cpu_load,
            'ram_load': self.ram_load,
            'gpu_load': self.average_gpu_load,
            'gpu_memory_load': self.average_gpu_memory_load
        }

    def to_json(self):
        return json.dumps(self.to_dict())

    def calc_tflops(self):
        # TODO: define this values
        gpu_tflops = 100000
        cpu_tflops = 10000
        return int(self.average_gpu_load * gpu_tflops + self.average_cpu_load * cpu_tflops)