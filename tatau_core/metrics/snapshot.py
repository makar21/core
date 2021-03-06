import json
import os
from datetime import datetime
from logging import getLogger

import psutil

from tatau_core import settings

logger = getLogger('tatau_core')


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
        gpu_tflops = settings.GPU_TFLOPS
        cpu_tflops = settings.CPU_TFLOPS
        return float(self.average_gpu_load/100.0 * gpu_tflops + self.average_cpu_load/100.0 * cpu_tflops)


class ProcessSnapshot:

    def __init__(self, pid):
        self._pid = pid
        self.process = psutil.Process(pid=self._pid)
        self.process.cpu_percent(interval=1)
        self._cpu_count = psutil.cpu_count()
        self._cpu_percent = 0.0
        self._gpu_percent = 0.0

    def update(self):
        self._cpu_percent = self.process.cpu_percent() / self._cpu_count
        self._gpu_percent = 0.0

        if os.name != 'nt':
            try:
                import gpustat
                gpu_load = []
                for gpu in gpustat.GPUStatCollection.new_query():
                    gpu_load.append(gpu.utilization)
                if len(gpu_load):
                    self._gpu_percent = sum(g for g in gpu_load) / float(len(gpu_load))
            except Exception as ex:
                logger.error('Collect gpu metrics error: {}'.format(ex))
        logger.info('Metrics: cpu: {:.2f}% gpu: {:.2f}%'.format(self._cpu_percent, self._gpu_percent))

    def get_cpu_tflops(self):
        cpu_tflops = settings.CPU_TFLOPS
        return float(self._cpu_percent / 100.0 * cpu_tflops)

    def get_cpu_load(self):
        return self._cpu_percent

    def get_gpu_tflops(self):
        gpu_tflops = settings.GPU_TFLOPS
        return float(self._gpu_percent / 100.0 * gpu_tflops)

    def get_gpu_load(self):
        return self._gpu_percent

    def get_total_tflops(self):
        return self.get_cpu_tflops() + self.get_gpu_tflops()


