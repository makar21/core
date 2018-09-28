from logging import getLogger

from tatau_core.db import models, fields

logger = getLogger('tatau_core')


class BenchmarkInfo(models.Model):
    worker_id = fields.CharField(immutable=True)
    info_ipfs = fields.CharField(immutable=True)
    downloaded_size = fields.IntegerField(immutable=True)
    download_time = fields.IntegerField(immutable=True)
    model_train_tflops = fields.FloatField(immutable=True)
    train_time = fields.IntegerField(immutable=True)
    av_cpu_load = fields.FloatField(immutable=True)
    av_gpu_load = fields.FloatField(immutable=True)

    @property
    def download_speed(self):
        return self.downloaded_size / self.download_time

    @classmethod
    def create(cls, **kwargs):
        assert kwargs['train_time'] > 0 and kwargs['download_time'] > 0
        return super(BenchmarkInfo, cls).create(**kwargs)
