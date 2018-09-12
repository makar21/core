from tatau_core.db import models, fields
from tatau_core.models.benchmark_info import BenchmarkInfo


class NodeType:
    PRODUCER = 'producer'
    WORKER = 'worker'
    VERIFIER = 'verifier'


class ProducerNode(models.Model):
    node_type = fields.CharField(immutable=True, initial=NodeType.PRODUCER)
    enc_key = fields.CharField(immutable=True)
    account_address = fields.CharField(immutable=False)


class WorkerNode(models.Model):
    node_type = fields.CharField(immutable=True, initial=NodeType.WORKER)
    enc_key = fields.CharField(immutable=True)
    account_address = fields.CharField(immutable=False)
    benchmark_info_id = fields.CharField(null=True, initial=None)

    @property
    def benchmark_info(self):
        if self.benchmark_info_id:
            return BenchmarkInfo.get(self.benchmark_info_id, db=self.db, encryption=self.encryption)
        return None


class VerifierNode(models.Model):
    node_type = fields.CharField(immutable=True, initial=NodeType.VERIFIER)
    enc_key = fields.CharField(immutable=True)
    account_address = fields.CharField(immutable=False)